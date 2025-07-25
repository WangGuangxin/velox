/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <fmt/format.h>
#include <folly/Math.h>
#include <re2/re2.h>

#include "folly/experimental/EventCount.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/common/memory/tests/SharedArbitratorTestUtil.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/GroupingSet.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/PrefixSort.h"
#include "velox/exec/Values.h"
#include "velox/exec/prefixsort/PrefixSortEncoder.h"
#include "velox/exec/tests/utils/ArbitratorTestUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/SumNonPODAggregate.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

namespace facebook::velox::exec::test {

using core::QueryConfig;
using facebook::velox::test::BatchMaker;
using namespace common::testutil;

/// No-op implementation of Aggregate. Provides public access to following
/// base class methods: setNull, clearNull and isNull.
class AggregateFunc : public Aggregate {
 public:
  explicit AggregateFunc(TypePtr resultType) : Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return 0;
  }

  bool setNullTest(char* group) {
    return Aggregate::setNull(group);
  }

  bool clearNullTest(char* group) {
    return Aggregate::clearNull(group);
  }

  bool isNullTest(char* group) const {
    return Aggregate::isNull(group);
  }

  void addRawInput(
      char** /*groups*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {}

  void extractValues(
      char** /*groups*/,
      int32_t /*numGroups*/,
      VectorPtr* /*result*/) override {}

  void addIntermediateResults(
      char** /*groups*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {}

  void addSingleGroupRawInput(
      char* /*group*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {}

  void addSingleGroupIntermediateResults(
      char* /*group*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {}

  void extractAccumulators(
      char** /*groups*/,
      int32_t /*numGroups*/,
      VectorPtr* /*result*/) override {}

 protected:
  void initializeNewGroupsInternal(
      char** /*groups*/,
      folly::Range<const vector_size_t*> /*indices*/) override {}
};

void checkSpillStats(PlanNodeStats& stats, bool expectedSpill) {
  if (expectedSpill) {
    ASSERT_GT(stats.spilledRows, 0);
    ASSERT_GT(stats.spilledInputBytes, 0);
    ASSERT_GT(stats.spilledBytes, 0);
    ASSERT_GT(stats.spilledPartitions, 0);
    ASSERT_GT(stats.customStats[Operator::kSpillRuns].sum, 0);
    ASSERT_GT(stats.customStats[Operator::kSpillFillTime].sum, 0);
    ASSERT_GT(stats.customStats[Operator::kSpillSortTime].sum, 0);
    ASSERT_GT(stats.customStats[Operator::kSpillExtractVectorTime].sum, 0);
    ASSERT_GT(stats.customStats[Operator::kSpillSerializationTime].sum, 0);
    ASSERT_GT(stats.customStats[Operator::kSpillFlushTime].sum, 0);
    ASSERT_GT(stats.customStats[Operator::kSpillWrites].sum, 0);
    ASSERT_GT(stats.customStats[Operator::kSpillWriteTime].sum, 0);
  } else {
    ASSERT_EQ(stats.spilledRows, 0);
    ASSERT_EQ(stats.spilledInputBytes, 0);
    ASSERT_EQ(stats.spilledBytes, 0);
    ASSERT_EQ(stats.spilledPartitions, 0);
    ASSERT_EQ(stats.spilledFiles, 0);
    ASSERT_EQ(stats.customStats[Operator::kSpillRuns].sum, 0);
    ASSERT_EQ(stats.customStats[Operator::kSpillFillTime].sum, 0);
    ASSERT_EQ(stats.customStats[Operator::kSpillSortTime].sum, 0);
    ASSERT_EQ(stats.customStats[Operator::kSpillExtractVectorTime].sum, 0);
    ASSERT_EQ(stats.customStats[Operator::kSpillSerializationTime].sum, 0);
    ASSERT_EQ(stats.customStats[Operator::kSpillFlushTime].sum, 0);
    ASSERT_EQ(stats.customStats[Operator::kSpillWrites].sum, 0);
    ASSERT_EQ(stats.customStats[Operator::kSpillWriteTime].sum, 0);
  }
  ASSERT_EQ(
      stats.customStats[Operator::kSpillSerializationTime].count,
      stats.customStats[Operator::kSpillFlushTime].count);
  ASSERT_EQ(
      stats.customStats[Operator::kSpillWrites].count,
      stats.customStats[Operator::kSpillWriteTime].count);
}

class AggregationTest : public OperatorTestBase {
 protected:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
    TestValue::enable();
  }

  void SetUp() override {
    OperatorTestBase::SetUp();
    filesystems::registerLocalFileSystem();
    registerSumNonPODAggregate("sumnonpod", 64);
  }

  std::vector<RowVectorPtr>
  makeVectors(const RowTypePtr& rowType, size_t size, int numVectors) {
    std::vector<RowVectorPtr> vectors;
    VectorFuzzer fuzzer({.vectorSize = size}, pool());
    for (int32_t i = 0; i < numVectors; ++i) {
      vectors.push_back(fuzzer.fuzzInputRow(rowType));
    }
    return vectors;
  }

  template <typename T>
  void testSingleKey(
      const std::vector<RowVectorPtr>& vectors,
      const std::string& keyName,
      bool ignoreNullKeys,
      bool distinct) {
    NonPODInt64::clearStats();
    std::vector<std::string> aggregates;
    if (!distinct) {
      aggregates = {
          "sum(15)", "sum(0.1)", "sum(c1)",     "sum(c2)", "sum(c4)", "sum(c5)",
          "min(15)", "min(0.1)", "min(c1)",     "min(c2)", "min(c3)", "min(c4)",
          "min(c5)", "max(15)",  "max(0.1)",    "max(c1)", "max(c2)", "max(c3)",
          "max(c4)", "max(c5)",  "sumnonpod(1)"};
    }

    auto op = PlanBuilder()
                  .values(vectors)
                  .aggregation(
                      {keyName},
                      aggregates,
                      {},
                      core::AggregationNode::Step::kPartial,
                      ignoreNullKeys)
                  .planNode();

    std::string fromClause = "FROM tmp";
    if (ignoreNullKeys) {
      fromClause += " WHERE " + keyName + " IS NOT NULL";
    }
    if (distinct) {
      assertQuery(op, "SELECT distinct " + keyName + " " + fromClause);
    } else {
      assertQuery(
          op,
          "SELECT " + keyName +
              ", sum(15), sum(cast(0.1 as double)), sum(c1), sum(c2), sum(c4), sum(c5) , min(15), min(0.1), min(c1), min(c2), min(c3), min(c4), min(c5), max(15), max(0.1), max(c1), max(c2), max(c3), max(c4), max(c5), sum(1) " +
              fromClause + " GROUP BY " + keyName);
    }
    EXPECT_EQ(NonPODInt64::constructed, NonPODInt64::destructed);
  }

  void testMultiKey(
      const std::vector<RowVectorPtr>& vectors,
      bool ignoreNullKeys,
      bool distinct) {
    std::vector<std::string> aggregates;
    if (!distinct) {
      aggregates = {
          "sum(15)",
          "sum(0.1)",
          "sum(c4)",
          "sum(c5)",
          "min(15)",
          "min(0.1)",
          "min(c3)",
          "min(c4)",
          "min(c5)",
          "max(15)",
          "max(0.1)",
          "max(c3)",
          "max(c4)",
          "max(c5)",
          "sumnonpod(1)"};
    }
    auto op = PlanBuilder()
                  .values(vectors)
                  .aggregation(
                      {"c0", "c1", "c6"},
                      aggregates,
                      {},
                      core::AggregationNode::Step::kPartial,
                      ignoreNullKeys)
                  .planNode();

    std::string fromClause = "FROM tmp";
    if (ignoreNullKeys) {
      fromClause +=
          " WHERE c0 IS NOT NULL AND c1 IS NOT NULL AND c6 IS NOT NULL";
    }
    if (distinct) {
      assertQuery(op, "SELECT distinct c0, c1, c6 " + fromClause);
    } else {
      assertQuery(
          op,
          "SELECT c0, c1, c6, sum(15), sum(cast(0.1 as double)), sum(c4), sum(c5), min(15), min(0.1), min(c3), min(c4), min(c5), max(15), max(0.1), max(c3), max(c4), max(c5), sum(1) " +
              fromClause + " GROUP BY c0, c1, c6");
    }
    EXPECT_EQ(NonPODInt64::constructed, NonPODInt64::destructed);
  }

  template <typename T>
  void setTestKey(
      int64_t value,
      int32_t multiplier,
      vector_size_t row,
      FlatVector<T>* vector) {
    vector->set(row, value * multiplier);
  }

  template <typename T>
  void setKey(
      int32_t column,
      int32_t cardinality,
      int32_t multiplier,
      int32_t row,
      RowVector* batch) {
    auto vector = batch->childAt(column)->asUnchecked<FlatVector<T>>();
    auto value = folly::Random::rand32(rng_) % cardinality;
    setTestKey(value, multiplier, row, vector);
  }

  void makeModeTestKeys(
      TypePtr rowType,
      int32_t numRows,
      int32_t c0,
      int32_t c1,
      int32_t c2,
      int32_t c3,
      int32_t c4,
      int32_t c5,
      std::vector<RowVectorPtr>& batches) {
    RowVectorPtr rowVector;
    for (auto count = 0; count < numRows; ++count) {
      if (count % 1000 == 0) {
        rowVector = BaseVector::create<RowVector>(
            rowType, std::min(1000, numRows - count), pool_.get());
        batches.push_back(rowVector);
        for (auto& child : rowVector->children()) {
          child->resize(1000);
        }
      }
      setKey<int64_t>(0, c0, 6, count % 1000, rowVector.get());
      setKey<int16_t>(1, c1, 1, count % 1000, rowVector.get());
      setKey<int8_t>(2, c2, 1, count % 1000, rowVector.get());
      setKey<StringView>(3, c3, 2, count % 1000, rowVector.get());
      setKey<StringView>(4, c4, 5, count % 1000, rowVector.get());
      setKey<StringView>(5, c5, 8, count % 1000, rowVector.get());
    }
  }

  // Inserts 'key' into 'order' with random bits and a serial
  // number. The serial number makes repeats of 'key' unique and the
  // random bits randomize the order in the set.
  void insertRandomOrder(
      int64_t key,
      int64_t serial,
      folly::F14FastSet<uint64_t>& order) {
    // The word has 24 bits of grouping key, 8 random bits and 32 bits of serial
    // number.
    order.insert(
        ((folly::Random::rand32(rng_) & 0xff) << 24) | key | (serial << 32));
  }

  // Returns the key from a value inserted with insertRandomOrder().
  int32_t randomOrderKey(uint64_t key) {
    return key & ((1 << 24) - 1);
  }

  void addBatch(
      int32_t count,
      RowVectorPtr rows,
      BufferPtr& dictionary,
      std::vector<RowVectorPtr>& batches) {
    std::vector<VectorPtr> children;
    dictionary->setSize(count * sizeof(vector_size_t));
    children.push_back(BaseVector::wrapInDictionary(
        BufferPtr(nullptr), dictionary, count, rows->childAt(0)));
    children.push_back(BaseVector::wrapInDictionary(
        BufferPtr(nullptr), dictionary, count, rows->childAt(1)));
    children.push_back(children[1]);
    batches.push_back(vectorMaker_.rowVector(children));
    dictionary = AlignedBuffer::allocate<vector_size_t>(
        dictionary->capacity() / sizeof(vector_size_t), rows->pool());
  }

  // Makes batches which reference rows in 'rows' via dictionary. The
  // dictionary indices are given by 'order', wich has values with
  // indices plus random bits so as to create randomly scattered,
  // sometimes repeated values.
  void makeBatches(
      RowVectorPtr rows,
      folly::F14FastSet<uint64_t>& order,
      std::vector<RowVectorPtr>& batches) {
    constexpr int32_t kBatch = 1000;
    BufferPtr dictionary =
        AlignedBuffer::allocate<vector_size_t>(kBatch, rows->pool());
    auto rawIndices = dictionary->asMutable<vector_size_t>();
    int32_t counter = 0;
    for (auto& n : order) {
      rawIndices[counter++] = randomOrderKey(n);
      if (counter == kBatch) {
        addBatch(counter, rows, dictionary, batches);
        rawIndices = dictionary->asMutable<vector_size_t>();
        counter = 0;
      }
    }
    if (counter > 0) {
      addBatch(counter, rows, dictionary, batches);
    }
  }

  std::unique_ptr<RowContainer> makeRowContainer(
      const std::vector<TypePtr>& keyTypes,
      const std::vector<TypePtr>& dependentTypes) {
    return std::make_unique<RowContainer>(
        keyTypes,
        false,
        std::vector<Accumulator>{},
        dependentTypes,
        false,
        false,
        true,
        true,
        pool_.get());
  }

  RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6"},
          {BIGINT(),
           SMALLINT(),
           INTEGER(),
           BIGINT(),
           REAL(),
           DOUBLE(),
           VARCHAR()})};
  folly::Random::DefaultGenerator rng_;
  memory::MemoryReclaimer::Stats reclaimerStats_;
  VectorFuzzer::Options fuzzerOpts_{
      .vectorSize = 1024,
      .nullRatio = 0,
      .stringLength = 1024,
      .stringVariableLength = false,
      .allowLazyVector = false};
};

template <>
void AggregationTest::setTestKey(
    int64_t value,
    int32_t multiplier,
    vector_size_t row,
    FlatVector<StringView>* vector) {
  std::string chars;
  if (multiplier == 2) {
    chars.resize(2);
    chars[0] = (value % 64) + 32;
    chars[1] = ((value / 64) % 64) + 32;
  } else {
    chars = fmt::format("{}", value);
    for (int i = 2; i < multiplier; ++i) {
      chars = chars + fmt::format("{}", i * value);
    }
  }
  vector->set(row, StringView(chars));
}

TEST_F(AggregationTest, missingFunctionOrSignature) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
      makeFlatVector<bool>({true, true, false}),
  });

  // (smallint, varchar) -> bigint
  registerAggregateFunction(
      "test_aggregate",
      {AggregateFunctionSignatureBuilder()
           .returnType("bigint")
           .intermediateType("tinyint")
           .argumentType("smallint")
           .argumentType("varchar")
           .build()},
      [&](core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> { VELOX_UNREACHABLE(); },
      false /*registerCompanionFunctions*/,
      true /*overwrite*/);

  std::vector<core::TypedExprPtr> inputs = {
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "c0"),
      std::make_shared<core::FieldAccessTypedExpr>(BOOLEAN(), "c1"),
  };
  auto missingFunc = std::make_shared<core::CallTypedExpr>(
      BIGINT(), inputs, "missing-function");
  auto wrongInputTypes =
      std::make_shared<core::CallTypedExpr>(BIGINT(), inputs, "test_aggregate");
  auto missingInputs = std::make_shared<core::CallTypedExpr>(
      BIGINT(), std::vector<core::TypedExprPtr>{}, "test_aggregate");

  auto makePlan = [&](const core::CallTypedExprPtr& aggExpr) {
    return PlanBuilder()
        .values({data})
        .addNode([&](auto nodeId, auto source) -> core::PlanNodePtr {
          std::vector<TypePtr> rawInputTypes;
          for (const auto& input : aggExpr->inputs()) {
            rawInputTypes.push_back(input->type());
          }

          std::vector<core::AggregationNode::Aggregate> aggregates{
              {aggExpr, rawInputTypes, nullptr, {}, {}}};

          return std::make_shared<core::AggregationNode>(
              nodeId,
              core::AggregationNode::Step::kSingle,
              std::vector<core::FieldAccessTypedExprPtr>{},
              std::vector<core::FieldAccessTypedExprPtr>{},
              std::vector<std::string>{"agg"},
              aggregates,
              false,
              std::move(source));
        })
        .planNode();
  };

  CursorParameters params;
  params.planNode = makePlan(missingFunc);
  VELOX_ASSERT_THROW(
      readCursor(params),
      "Aggregate function not registered: missing-function");

  params.planNode = makePlan(wrongInputTypes);
  VELOX_ASSERT_THROW(
      readCursor(params),
      "Aggregate function signature is not supported: test_aggregate(BIGINT, BOOLEAN). "
      "Supported signatures: (smallint,varchar) -> tinyint -> bigint.");

  params.planNode = makePlan(missingInputs);
  VELOX_ASSERT_THROW(
      readCursor(params),
      "Aggregate function signature is not supported: test_aggregate(). "
      "Supported signatures: (smallint,varchar) -> tinyint -> bigint.");
}

TEST_F(AggregationTest, missingLambdaFunction) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
  });

  auto field = [](const std::string& name) {
    return std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), name);
  };

  std::vector<core::TypedExprPtr> inputs = {
      field("c0"),
      // (a, b) -> a + b.
      std::make_shared<core::LambdaTypedExpr>(
          ROW({"a", "b"}, {BIGINT(), BIGINT()}),
          std::make_shared<core::CallTypedExpr>(
              BIGINT(),
              std::vector<core::TypedExprPtr>{field("a"), field("b")},
              "multiply")),
  };

  auto plan = PlanBuilder()
                  .values({data})
                  .addNode([&](auto nodeId, auto source) -> core::PlanNodePtr {
                    std::vector<core::AggregationNode::Aggregate> aggregates{
                        {std::make_shared<core::CallTypedExpr>(
                             BIGINT(), inputs, "missing-lambda"),
                         {BIGINT()},
                         nullptr,
                         {},
                         {}}};

                    return std::make_shared<core::AggregationNode>(
                        nodeId,
                        core::AggregationNode::Step::kSingle,
                        std::vector<core::FieldAccessTypedExprPtr>{},
                        std::vector<core::FieldAccessTypedExprPtr>{},
                        std::vector<std::string>{"agg"},
                        aggregates,
                        false,
                        std::move(source));
                  })
                  .planNode();

  CursorParameters params;
  params.planNode = plan;
  VELOX_ASSERT_THROW(
      readCursor(params), "Aggregate function not registered: missing-lambda");
}

TEST_F(AggregationTest, DISABLED_resultTypeMismatch) {
  using Step = core::AggregationNode::Step;

  registerAggregateFunction(
      "test_aggregate",
      {AggregateFunctionSignatureBuilder()
           .returnType("bigint")
           .intermediateType("bigint")
           .argumentType("bigint")
           .build()},
      [&](Step /*step*/,
          const std::vector<TypePtr>& /*argTypes*/,
          const TypePtr& /*resultType*/,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> { VELOX_UNREACHABLE(); },
      false /*registerCompanionFunctions*/,
      true /*overwrite*/);

  for (auto step : {Step::kIntermediate, Step::kPartial}) {
    VELOX_ASSERT_THROW(
        Aggregate::create(
            "test_aggregate",
            step,
            std::vector<TypePtr>{BIGINT()},
            INTEGER(),
            core::QueryConfig{{}}),
        "Intermediate type mismatch");
  }

  for (auto step : {Step::kFinal, Step::kSingle}) {
    VELOX_ASSERT_THROW(
        Aggregate::create(
            "test_aggregate",
            step,
            std::vector<TypePtr>{BIGINT()},
            INTEGER(),
            core::QueryConfig{{}}),
        "Final type mismatch");
  }
}

TEST_F(AggregationTest, global) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);

  auto op = PlanBuilder()
                .values(vectors)
                .aggregation(
                    {},
                    {"sum(15)",
                     "sum(c1)",
                     "sum(c2)",
                     "sum(c4)",
                     "sum(c5)",
                     "min(15)",
                     "min(c1)",
                     "min(c2)",
                     "min(c3)",
                     "min(c4)",
                     "min(c5)",
                     "max(15)",
                     "max(c1)",
                     "max(c2)",
                     "max(c3)",
                     "max(c4)",
                     "max(c5)",
                     "sumnonpod(1)"},
                    {},
                    core::AggregationNode::Step::kPartial,
                    false)
                .planNode();

  assertQuery(
      op,
      "SELECT sum(15), sum(c1), sum(c2), sum(c4), sum(c5), "
      "min(15), min(c1), min(c2), min(c3), min(c4), min(c5), "
      "max(15), max(c1), max(c2), max(c3), max(c4), max(c5), sum(1) FROM tmp");
}

TEST_F(AggregationTest, manyGlobalAggregations) {
  // Test a query with a large number of global aggregations.
  // Global aggregations have a separate code path that does not use a
  // HashTable, but rather a single row outside of a RowContainer.  Having many
  // aggregations can expose issues with that single row that may not occur with
  // only a few aggregations.
  auto rowType =
      velox::test::VectorMaker::rowType(std::vector<TypePtr>(100, SMALLINT()));
  auto vectors = makeVectors(rowType, 10, 100);
  createDuckDbTable(vectors);

  std::vector<std::string> aggregates;
  for (int i = 0; i < rowType->size(); i++) {
    aggregates.push_back(fmt::format("sum({})", rowType->nameOf(i)));
  }

  auto op = PlanBuilder()
                .values(vectors)
                .singleAggregation({}, aggregates)
                .planNode();

  assertQuery(op, "SELECT " + folly::join(", ", aggregates) + " FROM tmp");

  aggregates.clear();
  for (int i = 0; i < rowType->size(); i++) {
    aggregates.push_back(fmt::format("sum(distinct {})", rowType->nameOf(i)));
  }

  op = PlanBuilder()
           .values(vectors)
           .singleAggregation({}, aggregates)
           .planNode();

  assertQuery(op, "SELECT " + folly::join(", ", aggregates) + " FROM tmp");

  rowType =
      velox::test::VectorMaker::rowType(std::vector<TypePtr>(32, SMALLINT()));
  vectors = makeVectors(rowType, 10, 32);
  createDuckDbTable(vectors);
  aggregates.clear();
  for (int i = 0; i < rowType->size(); i++) {
    aggregates.push_back(fmt::format(
        "array_agg({} ORDER BY {})", rowType->nameOf(i), rowType->nameOf(i)));
  }

  op = PlanBuilder()
           .values(vectors)
           .singleAggregation({}, aggregates)
           .planNode();

  assertQuery(op, "SELECT " + folly::join(", ", aggregates) + " FROM tmp");

  EXPECT_EQ(NonPODInt64::constructed, NonPODInt64::destructed);
}

TEST_F(AggregationTest, singleBigintKey) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  testSingleKey<int64_t>(vectors, "c0", false, false);
  testSingleKey<int64_t>(vectors, "c0", true, false);
}

TEST_F(AggregationTest, singleBigintKeyDistinct) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  testSingleKey<int64_t>(vectors, "c0", false, true);
  testSingleKey<int64_t>(vectors, "c0", true, true);
}

TEST_F(AggregationTest, singleStringKey) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  testSingleKey<StringView>(vectors, "c6", false, false);
  testSingleKey<StringView>(vectors, "c6", true, false);
}

TEST_F(AggregationTest, singleStringKeyDistinct) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  testSingleKey<StringView>(vectors, "c6", false, true);
  testSingleKey<StringView>(vectors, "c6", true, true);
}

TEST_F(AggregationTest, multiKey) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  testMultiKey(vectors, false, false);
  testMultiKey(vectors, true, false);
}

TEST_F(AggregationTest, multiKeyDistinct) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  testMultiKey(vectors, false, true);
  testMultiKey(vectors, true, true);
}

TEST_F(AggregationTest, aggregateOfNulls) {
  auto rowVector = makeRowVector({
      BatchMaker::createVector<TypeKind::BIGINT>(
          rowType_->childAt(0), 100, *pool_),
      makeNullConstant(TypeKind::SMALLINT, 100),
  });

  auto vectors = {rowVector};
  createDuckDbTable(vectors);

  auto op = PlanBuilder()
                .values(vectors)
                .aggregation(
                    {"c0"},
                    {"sum(c1)", "min(c1)", "max(c1)"},
                    {},
                    core::AggregationNode::Step::kPartial,
                    false)
                .planNode();

  assertQuery(op, "SELECT c0, sum(c1), min(c1), max(c1) FROM tmp GROUP BY c0");

  // global aggregation
  op = PlanBuilder()
           .values(vectors)
           .aggregation(
               {},
               {"sum(c1)", "min(c1)", "max(c1)"},
               {},
               core::AggregationNode::Step::kPartial,
               false)
           .planNode();

  assertQuery(op, "SELECT sum(c1), min(c1), max(c1) FROM tmp");
}

// Verify behavior of setNull method.
TEST_F(AggregationTest, setNull) {
  AggregateFunc aggregate(BIGINT());
  int32_t accumulatorFlagsOffset = 0;
  aggregate.setOffsets(
      0,
      RowContainer::nullByte(accumulatorFlagsOffset),
      RowContainer::nullMask(accumulatorFlagsOffset),
      RowContainer::initializedByte(accumulatorFlagsOffset),
      RowContainer::initializedMask(accumulatorFlagsOffset),
      0);
  char group{0};
  aggregate.clearNullTest(&group);
  EXPECT_FALSE(aggregate.isNullTest(&group));

  // Verify setNull returns true if value is non null.
  EXPECT_TRUE(aggregate.setNullTest(&group));
  EXPECT_TRUE(aggregate.isNullTest(&group));

  // Verify setNull returns false if value is already null.
  EXPECT_FALSE(aggregate.setNullTest(&group));
  EXPECT_TRUE(aggregate.isNullTest(&group));
}

TEST_F(AggregationTest, hashmodes) {
  rng_.seed(1);
  auto rowType =
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), SMALLINT(), TINYINT(), VARCHAR(), VARCHAR(), VARCHAR()});

  std::vector<RowVectorPtr> batches;

  // 20K rows with all at low cardinality.
  makeModeTestKeys(rowType, 20000, 2, 2, 2, 4, 4, 4, batches);
  // 20K rows with all at slightly higher cardinality, still in array range.
  makeModeTestKeys(rowType, 20000, 2, 2, 2, 4, 16, 4, batches);
  // 25K rows with cardinality outside of array range. We transit to
  // generic hash table from normalized keys when running out of quota
  // for distinct string storage for the sixth key.
  makeModeTestKeys(rowType, 25000, 1000000, 2, 2, 4, 4, 1000000, batches);
  createDuckDbTable(batches);
  auto op =
      PlanBuilder()
          .values(batches)
          .singleAggregation({"c0", "c1", "c2", "c3", "c4", "c5"}, {"sum(1)"})
          .planNode();

  std::atomic<BaseHashTable::HashMode> mode{BaseHashTable::HashMode::kArray};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::HashTable::setHashMode",
      std::function<void(void*)>([&](void* newMode) {
        mode = *reinterpret_cast<BaseHashTable::HashMode*>(newMode);
      }));
  assertQuery(
      op,
      "SELECT c0, c1, C2, C3, C4, C5, sum(1) FROM tmp "
      " GROUP BY c0, c1, c2, c3, c4, c5");
#ifndef NDEBUG
  EXPECT_EQ(mode, BaseHashTable::HashMode::kHash);
#endif
}

TEST_F(AggregationTest, rangeToDistinct) {
  rng_.seed(1);
  auto rowType =
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), SMALLINT(), TINYINT(), VARCHAR(), VARCHAR(), VARCHAR()});

  std::vector<RowVectorPtr> batches;
  // 20K rows with all at low cardinality. c0 is a range.
  makeModeTestKeys(rowType, 20000, 2000, 2, 2, 4, 4, 4, batches);
  // 20 rows that make c0 represented as distincts.
  makeModeTestKeys(rowType, 20, 200000000, 2, 2, 4, 4, 4, batches);
  // More keys in the low cardinality range. We see if these still hit
  // after the re-encoding of c0.
  makeModeTestKeys(rowType, 10000, 2000, 2, 2, 4, 4, 4, batches);

  createDuckDbTable(batches);
  auto op =
      PlanBuilder()
          .values(batches)
          .singleAggregation({"c0", "c1", "c2", "c3", "c4", "c5"}, {"sum(1)"})
          .planNode();

  assertQuery(
      op,
      "SELECT c0, c1, c2, c3, c4, c5, sum(1) FROM tmp "
      " GROUP BY c0, c1, c2, c3, c4, c5");
}

TEST_F(AggregationTest, allKeyTypes) {
  // Covers different key types. Unlike the integer/string tests, the
  // hash table begins life in the generic mode, not array or
  // normalized key. Add types here as they become supported.
  auto rowType =
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {DOUBLE(), REAL(), BIGINT(), INTEGER(), BOOLEAN(), VARCHAR()});

  std::vector<RowVectorPtr> batches;
  for (auto i = 0; i < 10; ++i) {
    batches.push_back(std::static_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType, 100, *pool_)));
  }
  createDuckDbTable(batches);
  auto op =
      PlanBuilder()
          .values(batches)
          .singleAggregation({"c0", "c1", "c2", "c3", "c4", "c5"}, {"sum(1)"})
          .planNode();

  assertQuery(
      op,
      "SELECT c0, c1, c2, c3, c4, c5, sum(1) FROM tmp "
      " GROUP BY c0, c1, c2, c3, c4, c5");
}

TEST_F(AggregationTest, partialAggregationMemoryLimit) {
  auto vectors = {
      makeRowVector({makeFlatVector<int32_t>(
          100, [](auto row) { return row; }, nullEvery(5))}),
      makeRowVector({makeFlatVector<int32_t>(
          110, [](auto row) { return row + 29; }, nullEvery(7))}),
      makeRowVector({makeFlatVector<int32_t>(
          90, [](auto row) { return row - 71; }, nullEvery(7))}),
  };

  createDuckDbTable(vectors);

  // Set an artificially low limit on the amount of data to accumulate in
  // the partial aggregation.

  // Distinct aggregation.
  core::PlanNodeId aggNodeId;
  auto task = AssertQueryBuilder(duckDbQueryRunner_)
                  .config(QueryConfig::kMaxPartialAggregationMemory, 100)
                  .plan(PlanBuilder()
                            .values(vectors)
                            .partialAggregation({"c0"}, {})
                            .capturePlanNodeId(aggNodeId)
                            .finalAggregation()
                            .planNode())
                  .assertResults("SELECT distinct c0 FROM tmp");
  EXPECT_GT(
      toPlanStats(task->taskStats())
          .at(aggNodeId)
          .customStats.at("flushRowCount")
          .sum,
      0);
  EXPECT_GT(
      toPlanStats(task->taskStats())
          .at(aggNodeId)
          .customStats.at("flushRowCount")
          .max,
      0);

  // Count aggregation.
  task = AssertQueryBuilder(duckDbQueryRunner_)
             .config(QueryConfig::kMaxPartialAggregationMemory, 1)
             .plan(PlanBuilder()
                       .values(vectors)
                       .partialAggregation({"c0"}, {"count(1)"})
                       .capturePlanNodeId(aggNodeId)
                       .finalAggregation()
                       .planNode())
             .assertResults("SELECT c0, count(1) FROM tmp GROUP BY 1");
  EXPECT_GT(
      toPlanStats(task->taskStats())
          .at(aggNodeId)
          .customStats.at("flushRowCount")
          .count,
      0);
  EXPECT_GT(
      toPlanStats(task->taskStats())
          .at(aggNodeId)
          .customStats.at("flushRowCount")
          .max,
      0);

  // Global aggregation.
  task = AssertQueryBuilder(duckDbQueryRunner_)
             .config(QueryConfig::kMaxPartialAggregationMemory, 1)
             .plan(PlanBuilder()
                       .values(vectors)
                       .partialAggregation({}, {"sum(c0)"})
                       .capturePlanNodeId(aggNodeId)
                       .finalAggregation()
                       .planNode())
             .assertResults("SELECT sum(c0) FROM tmp");
  EXPECT_EQ(
      0,
      toPlanStats(task->taskStats())
          .at(aggNodeId)
          .customStats.count("flushRowCount"));
}

TEST_F(AggregationTest, partialDistinctWithAbandon) {
  auto vectors = {
      // 1st batch will produce 100 distinct groups from 10 rows.
      makeRowVector(
          {makeFlatVector<int32_t>(100, [](auto row) { return row; })}),
      // 2st batch will trigger abandon partial aggregation event with no new
      // distinct values.
      makeRowVector({makeFlatVector<int32_t>(1, [](auto row) { return row; })}),
      // 3rd batch will not produce any new distinct values.
      makeRowVector(
          {makeFlatVector<int32_t>(50, [](auto row) { return row; })}),
      // 4th batch will not produce 10 new distinct values.
      makeRowVector(
          {makeFlatVector<int32_t>(200, [](auto row) { return row % 110; })}),
  };

  createDuckDbTable(vectors);

  // We are setting abandon partial aggregation config properties to low values,
  // so they are triggered on the second batch.

  // Distinct aggregation.
  auto task = AssertQueryBuilder(duckDbQueryRunner_)
                  .config(QueryConfig::kAbandonPartialAggregationMinRows, 100)
                  .config(QueryConfig::kAbandonPartialAggregationMinPct, 50)
                  .maxDrivers(1)
                  .plan(PlanBuilder()
                            .values(vectors)
                            .partialAggregation({"c0"}, {})
                            .finalAggregation()
                            .planNode())
                  .assertResults("SELECT distinct c0 FROM tmp");

  // with aggregation, just in case.
  task = AssertQueryBuilder(duckDbQueryRunner_)
             .config(QueryConfig::kAbandonPartialAggregationMinRows, 100)
             .config(QueryConfig::kAbandonPartialAggregationMinPct, 50)
             .maxDrivers(1)
             .plan(PlanBuilder()
                       .values(vectors)
                       .partialAggregation({"c0"}, {"sum(c0)"})
                       .finalAggregation()
                       .planNode())
             .assertResults("SELECT distinct c0, sum(c0) FROM tmp group by c0");
}

TEST_F(AggregationTest, distinctWithGroupingKeysReordered) {
  rowType_ =
      ROW({"c0", "c1", "c2", "c3", "c4"},
          {BIGINT(),
           VARCHAR(),
           INTEGER(),
           ROW({"a0", "a1", "a2"}, {VARCHAR(), BOOLEAN(), BIGINT()}),
           BOOLEAN()});

  const int vectorSize = 2'000;
  VectorFuzzer::Options options;
  options.vectorSize = vectorSize;
  options.stringVariableLength = false;
  options.stringLength = 128;
  VectorFuzzer fuzzer(options, pool());
  const int numVectors{5};
  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(fuzzer.fuzzRow(rowType_));
  }

  createDuckDbTable(vectors);

  // Distinct aggregation with grouping key with larger prefix encoded size
  // first.
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  TestScopedSpillInjection scopedSpillInjection(100);
  auto task =
      AssertQueryBuilder(duckDbQueryRunner_)
          .config(QueryConfig::kAbandonPartialAggregationMinRows, 100)
          .config(QueryConfig::kAbandonPartialAggregationMinPct, 50)
          .spillDirectory(spillDirectory->getPath())
          .config(QueryConfig::kSpillEnabled, true)
          .config(QueryConfig::kAggregationSpillEnabled, true)
          .config(QueryConfig::kSpillPrefixSortEnabled, true)
          .maxDrivers(1)
          .plan(PlanBuilder()
                    .values(vectors)
                    .singleAggregation({"c4", "c1", "c3", "c2", "c0"}, {})
                    .planNode())
          .assertResults("SELECT distinct c4, c1, c3, c2, c0 FROM tmp");
}

TEST_F(AggregationTest, largeValueRangeArray) {
  // We have keys that map to integer range. The keys are
  // a little under max array hash table size apart. This wastes 16MB of
  // memory for the array hash table. Every batch will overflow the
  // max partial memory. We check that when detecting the first
  // overflow, the partial agg rehashes itself not to use a value
  // range array hash mode and will accept more batches without
  // flushing.
  std::string string1k;
  string1k.resize(1000);
  std::vector<RowVectorPtr> vectors;
  // Make two identical ectors. The first one overflows the max size
  // but gets rehashed to smaller by using value ids instead of
  // ranges. The next vector fits in the space made freed.
  for (auto i = 0; i < 2; ++i) {
    vectors.push_back(makeRowVector(
        {makeFlatVector<int64_t>(
             1000, [](auto row) { return row % 2 == 0 ? 100 : 1000000; }),
         makeFlatVector<StringView>(
             1000, [&](auto /*row*/) { return StringView(string1k); })}));
  }
  std::vector<RowVectorPtr> expected = {makeRowVector(
      {makeFlatVector<int64_t>({100, 1000000}),
       makeFlatVector<int64_t>({1000, 1000})})};

  core::PlanNodeId partialAggId;
  core::PlanNodeId finalAggId;
  auto op = PlanBuilder()
                .values({vectors})
                .partialAggregation({"c0"}, {"array_agg(c1)"})
                .capturePlanNodeId(partialAggId)
                .finalAggregation()
                .capturePlanNodeId(finalAggId)
                .project({"c0", "cardinality(a0) as l"})
                .planNode();
  auto task = test::assertQuery(op, expected);
  auto stats = toPlanStats(task->taskStats());
  auto runtimeStats = stats.at(partialAggId).customStats;

  // The partial agg is expected to exceed max size after the first batch and
  // see that it has an oversize range based array with just 2 entries. It is
  // then expected to change hash mode and rehash.
  EXPECT_EQ(1, runtimeStats.at("hashtable.numRehashes").count);

  // The partial agg is expected to flush just once. The final agg gets one
  // batch.
  EXPECT_EQ(1, stats.at(finalAggId).inputVectors);
}

TEST_F(AggregationTest, partialAggregationMemoryLimitIncrease) {
  constexpr int64_t kGB = 1 << 30;
  auto vectors = {
      makeRowVector({makeFlatVector<int32_t>(
          100, [](auto row) { return row; }, nullEvery(5))}),
      makeRowVector({makeFlatVector<int32_t>(
          110, [](auto row) { return row + 29; }, nullEvery(7))}),
      makeRowVector({makeFlatVector<int32_t>(
          90, [](auto row) { return row - 71; }, nullEvery(7))}),
  };

  createDuckDbTable(vectors);

  struct {
    int64_t initialPartialMemoryLimit;
    int64_t extendedPartialMemoryLimit;
    bool expectedPartialOutputFlush;
    bool expectedPartialAggregationMemoryLimitIncrease;

    std::string debugString() const {
      return fmt::format(
          "initialPartialMemoryLimit: {}, extendedPartialMemoryLimit: {}, expectedPartialOutputFlush: {}, expectedPartialAggregationMemoryLimitIncrease: {}",
          initialPartialMemoryLimit,
          extendedPartialMemoryLimit,
          expectedPartialOutputFlush,
          expectedPartialAggregationMemoryLimitIncrease);
    }
  } testSettings[] = {// Set with a large initial partial aggregation memory
                      // limit and expect no flush and memory limit bump.
                      {kGB, 2 * kGB, false, false},
                      // Set with a very small initial and extended partial
                      // aggregation memory limit.
                      {100, 100, true, false},
                      // Set with a very small initial partial aggregation
                      // memory limit but large extended memory limit.
                      {100, kGB, true, true}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    // Distinct aggregation.
    core::PlanNodeId aggNodeId;
    auto task = AssertQueryBuilder(duckDbQueryRunner_)
                    .config(
                        QueryConfig::kMaxPartialAggregationMemory,
                        std::to_string(testData.initialPartialMemoryLimit))
                    .config(
                        QueryConfig::kMaxExtendedPartialAggregationMemory,
                        std::to_string(testData.extendedPartialMemoryLimit))
                    .plan(PlanBuilder()
                              .values(vectors)
                              .partialAggregation({"c0"}, {})
                              .capturePlanNodeId(aggNodeId)
                              .finalAggregation()
                              .planNode())
                    .assertResults("SELECT distinct c0 FROM tmp");
    const auto runtimeStats =
        toPlanStats(task->taskStats()).at(aggNodeId).customStats;
    if (testData.expectedPartialOutputFlush > 0) {
      EXPECT_LT(0, runtimeStats.at("flushRowCount").count);
      EXPECT_LT(0, runtimeStats.at("flushRowCount").max);
      EXPECT_LT(0, runtimeStats.at("partialAggregationPct").max);
    } else {
      EXPECT_EQ(0, runtimeStats.count("flushRowCount"));
      EXPECT_EQ(0, runtimeStats.count("partialAggregationPct"));
    }
    if (testData.expectedPartialAggregationMemoryLimitIncrease) {
      EXPECT_LT(
          testData.initialPartialMemoryLimit,
          runtimeStats.at("maxExtendedPartialAggregationMemoryUsage").max);
      EXPECT_GE(
          testData.extendedPartialMemoryLimit,
          runtimeStats.at("maxExtendedPartialAggregationMemoryUsage").max);
    } else {
      EXPECT_EQ(
          0, runtimeStats.count("maxExtendedPartialAggregationMemoryUsage"));
    }
  }
}

TEST_F(AggregationTest, partialAggregationMaybeReservationReleaseCheck) {
  auto vectors = {
      makeRowVector({makeFlatVector<int32_t>(
          100, [](auto row) { return row; }, nullEvery(5))}),
      makeRowVector({makeFlatVector<int32_t>(
          110, [](auto row) { return row + 29; }, nullEvery(7))}),
      makeRowVector({makeFlatVector<int32_t>(
          90, [](auto row) { return row - 71; }, nullEvery(7))}),
  };

  createDuckDbTable(vectors);

  constexpr int64_t kGB = 1 << 30;
  const int64_t kMaxPartialMemoryUsage = 1 * kGB;
  // Make sure partial aggregation runs out of memory after first batch.
  CursorParameters params;
  params.queryCtx = core::QueryCtx::create(executor_.get());
  params.queryCtx->testingOverrideConfigUnsafe({
      {QueryConfig::kMaxPartialAggregationMemory,
       std::to_string(kMaxPartialMemoryUsage)},
      {QueryConfig::kMaxExtendedPartialAggregationMemory,
       std::to_string(kMaxPartialMemoryUsage)},
  });

  core::PlanNodeId aggNodeId;
  params.planNode = PlanBuilder()
                        .values(vectors)
                        .partialAggregation({"c0"}, {})
                        .capturePlanNodeId(aggNodeId)
                        .finalAggregation()
                        .planNode();
  auto task = assertQuery(params, "SELECT distinct c0 FROM tmp");
  const auto runtimeStats =
      toPlanStats(task->taskStats()).at(aggNodeId).customStats;
  EXPECT_EQ(0, runtimeStats.count("flushRowCount"));
  EXPECT_EQ(0, runtimeStats.count("maxExtendedPartialAggregationMemoryUsage"));
  EXPECT_EQ(0, runtimeStats.count("partialAggregationPct"));
  // Check all the reserved memory have been released.
  EXPECT_EQ(0, task->pool()->availableReservation());
  EXPECT_GT(kMaxPartialMemoryUsage, task->pool()->reservedBytes());
}

TEST_F(AggregationTest, spillAll) {
  auto inputs = makeVectors(rowType_, 100, 10);

  const auto numDistincts =
      AssertQueryBuilder(PlanBuilder()
                             .values(inputs)
                             .singleAggregation({"c0"}, {}, {})
                             .planNode())
          .copyResults(pool_.get())
          ->size();

  auto plan = PlanBuilder()
                  .values(inputs)
                  .singleAggregation({"c0"}, {"array_agg(c1)"})
                  .planNode();

  auto results = AssertQueryBuilder(plan).copyResults(pool_.get());

  for (int numPartitionBits : {1, 2, 3}) {
    auto tempDirectory = exec::test::TempDirectoryPath::create();
    auto queryCtx = core::QueryCtx::create(executor_.get());
    TestScopedSpillInjection scopedSpillInjection(100);
    auto task = AssertQueryBuilder(plan)
                    .spillDirectory(tempDirectory->getPath())
                    .config(QueryConfig::kSpillEnabled, true)
                    .config(QueryConfig::kAggregationSpillEnabled, true)
                    .config(
                        QueryConfig::kSpillNumPartitionBits,
                        std::to_string(numPartitionBits))
                    .assertResults(results);

    auto stats = task->taskStats().pipelineStats;
    ASSERT_LT(
        0, stats[0].operatorStats[1].runtimeStats[Operator::kSpillRuns].count);
    // Check spilled bytes.
    ASSERT_LT(0, stats[0].operatorStats[1].spilledInputBytes);
    ASSERT_LT(0, stats[0].operatorStats[1].spilledBytes);
    ASSERT_EQ(
        stats[0].operatorStats[1].spilledPartitions, 1 << numPartitionBits);
    // Verifies all the rows have been spilled.
    ASSERT_EQ(stats[0].operatorStats[1].spilledRows, numDistincts);
    OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
  }
}

// Verify number of memory allocations in the HashAggregation operator.
TEST_F(AggregationTest, memoryAllocations) {
  vector_size_t size = 1'024;
  std::vector<RowVectorPtr> data;
  for (auto i = 0; i < 10; ++i) {
    data.push_back(makeRowVector({
        makeFlatVector<int64_t>(size, [](auto row) { return row; }),
        makeFlatVector<int64_t>(size, [](auto row) { return row + 3; }),
    }));
  }

  createDuckDbTable(data);

  core::PlanNodeId projectNodeId;
  core::PlanNodeId aggNodeId;
  auto plan = PlanBuilder()
                  .values(data)
                  .project({"c0 + c1"})
                  .capturePlanNodeId(projectNodeId)
                  .singleAggregation({}, {"sum(p0)"})
                  .capturePlanNodeId(aggNodeId)
                  .planNode();

  auto task = assertQuery(plan, "SELECT sum(c0 + c1) FROM tmp");

  // Verify memory allocations. Aggregation should make 2 allocations: 1 for the
  // RowContainer holding single accumulator and 1 for the result.
  auto planStats = toPlanStats(task->taskStats());
  ASSERT_EQ(5, planStats.at(aggNodeId).numMemoryAllocations);

  plan = PlanBuilder()
             .values(data)
             .project({"c0", "c0 + c1"})
             .capturePlanNodeId(projectNodeId)
             .singleAggregation({"c0"}, {"sum(p1)"})
             .capturePlanNodeId(aggNodeId)
             .planNode();

  task = assertQuery(plan, "SELECT c0, sum(c0 + c1) FROM tmp GROUP BY 1");

  // Verify memory allocations. Aggregation should make 4 allocations: 1 for the
  // hash table, 1 for the RowContainer holding accumulators, 2 for results (1
  // for values of the grouping key column, 1 for sum column).
  planStats = toPlanStats(task->taskStats());
  ASSERT_EQ(8, planStats.at(aggNodeId).numMemoryAllocations);
}

TEST_F(AggregationTest, groupingSets) {
  vector_size_t size = 1'000;
  auto data = makeRowVector(
      {"k1", "k2", "a", "b"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row % 11; }),
          makeFlatVector<int64_t>(size, [](auto row) { return row % 17; }),
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<std::string>(
              size, [](auto row) { return std::string(row % 12, 'x'); }),
      });

  createDuckDbTable({data});

  auto plan =
      PlanBuilder()
          .values({data})
          .groupId({"k1", "k2"}, {{"k1"}, {"k2"}}, {"a", "b"})
          .singleAggregation(
              {"k1", "k2", "group_id"},
              {"count(1) as count_1", "sum(a) as sum_a", "max(b) as max_b"})
          .project({"k1", "k2", "count_1", "sum_a", "max_b"})
          .planNode();

  assertQuery(
      plan,
      "SELECT k1, k2, count(1), sum(a), max(b) FROM tmp GROUP BY GROUPING SETS ((k1), (k2))");

  // Distinct aggregations.
  plan = PlanBuilder()
             .values({data})
             .groupId({"k1", "k2"}, {{"k1"}, {"k2"}}, {})
             .singleAggregation({"k1", "k2", "group_id"}, {})
             .project({"k1", "k2"})
             .planNode();

  assertQuery(
      plan, "SELECT k1, k2 FROM tmp GROUP BY GROUPING SETS ((k1), (k2))");

  // Distinct aggregations with global grouping sets.
  plan = PlanBuilder()
             .values({data})
             .groupId({"k1", "k2"}, {{"k1"}, {"k2"}, {}}, {})
             .singleAggregation({"k1", "k2", "group_id"}, {})
             .project({"k1", "k2"})
             .planNode();

  assertQuery(
      plan, "SELECT k1, k2 FROM tmp GROUP BY GROUPING SETS ((k1), (k2), ())");

  // Compute a subset of aggregates per grouping set by using masks based on
  // group_id column.
  plan = PlanBuilder()
             .values({data})
             .groupId({"k1", "k2"}, {{"k1"}, {"k2"}}, {"a", "b"})
             .project(
                 {"k1",
                  "k2",
                  "group_id",
                  "a",
                  "b",
                  "group_id = 0 as mask_a",
                  "group_id = 1 as mask_b"})
             .singleAggregation(
                 {"k1", "k2", "group_id"},
                 {"count(1) as count_1", "sum(a) as sum_a", "max(b) as max_b"},
                 {"", "mask_a", "mask_b"})
             .project({"k1", "k2", "count_1", "sum_a", "max_b"})
             .planNode();

  assertQuery(
      plan,
      "SELECT k1, null, count(1), sum(a), null FROM tmp GROUP BY k1 "
      "UNION ALL "
      "SELECT null, k2, count(1), null, max(b) FROM tmp GROUP BY k2");

  // Cube.
  plan =
      PlanBuilder()
          .values({data})
          .groupId({"k1", "k2"}, {{"k1", "k2"}, {"k1"}, {"k2"}, {}}, {"a", "b"})
          .singleAggregation(
              {"k1", "k2", "group_id"},
              {"count(1) as count_1", "sum(a) as sum_a", "max(b) as max_b"})
          .project({"k1", "k2", "count_1", "sum_a", "max_b"})
          .planNode();

  assertQuery(
      plan,
      "SELECT k1, k2, count(1), sum(a), max(b) FROM tmp GROUP BY CUBE (k1, k2)");

  // Rollup.
  plan = PlanBuilder()
             .values({data})
             .groupId({"k1", "k2"}, {{"k1", "k2"}, {"k1"}, {}}, {"a", "b"})
             .singleAggregation(
                 {"k1", "k2", "group_id"},
                 {"count(1) as count_1", "sum(a) as sum_a", "max(b) as max_b"})
             .project({"k1", "k2", "count_1", "sum_a", "max_b"})
             .planNode();

  assertQuery(
      plan,
      "SELECT k1, k2, count(1), sum(a), max(b) FROM tmp GROUP BY ROLLUP (k1, k2)");
}

TEST_F(AggregationTest, groupingSetsOutput) {
  vector_size_t size = 1'000;
  auto data = makeRowVector(
      {"k1", "k2", "a", "b"},
      {
          makeFlatVector<int64_t>(size, [](auto row) { return row % 11; }),
          makeFlatVector<int64_t>(size, [](auto row) { return row % 17; }),
          makeFlatVector<int64_t>(size, [](auto row) { return row; }),
          makeFlatVector<std::string>(
              size, [](auto row) { return std::string(row % 12, 'x'); }),
      });

  createDuckDbTable({data});

  core::PlanNodePtr reversedOrderGroupIdNode;
  core::PlanNodePtr orderGroupIdNode;
  auto reversedOrderPlan =
      PlanBuilder()
          .values({data})
          .groupId({"k2", "k1"}, {{"k2", "k1"}, {}}, {"a", "b"})
          .capturePlanNode(reversedOrderGroupIdNode)
          .singleAggregation(
              {"k2", "k1", "group_id"},
              {"count(1) as count_1", "sum(a) as sum_a", "max(b) as max_b"})
          .project({"k1", "k2", "count_1", "sum_a", "max_b"})
          .planNode();

  auto orderPlan =
      PlanBuilder()
          .values({data})
          .groupId({"k1", "k2"}, {{"k1", "k2"}, {}}, {"a", "b"})
          .capturePlanNode(orderGroupIdNode)
          .singleAggregation(
              {"k1", "k2", "group_id"},
              {"count(1) as count_1", "sum(a) as sum_a", "max(b) as max_b"})
          .project({"k1", "k2", "count_1", "sum_a", "max_b"})
          .planNode();

  auto reversedOrderExpectedRowType =
      ROW({"k2", "k1", "a", "b", "group_id"},
          {BIGINT(), BIGINT(), BIGINT(), VARCHAR(), BIGINT()});
  auto orderExpectedRowType =
      ROW({"k1", "k2", "a", "b", "group_id"},
          {BIGINT(), BIGINT(), BIGINT(), VARCHAR(), BIGINT()});
  ASSERT_EQ(
      *reversedOrderGroupIdNode->outputType(), *reversedOrderExpectedRowType);
  ASSERT_EQ(*orderGroupIdNode->outputType(), *orderExpectedRowType);

  CursorParameters orderParams;
  orderParams.planNode = orderPlan;
  auto orderResult = readCursor(orderParams);

  CursorParameters reversedOrderParams;
  reversedOrderParams.planNode = reversedOrderPlan;
  auto reversedOrderResult = readCursor(reversedOrderParams);

  assertEqualResults(orderResult.second, reversedOrderResult.second);
}

TEST_F(AggregationTest, groupingSetsSameKey) {
  auto data = makeRowVector(
      {"o_key", "o_status"},
      {makeFlatVector<int64_t>({0, 1, 2, 3, 4}),
       makeFlatVector<std::string>({"", "x", "xx", "xxx", "xxxx"})});

  createDuckDbTable({data});

  auto plan = PlanBuilder()
                  .values({data})
                  .groupId(
                      {"o_key", "o_key as o_key_1"},
                      {{"o_key", "o_key_1"}, {"o_key"}, {"o_key_1"}, {}},
                      {"o_status"})
                  .singleAggregation(
                      {"o_key", "o_key_1", "group_id"},
                      {"max(o_status) as max_o_status"})
                  .project({"o_key", "o_key_1", "max_o_status"})
                  .planNode();

  assertQuery(
      plan,
      "SELECT o_key, o_key_1, max(o_status) as max_o_status FROM ("
      "select o_key, o_key as o_key_1, o_status FROM tmp) GROUP BY GROUPING SETS ((o_key, o_key_1), (o_key), (o_key_1), ())");
}

TEST_F(AggregationTest, groupingSetsEmptyInput) {
  auto data = makeRowVector(
      {"c1", "c2"},
      {makeFlatVector<int64_t>({0, 1, 2, 3, 4}),
       makeFlatVector<std::string>({"", "x", "xx", "xxx", "xxxx"})});

  createDuckDbTable({data});

  auto plan =
      PlanBuilder()
          .values({data})
          .filter("c1 < 0")
          .groupId({"c1"}, {{"c1"}, {}}, {"c2"})
          .singleAggregation({"c1", "group_id"}, {"count(c2) as count_c2"}, {})
          .project({"count_c2"})
          .planNode();

  assertQuery(
      plan,
      "SELECT count(c2) as count_c2 FROM tmp WHERE c1 < 0 GROUP BY GROUPING SETS ((c1), ())");

  plan =
      PlanBuilder()
          .values({data})
          .filter("c1 < 0")
          .groupId({"c1"}, {{"c1"}, {}}, {"c2"})
          .partialAggregation({"c1", "group_id"}, {"count(c2) as count_c2"}, {})
          .finalAggregation()
          .project({"count_c2"})
          .planNode();

  assertQuery(
      plan,
      "SELECT count(c2) as count_c2 FROM tmp WHERE c1 < 0 GROUP BY GROUPING SETS ((c1), ())");

  plan =
      PlanBuilder()
          .values({data})
          .filter("c1 < 0")
          .groupId({"c1"}, {{"c1"}, {}}, {"c2"})
          .partialAggregation({"c1", "group_id"}, {"count(c2) as count_c2"}, {})
          .intermediateAggregation()
          .finalAggregation()
          .project({"count_c2"})
          .planNode();

  assertQuery(
      plan,
      "SELECT count(c2) as count_c2 FROM tmp WHERE c1 < 0 GROUP BY GROUPING SETS ((c1), ())");

  // Distinct aggregations with GROUPING SETS.
  plan = PlanBuilder()
             .values({data})
             .filter("c1 < 0")
             .groupId({"c1"}, {{"c1"}, {}}, {})
             .partialAggregation({"c1", "group_id"}, {}, {})
             .finalAggregation()
             .project({"c1"})
             .planNode();

  assertQuery(
      plan,
      "SELECT c1 FROM tmp WHERE c1 < 0 GROUP BY GROUPING SETS ((c1), ())");

  plan =
      PlanBuilder()
          .values({data})
          .filter("c1 < 0")
          .groupId({"c1"}, {{}, {}}, {"c2"})
          .partialAggregation({"c1", "group_id"}, {"count(c2) as count_c2"}, {})
          .intermediateAggregation()
          .finalAggregation()
          .project({"count_c2"})
          .planNode();

  assertQuery(plan, makeRowVector({makeFlatVector<int64_t>({0, 0})}));

  // Distinct aggregations over empty input with global GROUPING SETs.
  plan = PlanBuilder()
             .values({data})
             .filter("c1 < 0")
             .groupId({"c1"}, {{}, {}}, {})
             .singleAggregation({"c1", "group_id"}, {}, {})
             .planNode();

  assertQuery(
      plan,
      makeRowVector({
          makeAllNullFlatVector<int64_t>(2),
          makeFlatVector<int64_t>({0, 1}),
      }));

  // Aggregations over distinct inputs over empty input with global grouping
  // sets.
  plan = PlanBuilder()
             .values({data})
             .filter("c1 < 0")
             .groupId({"c1"}, {{}, {}}, {"c2"})
             .singleAggregation(
                 {"c1", "group_id"}, {"count(distinct c2)", "min(distinct c2)"})
             .planNode();

  assertQuery(
      plan,
      makeRowVector({
          makeAllNullFlatVector<int64_t>(2),
          makeFlatVector<int64_t>({0, 1}),
          makeFlatVector<int64_t>({0, 0}),
          makeAllNullFlatVector<std::string>(2),
      }));

  // Aggregations over sorted inputs over empty input with global grouping sets.
  plan =
      PlanBuilder()
          .values({data})
          .filter("c1 < 0")
          .groupId({"c1"}, {{}, {}}, {"c2"})
          .singleAggregation({"c1", "group_id"}, {"array_agg(c2 order by c2)"})
          .planNode();

  assertQuery(
      plan,
      makeRowVector({
          makeAllNullFlatVector<int64_t>(2),
          makeFlatVector<int64_t>({0, 1}),
          makeAllNullArrayVector(2, VARCHAR()),
      }));
}

TEST_F(AggregationTest, disableNonBooleanMasks) {
  auto data = makeRowVector(
      {"c0", "c1"},
      {makeFlatVector<int64_t>({1, -1, 0, -2, 10}),
       makeFlatVector<std::string>({"a", "a", "b", "c", "a"})});

  auto plan = PlanBuilder()
                  .values({data})
                  .aggregation(
                      {"c1"},
                      {"count(c0) FILTER(WHERE c0)"},
                      {},
                      core::AggregationNode::Step::kPartial,
                      false)
                  .planNode();

  VELOX_ASSERT_USER_THROW(
      AssertQueryBuilder(plan).copyResults(pool()),
      "FILTER(WHERE..) clause must use masks that are BOOLEAN");

  // Planbuilder doesnt allow expressions in FILTER clauses
  plan = PlanBuilder()
             .values({data})
             .project({"c0", "c1", "c0 > 0 as mask"})
             .aggregation(
                 {"c1"},
                 {"count(c0) FILTER(WHERE mask)"},
                 {},
                 core::AggregationNode::Step::kPartial,
                 true)
             .planNode();

  AssertQueryBuilder(plan).copyResults(pool());
}

TEST_F(AggregationTest, outputBatchSizeCheckWithSpill) {
  const int numVectors = 5;
  const int vectorSize = 20;
  const std::string strValue(1L << 20, 'a');

  std::vector<RowVectorPtr> largeVectors;
  std::vector<RowVectorPtr> smallVectors;
  for (int i = 0; i < numVectors; ++i) {
    largeVectors.push_back(makeRowVector(
        {makeFlatVector<int32_t>(
             vectorSize, [&](auto row) { return i * vectorSize + row; }),
         makeFlatVector<StringView>(vectorSize, [&](auto /*unused*/) {
           return StringView(strValue);
         })}));
    smallVectors.push_back(makeRowVector(
        {makeFlatVector<int32_t>(
             vectorSize, [&](auto row) { return i * vectorSize + row; }),
         makeFlatVector<int32_t>(
             vectorSize, [&](auto row) { return i * vectorSize + row; })}));
  }
  auto largeRowType = asRowType(largeVectors.back()->type());
  auto smallRowType = asRowType(smallVectors.back()->type());

  struct {
    bool smallInput;
    uint32_t maxOutputRows;
    uint32_t maxOutputBytes;
    uint32_t expectedNumOutputVectors;

    std::string debugString() const {
      return fmt::format(
          "smallInput: {} maxOutputRows: {}, maxOutputBytes: {}, expectedNumOutputVectors: {}",
          smallInput,
          maxOutputRows,
          succinctBytes(maxOutputBytes),
          expectedNumOutputVectors);
    }
  } testSettings[] = {
      {true, 1000, 1000'000, 1},
      {true, 10, 1000'000, 10},
      {true, 1, 1000'000, 100},
      {true, 1, 1, 100},
      {true, 10, 1, 100},
      {true, 100, 1, 100},
      {true, 1000, 1, 100},
      {false, 1000, 1, 100},
      {false, 1000, 1000'000'000, 1},
      {false, 100, 1000'000'000, 1},
      {false, 10, 1000'000'000, 10},
      {false, 1, 1000'000'000, 100},
      {false, 1, 1, 100}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    std::vector<RowVectorPtr> inputs;
    if (testData.smallInput) {
      inputs = smallVectors;
    } else {
      inputs = largeVectors;
    }
    createDuckDbTable(inputs);
    auto tempDirectory = exec::test::TempDirectoryPath::create();
    core::PlanNodeId aggrNodeId;
    TestScopedSpillInjection scopedSpillInjection(100);
    auto task =
        AssertQueryBuilder(duckDbQueryRunner_)
            .spillDirectory(tempDirectory->getPath())
            .config(QueryConfig::kSpillEnabled, true)
            .config(QueryConfig::kAggregationSpillEnabled, true)
            .config(
                QueryConfig::kPreferredOutputBatchBytes,
                std::to_string(testData.maxOutputBytes))
            .config(
                QueryConfig::kMaxOutputBatchRows,
                std::to_string(testData.maxOutputRows))
            .plan(PlanBuilder()
                      .values(inputs)
                      .singleAggregation({"c0"}, {"array_agg(c1)"})
                      .capturePlanNodeId(aggrNodeId)
                      .planNode())
            .assertResults("SELECT c0, array_agg(c1) FROM tmp GROUP BY 1");
    ASSERT_GT(toPlanStats(task->taskStats()).at(aggrNodeId).spilledBytes, 0);
    ASSERT_EQ(
        toPlanStats(task->taskStats()).at(aggrNodeId).outputVectors,
        testData.expectedNumOutputVectors);
    OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
  }
}

TEST_F(AggregationTest, outputBatchSizeCheckWithSpillForOrderedAggr) {
  const int numVectors = 5;
  const int vectorSize = 20;
  const std::string strValue(1L << 20, 'a'); // 1MB

  std::vector<RowVectorPtr> vectors;
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(makeRowVector(
        {makeFlatVector<int32_t>(vectorSize, [&](auto row) { return row % 5; }),
         makeFlatVector<StringView>(vectorSize, [&](auto /*unused*/) {
           return StringView(strValue);
         })}));
  }
  auto rowType = asRowType(vectors.back()->type());

  struct {
    vector_size_t maxOutputRows;
    uint32_t maxOutputBytes;
    uint32_t expectedNumOutputVectors;

    std::string debugString() const {
      return fmt::format(
          "maxOutputRows: {}, maxOutputBytes: {}, expectedNumOutputVectors: {}",
          maxOutputRows,
          succinctBytes(maxOutputBytes),
          expectedNumOutputVectors);
    }
  } testSettings[] = {
      {1, std::numeric_limits<uint32_t>::max(), 5},
      {std::numeric_limits<vector_size_t>::max(), 15L << 20, 5},
      {std::numeric_limits<vector_size_t>::max(), 35L << 20, 3}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    createDuckDbTable(vectors);
    auto tempDirectory = exec::test::TempDirectoryPath::create();
    core::PlanNodeId aggrNodeId;
    TestScopedSpillInjection scopedSpillInjection(100);
    auto task =
        AssertQueryBuilder(duckDbQueryRunner_)
            .spillDirectory(tempDirectory->getPath())
            .config(QueryConfig::kSpillEnabled, true)
            .config(QueryConfig::kAggregationSpillEnabled, true)
            .config(
                QueryConfig::kPreferredOutputBatchBytes,
                std::to_string(testData.maxOutputBytes))
            .config(
                QueryConfig::kMaxOutputBatchRows,
                std::to_string(testData.maxOutputRows))
            .plan(PlanBuilder()
                      .values(vectors)
                      .singleAggregation({"c0"}, {"array_agg(c1 order by c1)"})
                      .capturePlanNodeId(aggrNodeId)
                      .planNode())
            .assertResults(
                "SELECT c0, array_agg(c1 order by c1) FROM tmp GROUP BY 1");
    ASSERT_GT(toPlanStats(task->taskStats()).at(aggrNodeId).spilledBytes, 0);
    ASSERT_EQ(
        toPlanStats(task->taskStats()).at(aggrNodeId).outputVectors,
        testData.expectedNumOutputVectors);
    OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
  }
}

TEST_F(AggregationTest, spillDuringOutputProcessing) {
  rowType_ = ROW(
      {"c0", "c1", "c2", "c3"}, {INTEGER(), INTEGER(), VARCHAR(), VARCHAR()});

  const int vectorSize = 2'000;
  VectorFuzzer::Options options;
  options.vectorSize = vectorSize;
  options.stringVariableLength = false;
  options.stringLength = 128;
  VectorFuzzer fuzzer(options, pool());
  RowVectorPtr input = fuzzer.fuzzRow(rowType_);

  createDuckDbTable({input});

  const int numOutputRows = 5;
  auto tempDirectory = exec::test::TempDirectoryPath::create();
  core::PlanNodeId aggrNodeId;
  TestScopedSpillInjection scopedSpillInjection(100);
  auto task =
      AssertQueryBuilder(duckDbQueryRunner_)
          .spillDirectory(tempDirectory->getPath())
          .config(QueryConfig::kSpillEnabled, true)
          .config(QueryConfig::kAggregationSpillEnabled, true)
          // Set very large output buffer size, the number of output rows is
          // effectively controlled by 'kPreferredOutputBatchBytes'.
          .config(
              QueryConfig::kPreferredOutputBatchBytes,
              std::to_string(1'000'000'000))
          .config(
              QueryConfig::kMaxOutputBatchRows, std::to_string(numOutputRows))
          .config(QueryConfig::kSpillNumPartitionBits, "0")
          .plan(PlanBuilder()
                    .values({input})
                    .singleAggregation({"c0", "c1"}, {"max(c2)", "min(c3)"})
                    .capturePlanNodeId(aggrNodeId)
                    .planNode())
          .assertResults(
              "SELECT c0, c1, max(c2), min(c3) FROM tmp GROUP BY 1, 2");

  ASSERT_EQ(
      toPlanStats(task->taskStats()).at(aggrNodeId).outputVectors,
      vectorSize / numOutputRows);
  ASSERT_GT(toPlanStats(task->taskStats()).at(aggrNodeId).spilledBytes, 0);
  // There is only one partition for spilling triggered during output stage.
  ASSERT_EQ(toPlanStats(task->taskStats()).at(aggrNodeId).spilledPartitions, 1);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(AggregationTest, outputBatchSizeCheckWithoutSpill) {
  const int vectorSize = 100;
  const std::string strValue(1L << 20, 'a');

  RowVectorPtr largeVector = makeRowVector(
      {makeFlatVector<int32_t>(vectorSize, [&](auto row) { return row; }),
       makeFlatVector<StringView>(
           vectorSize, [&](auto /*unused*/) { return StringView(strValue); })});
  auto largeRowType = asRowType(largeVector->type());

  RowVectorPtr smallVector = makeRowVector(
      {makeFlatVector<int32_t>(vectorSize, [&](auto row) { return row; }),
       makeFlatVector<int32_t>(vectorSize, [&](auto row) { return row; })});
  auto smallRowType = asRowType(smallVector->type());

  struct {
    bool smallInput;
    uint32_t maxOutputRows;
    uint32_t maxOutputBytes;
    uint32_t expectedNumOutputVectors;

    std::string debugString() const {
      return fmt::format(
          "smallInput: {} maxOutputRows: {}, maxOutputBytes: {}, expectedNumOutputVectors: {}",
          smallInput,
          maxOutputRows,
          succinctBytes(maxOutputBytes),
          expectedNumOutputVectors);
    }
  } testSettings[] = {
      {true, 1000, 1000'000, 1},
      {true, 10, 1000'000, 10},
      {true, 1, 1000'000, 100},
      {true, 1, 1, 100},
      {true, 10, 1, 100},
      {true, 100, 1, 100},
      {true, 1000, 1, 100},
      {false, 1000, 1, 100},
      {false, 1000, 1000'000'000, 1},
      {false, 100, 1000'000'000, 1},
      {false, 10, 1000'000'000, 10},
      {false, 1, 1000'000'000, 100},
      {false, 1, 1, 100}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    std::vector<RowVectorPtr> inputs;
    if (testData.smallInput) {
      inputs.push_back(smallVector);
    } else {
      inputs.push_back(largeVector);
    }
    createDuckDbTable(inputs);
    core::PlanNodeId aggrNodeId;
    auto task =
        AssertQueryBuilder(duckDbQueryRunner_)
            .config(
                QueryConfig::kPreferredOutputBatchBytes,
                std::to_string(testData.maxOutputBytes))
            .config(
                QueryConfig::kMaxOutputBatchRows,
                std::to_string(testData.maxOutputRows))
            .plan(PlanBuilder()
                      .values(inputs)
                      .singleAggregation({"c0"}, {"array_agg(c1)"})
                      .capturePlanNodeId(aggrNodeId)
                      .planNode())
            .assertResults("SELECT c0, array_agg(c1) FROM tmp GROUP BY 1");

    ASSERT_EQ(
        toPlanStats(task->taskStats()).at(aggrNodeId).outputVectors,
        testData.expectedNumOutputVectors);
  }
}

DEBUG_ONLY_TEST_F(AggregationTest, minSpillableMemoryReservation) {
  rowType_ = ROW(
      {"c0", "c1", "c2", "c3"}, {INTEGER(), INTEGER(), VARCHAR(), VARCHAR()});
  VectorFuzzer::Options options;
  options.vectorSize = 100;
  options.stringVariableLength = false;
  options.stringLength = 1024;
  VectorFuzzer fuzzer(options, pool());
  const int32_t numBatches = 50;
  std::vector<RowVectorPtr> batches;
  for (int32_t i = 0; i < numBatches; ++i) {
    batches.push_back(fuzzer.fuzzRow(rowType_));
  }

  createDuckDbTable(batches);

  for (int32_t minSpillableReservationPct : {5, 50, 100}) {
    SCOPED_TRACE(fmt::format(
        "minSpillableReservationPct: {}", minSpillableReservationPct));

    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::GroupingSet::addInputForActiveRows",
        std::function<void(exec::GroupingSet*)>(
            ([&](exec::GroupingSet* groupingSet) {
              memory::MemoryPool& pool = groupingSet->testingPool();
              const auto availableReservationBytes =
                  pool.availableReservation();
              const auto currentUsedBytes = pool.usedBytes();
              // Verifies we always have min reservation after ensuring the
              // input.
              ASSERT_GE(
                  availableReservationBytes,
                  currentUsedBytes * minSpillableReservationPct / 100);
            })));

    auto spillDirectory = exec::test::TempDirectoryPath::create();
    auto task =
        AssertQueryBuilder(duckDbQueryRunner_)
            .spillDirectory(spillDirectory->getPath())
            .config(QueryConfig::kSpillEnabled, true)
            .config(QueryConfig::kAggregationSpillEnabled, true)
            .config(
                QueryConfig::kMinSpillableReservationPct,
                std::to_string(minSpillableReservationPct))
            .config(
                QueryConfig::kSpillableReservationGrowthPct,
                std::to_string(minSpillableReservationPct + 1))
            .plan(PlanBuilder()
                      .values(batches)
                      .singleAggregation({"c0"}, {"array_agg(c2)", "max(c3)"})
                      .planNode())
            .assertResults(
                "SELECT c0, array_agg(c2), max(c3) FROM tmp GROUP BY 1");
    OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
  }
}

TEST_F(AggregationTest, distinctWithSpilling) {
  struct TestParam {
    std::vector<RowVectorPtr> inputs;
    uint32_t expectedSpilledPartitions;
    std::function<void(uint32_t)> expectedSpillFilesCheck{nullptr};
  };

  std::vector<TestParam> testParams{
      {makeVectors(rowType_, 10, 100),
       8,
       [](uint32_t spilledFiles) { ASSERT_GE(spilledFiles, 100); }},

      {{makeRowVector(
           {"c0"},
           {makeFlatVector<int64_t>(
               2'000, [](vector_size_t /* unused */) { return 100; })})},
       1,
       [](uint32_t spilledFiles) { ASSERT_EQ(spilledFiles, 1); }}};

  for (const auto& testParam : testParams) {
    createDuckDbTable(testParam.inputs);
    auto spillDirectory = exec::test::TempDirectoryPath::create();
    core::PlanNodeId aggrNodeId;
    TestScopedSpillInjection scopedSpillInjection(100);
    auto task = AssertQueryBuilder(duckDbQueryRunner_)
                    .spillDirectory(spillDirectory->getPath())
                    .config(QueryConfig::kSpillEnabled, true)
                    .config(QueryConfig::kAggregationSpillEnabled, true)
                    .plan(PlanBuilder()
                              .values(testParam.inputs)
                              .singleAggregation({"c0"}, {}, {})
                              .capturePlanNodeId(aggrNodeId)
                              .planNode())
                    .assertResults("SELECT distinct c0 FROM tmp");

    // Verify that spilling is not triggered.
    const auto planNodeStatsMap = toPlanStats(task->taskStats());
    const auto& aggrNodeStats = planNodeStatsMap.at(aggrNodeId);
    ASSERT_GT(aggrNodeStats.spilledInputBytes, 0);
    ASSERT_EQ(
        aggrNodeStats.spilledPartitions, testParam.expectedSpilledPartitions);
    ASSERT_GT(aggrNodeStats.spilledBytes, 0);
    testParam.expectedSpillFilesCheck(aggrNodeStats.spilledFiles);
    OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
  }
}

TEST_F(AggregationTest, spillingForAggrsWithDistinct) {
  auto vectors = makeVectors(rowType_, 100, 10);
  createDuckDbTable(vectors);
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  core::PlanNodeId aggrNodeId;
  TestScopedSpillInjection scopedSpillInjection(100);
  auto task =
      AssertQueryBuilder(duckDbQueryRunner_)
          .spillDirectory(spillDirectory->getPath())
          .config(QueryConfig::kSpillEnabled, true)
          .config(QueryConfig::kAggregationSpillEnabled, true)
          .plan(PlanBuilder()
                    .values(vectors)
                    .singleAggregation({"c1"}, {"count(DISTINCT c0)"}, {})
                    .capturePlanNodeId(aggrNodeId)
                    .planNode())
          .assertResults("SELECT c1, count(DISTINCT c0) FROM tmp GROUP BY c1");
  // Verify that spilling is not triggered.
  const auto& queryConfig = task->queryCtx()->queryConfig();
  ASSERT_TRUE(queryConfig.spillEnabled());
  ASSERT_TRUE(queryConfig.aggregationSpillEnabled());
  ASSERT_EQ(toPlanStats(task->taskStats()).at(aggrNodeId).spilledBytes, 0);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(AggregationTest, spillingForAggrsWithSorting) {
  auto vectors = makeVectors(rowType_, 100, 10);
  createDuckDbTable(vectors);
  auto spillDirectory = exec::test::TempDirectoryPath::create();

  core::PlanNodeId aggrNodeId;

  auto testPlan = [&](const core::PlanNodePtr& plan, const std::string& sql) {
    SCOPED_TRACE(sql);
    TestScopedSpillInjection scopedSpillInjection(100);
    auto task = AssertQueryBuilder(duckDbQueryRunner_)
                    .spillDirectory(spillDirectory->getPath())
                    .config(QueryConfig::kSpillEnabled, true)
                    .config(QueryConfig::kAggregationSpillEnabled, true)
                    .plan(plan)
                    .assertResults(sql);

    auto taskStats = exec::toPlanStats(task->taskStats());
    auto& stats = taskStats.at(aggrNodeId);
    checkSpillStats(stats, true);
    OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
  };

  auto plan = PlanBuilder()
                  .values(vectors)
                  .singleAggregation({"c0"}, {"array_agg(c1 ORDER BY c1)"}, {})
                  .capturePlanNodeId(aggrNodeId)
                  .planNode();
  testPlan(plan, "SELECT c0, array_agg(c1 ORDER BY c1) FROM tmp GROUP BY 1");

  plan = PlanBuilder()
             .values(vectors)
             .project({"c0 % 7", "c1"})
             .singleAggregation({"p0"}, {"array_agg(c1 ORDER BY c1)"}, {})
             .capturePlanNodeId(aggrNodeId)
             .planNode();
  testPlan(
      plan, "SELECT c0 % 7, array_agg(c1 ORDER BY c1) FROM tmp GROUP BY 1");
}

TEST_F(AggregationTest, spillPrefixSortOptimization) {
  const RowTypePtr rowType{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"},
          {BIGINT(),
           SMALLINT(),
           VARCHAR(),
           TINYINT(),
           INTEGER(),
           BIGINT(),
           REAL(),
           DOUBLE(),
           VARCHAR()})};
  auto vectors = makeVectors(rowType, 1024, 2);
  int64_t groupingKeyValue{0};
  for (auto& vector : vectors) {
    auto groupingVector = BaseVector::create(
        vector->childAt(0)->type(), vector->childAt(0)->size(), pool_.get());
    auto* flatGroupingKeyVector = groupingVector->asFlatVector<int64_t>();
    for (auto i = 0; i < flatGroupingKeyVector->size(); ++i) {
      flatGroupingKeyVector->set(i, groupingKeyValue++);
    }
    vector->childAt(0) = groupingVector;
  }

  createDuckDbTable(vectors);
  struct {
    bool prefixSortSpillEnabled;
    uint32_t maxNormalizedKeyBytes;
    uint32_t minNumRows;
    uint32_t expectedNumPrefixSortKeys;

    std::string debugString() const {
      return fmt::format(
          "prefixSortSpillEnabled {}, maxNormalizedKeyBytes {}, minNumRows {}, expectedNumPrefixSortKeys {}",
          prefixSortSpillEnabled,
          maxNormalizedKeyBytes,
          minNumRows,
          expectedNumPrefixSortKeys);
    }
  } testSettings[] = {
      {true, 0, 0, 0},
      {false, 0, 0, 0},
      {true,
       prefixsort::PrefixSortEncoder::encodedSize(TypeKind::SMALLINT, 12, false)
               .value() -
           1,
       0,
       0},
      {false,
       prefixsort::PrefixSortEncoder::encodedSize(TypeKind::SMALLINT, 12, false)
               .value() -
           1,
       0,
       0},
      {true,
       prefixsort::PrefixSortEncoder::encodedSize(TypeKind::SMALLINT, 12, false)
           .value(),
       0,
       1},
      {false,
       prefixsort::PrefixSortEncoder::encodedSize(TypeKind::SMALLINT, 12, false)
           .value(),
       0,
       0},
      {true, 1'000'000, 0, 4},
      {false, 1'000'000, 0, 0},
      {true, 1'000'000, 1'000'000, 0},
      {false, 1'000'000, 1'000'000, 0},
      {true,
       prefixsort::PrefixSortEncoder::encodedSize(TypeKind::SMALLINT, 12, false)
               .value() +
           prefixsort::PrefixSortEncoder::encodedSize(
               TypeKind::INTEGER, 12, false)
               .value(),
       0,
       2},
      {false,
       prefixsort::PrefixSortEncoder::encodedSize(TypeKind::SMALLINT, 12, false)
               .value() +
           prefixsort::PrefixSortEncoder::encodedSize(
               TypeKind::INTEGER, 12, false)
               .value(),
       0,
       0},
      {true,
       prefixsort::PrefixSortEncoder::encodedSize(TypeKind::SMALLINT, 12, false)
               .value() +
           prefixsort::PrefixSortEncoder::encodedSize(
               TypeKind::INTEGER, 12, false)
               .value(),
       1'000'000,
       0},
      {false,
       prefixsort::PrefixSortEncoder::encodedSize(TypeKind::SMALLINT, 12, false)
               .value() +
           prefixsort::PrefixSortEncoder::encodedSize(
               TypeKind::INTEGER, 12, false)
               .value(),
       1'000'000,
       0},
      {true,
       prefixsort::PrefixSortEncoder::encodedSize(TypeKind::SMALLINT, 12, false)
               .value() +
           prefixsort::PrefixSortEncoder::encodedSize(
               TypeKind::BIGINT, 12, false)
               .value(),
       0,
       2},
      {false,
       prefixsort::PrefixSortEncoder::encodedSize(TypeKind::SMALLINT, 12, false)
               .value() +
           prefixsort::PrefixSortEncoder::encodedSize(
               TypeKind::BIGINT, 12, false)
               .value(),
       0,
       0},
      {true,
       prefixsort::PrefixSortEncoder::encodedSize(TypeKind::SMALLINT, 12, false)
               .value() +
           prefixsort::PrefixSortEncoder::encodedSize(
               TypeKind::BIGINT, 12, false)
               .value() +
           prefixsort::PrefixSortEncoder::encodedSize(
               TypeKind::INTEGER, 12, false)
               .value() -
           1,
       0,
       2},
      {false,
       prefixsort::PrefixSortEncoder::encodedSize(TypeKind::SMALLINT, 12, false)
               .value() +
           prefixsort::PrefixSortEncoder::encodedSize(
               TypeKind::BIGINT, 12, false)
               .value() +
           prefixsort::PrefixSortEncoder::encodedSize(
               TypeKind::INTEGER, 12, false)
               .value() -
           1,
       0,
       0},
      {true,
       prefixsort::PrefixSortEncoder::encodedSize(TypeKind::SMALLINT, 12, false)
               .value() +
           prefixsort::PrefixSortEncoder::encodedSize(
               TypeKind::BIGINT, 12, false)
               .value() +
           prefixsort::PrefixSortEncoder::encodedSize(
               TypeKind::INTEGER, 12, false)
               .value(),
       0,
       3},
      {false,
       prefixsort::PrefixSortEncoder::encodedSize(TypeKind::SMALLINT, 12, false)
               .value() +
           prefixsort::PrefixSortEncoder::encodedSize(
               TypeKind::BIGINT, 12, false)
               .value() +
           prefixsort::PrefixSortEncoder::encodedSize(
               TypeKind::INTEGER, 12, false)
               .value(),
       0,
       0}};

  for (const auto& testData : testSettings) {
    auto spillDirectory = exec::test::TempDirectoryPath::create();

    core::PlanNodeId aggrNodeId;

    auto testPlan = [&](const core::PlanNodePtr& plan, const std::string& sql) {
      SCOPED_TRACE(sql);
      TestScopedSpillInjection scopedSpillInjection(100);
      auto task = AssertQueryBuilder(duckDbQueryRunner_)
                      .spillDirectory(spillDirectory->getPath())
                      .config(QueryConfig::kSpillEnabled, true)
                      .config(QueryConfig::kAggregationSpillEnabled, true)
                      .config(
                          QueryConfig::kSpillPrefixSortEnabled,
                          testData.prefixSortSpillEnabled)
                      .config(
                          QueryConfig::kPrefixSortMinRows,
                          std::to_string(testData.minNumRows))
                      .config(
                          QueryConfig::kPrefixSortNormalizedKeyMaxBytes,
                          std::to_string(testData.maxNormalizedKeyBytes))
                      .plan(plan)
                      .assertResults(sql);

      auto taskStats = exec::toPlanStats(task->taskStats());
      auto& stats = taskStats.at(aggrNodeId);
      checkSpillStats(stats, true);
      if (testData.expectedNumPrefixSortKeys > 0) {
        ASSERT_GE(
            stats.customStats.at(PrefixSort::kNumPrefixSortKeys).sum,
            testData.expectedNumPrefixSortKeys);
        ASSERT_EQ(
            stats.customStats.at(PrefixSort::kNumPrefixSortKeys).max,
            testData.expectedNumPrefixSortKeys);
        ASSERT_EQ(
            stats.customStats.at(PrefixSort::kNumPrefixSortKeys).min,
            testData.expectedNumPrefixSortKeys);
      } else {
        ASSERT_EQ(stats.customStats.count(PrefixSort::kNumPrefixSortKeys), 0);
      }
      OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
    };

    auto plan = PlanBuilder()
                    .values(vectors)
                    .singleAggregation(
                        {"c8", "c2", "c1", "c0", "c4", "c3"},
                        {"max(c8)",
                         "max(c2)",
                         "sum(c1)",
                         "sum(c0)",
                         "min(c4)",
                         "max(c3)"},
                        {})
                    .capturePlanNodeId(aggrNodeId)
                    .planNode();
    testPlan(
        plan,
        "SELECT c8, c2, c1, c0, c4, c3, max(c8), max(c2), sum(c1), sum(c0), min(c4), max(c3) FROM tmp GROUP BY 1, 2, 3, 4, 5, 6");
  }
}

TEST_F(AggregationTest, preGroupedAggregationWithSpilling) {
  std::vector<RowVectorPtr> vectors;
  int64_t val = 0;
  for (int32_t i = 0; i < 4; ++i) {
    vectors.push_back(makeRowVector(
        {// Pre-grouped key.
         makeFlatVector<int64_t>(10, [&](auto /*row*/) { return val++ / 5; }),
         // Payload.
         makeFlatVector<int64_t>(10, [](auto row) { return row; }),
         makeFlatVector<int64_t>(10, [](auto row) { return row; })}));
  }
  createDuckDbTable(vectors);
  auto spillDirectory = exec::test::TempDirectoryPath::create();
  core::PlanNodeId aggrNodeId;
  TestScopedSpillInjection scopedSpillInjection(100);
  auto task =
      AssertQueryBuilder(duckDbQueryRunner_)
          .spillDirectory(spillDirectory->getPath())
          .config(QueryConfig::kSpillEnabled, true)
          .config(QueryConfig::kAggregationSpillEnabled, true)
          .plan(PlanBuilder()
                    .values(vectors)
                    .aggregation(
                        {"c0", "c1"},
                        {"c0"},
                        {"sum(c2)"},
                        {},
                        core::AggregationNode::Step::kSingle,
                        false)
                    .capturePlanNodeId(aggrNodeId)
                    .planNode())
          .assertResults("SELECT c0, c1, sum(c2) FROM tmp GROUP BY c0, c1");
  auto stats = task->taskStats().pipelineStats;
  // Verify that spilling is not triggered.
  ASSERT_EQ(toPlanStats(task->taskStats()).at(aggrNodeId).spilledInputBytes, 0);
  ASSERT_EQ(toPlanStats(task->taskStats()).at(aggrNodeId).spilledBytes, 0);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(AggregationTest, adaptiveOutputBatchRows) {
  int32_t defaultOutputBatchRows = 10;
  vector_size_t size = defaultOutputBatchRows * 5;
  auto vectors = std::vector<RowVectorPtr>(
      8,
      makeRowVector(
          {"k0", "c0"},
          {makeFlatVector<int32_t>(size, [&](auto row) { return row; }),
           makeFlatVector<int8_t>(size, [&](auto row) { return row % 2; })}));

  createDuckDbTable(vectors);

  auto plan = PlanBuilder()
                  .values(vectors)
                  .singleAggregation({"k0"}, {"sum(c0)"})
                  .planNode();

  // Test setting larger output batch bytes will create batches of greater
  // number of rows.
  {
    auto outputBatchBytes = "1000";
    auto task =
        AssertQueryBuilder(plan, duckDbQueryRunner_)
            .config(QueryConfig::kPreferredOutputBatchBytes, outputBatchBytes)
            .assertResults("SELECT k0, SUM(c0) FROM tmp GROUP BY k0");

    auto aggOpStats = task->taskStats().pipelineStats[0].operatorStats[1];
    ASSERT_GT(
        aggOpStats.outputPositions / aggOpStats.outputVectors,
        defaultOutputBatchRows);
  }

  // Test setting smaller output batch bytes will create batches of fewer
  // number of rows.
  {
    auto outputBatchBytes = "1";
    auto task =
        AssertQueryBuilder(plan, duckDbQueryRunner_)
            .config(QueryConfig::kPreferredOutputBatchBytes, outputBatchBytes)
            .assertResults("SELECT k0, SUM(c0) FROM tmp GROUP BY k0");

    auto aggOpStats = task->taskStats().pipelineStats[0].operatorStats[1];
    ASSERT_LT(
        aggOpStats.outputPositions / aggOpStats.outputVectors,
        defaultOutputBatchRows);
  }
}

DEBUG_ONLY_TEST_F(AggregationTest, reclaimDuringInputProcessing) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), VARCHAR()});
  const int numBatches = 10;
  auto batches = makeVectors(rowType, 1000, numBatches);

  struct {
    // 0: trigger reclaim with some input processed.
    // 1: trigger reclaim after all the inputs processed.
    int triggerCondition;
    bool spillEnabled;
    bool expectedReclaimable;

    std::string debugString() const {
      return fmt::format(
          "triggerCondition {}, spillEnabled {}, expectedReclaimable {}",
          triggerCondition,
          spillEnabled,
          expectedReclaimable);
    }
  } testSettings[] = {
      {0, true, true}, {0, false, false}, {1, true, true}, {1, false, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto tempDirectory = exec::test::TempDirectoryPath::create();
    auto queryCtx = core::QueryCtx::create(executor_.get());
    queryCtx->testingOverrideMemoryPool(memory::memoryManager()->addRootPool(
        queryCtx->queryId(), kMaxBytes, memory::MemoryReclaimer::create()));
    auto expectedResult =
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                .planNode())
            .queryCtx(queryCtx)
            .copyResults(pool_.get());

    folly::EventCount driverWait;
    std::atomic_bool driverWaitFlag{true};
    folly::EventCount testWait;
    std::atomic_bool testWaitFlag{true};

    std::atomic_int numInputs{0};
    Operator* op{nullptr};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* testOp) {
          if (testOp->operatorType() != "Aggregation") {
            ASSERT_FALSE(testOp->canReclaim());
            return;
          }
          op = testOp;
          ++numInputs;
          if (testData.triggerCondition == 0) {
            if (numInputs != 2) {
              return;
            }
          }
          if (testData.triggerCondition == 1) {
            if (numInputs != numBatches) {
              return;
            }
          }
          ASSERT_EQ(op->canReclaim(), testData.expectedReclaimable);
          uint64_t reclaimableBytes{0};
          const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
          ASSERT_EQ(reclaimable, testData.expectedReclaimable);
          if (testData.expectedReclaimable) {
            ASSERT_GT(reclaimableBytes, 0);
          } else {
            ASSERT_EQ(reclaimableBytes, 0);
          }
          testWaitFlag = false;
          testWait.notifyAll();
          driverWait.await([&] { return !driverWaitFlag.load(); });
        })));

    std::thread taskThread([&]() {
      if (testData.spillEnabled) {
        auto task = AssertQueryBuilder(
                        PlanBuilder()
                            .values(batches)
                            .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                            .planNode())
                        .queryCtx(queryCtx)
                        .spillDirectory(tempDirectory->getPath())
                        .config(QueryConfig::kSpillEnabled, true)
                        .config(QueryConfig::kAggregationSpillEnabled, true)
                        .maxDrivers(1)
                        .assertResults(expectedResult);
      } else {
        auto task = AssertQueryBuilder(
                        PlanBuilder()
                            .values(batches)
                            .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                            .planNode())
                        .queryCtx(queryCtx)
                        .maxDrivers(1)
                        .assertResults(expectedResult);
      }
    });

    testWait.await([&]() { return !testWaitFlag.load(); });
    ASSERT_TRUE(op != nullptr);
    auto task = op->operatorCtx()->task();
    auto taskPauseWait = task->requestPause();

    driverWaitFlag = false;
    driverWait.notifyAll();
    taskPauseWait.wait();

    uint64_t reclaimableBytes{0};
    const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
    ASSERT_EQ(op->canReclaim(), testData.expectedReclaimable);
    ASSERT_EQ(reclaimable, testData.expectedReclaimable);
    if (testData.expectedReclaimable) {
      ASSERT_GT(reclaimableBytes, 0);
    } else {
      ASSERT_EQ(reclaimableBytes, 0);
    }

    if (testData.expectedReclaimable) {
      {
        memory::ScopedMemoryArbitrationContext ctx(op->pool());
        op->pool()->reclaim(
            folly::Random::oneIn(2) ? 0 : folly::Random::rand32(rng_),
            0,
            reclaimerStats_);
      }
      ASSERT_GT(reclaimerStats_.reclaimExecTimeUs, 0);
      ASSERT_GT(reclaimerStats_.reclaimedBytes, 0);
      reclaimerStats_.reset();
      // We expect all the memory has been freed from the hash table, except for
      // the ones used by raw_vector.
      ASSERT_EQ(op->pool()->usedBytes(), 28672);
    } else {
      {
        memory::ScopedMemoryArbitrationContext ctx(op->pool());
        VELOX_ASSERT_THROW(
            op->reclaim(
                folly::Random::oneIn(2) ? 0 : folly::Random::rand32(rng_),
                reclaimerStats_),
            "");
      }
    }

    Task::resume(task);

    taskThread.join();

    auto stats = task->taskStats().pipelineStats;
    if (testData.expectedReclaimable) {
      ASSERT_GT(stats[0].operatorStats[1].spilledBytes, 0);
      ASSERT_EQ(stats[0].operatorStats[1].spilledPartitions, 8);
    } else {
      ASSERT_EQ(stats[0].operatorStats[1].spilledBytes, 0);
      ASSERT_EQ(stats[0].operatorStats[1].spilledPartitions, 0);
    }
    OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
  }
  ASSERT_EQ(reclaimerStats_, memory::MemoryReclaimer::Stats{});
}

DEBUG_ONLY_TEST_F(AggregationTest, reclaimDuringReserve) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), VARCHAR()});
  const int32_t numBatches = 10;
  std::vector<RowVectorPtr> batches;
  for (int32_t i = 0; i < numBatches; ++i) {
    const size_t size = i == 0 ? 100 : 40000;
    VectorFuzzer fuzzer({.vectorSize = size}, pool());
    batches.push_back(fuzzer.fuzzRow(rowType));
  }

  auto tempDirectory = exec::test::TempDirectoryPath::create();
  auto queryCtx = core::QueryCtx::create(executor_.get());
  queryCtx->testingOverrideMemoryPool(memory::memoryManager()->addRootPool(
      queryCtx->queryId(), kMaxBytes, memory::MemoryReclaimer::create()));
  auto expectedResult =
      AssertQueryBuilder(PlanBuilder()
                             .values(batches)
                             .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                             .planNode())
          .queryCtx(queryCtx)
          .copyResults(pool_.get());

  folly::EventCount driverWait;
  auto driverWaitKey = driverWait.prepareWait();
  folly::EventCount testWait;
  auto testWaitKey = testWait.prepareWait();

  Operator* op;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::addInput",
      std::function<void(Operator*)>(([&](Operator* testOp) {
        if (testOp->operatorType() != "Aggregation") {
          ASSERT_FALSE(testOp->canReclaim());
          return;
        }
        op = testOp;
      })));

  std::atomic_bool injectOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::common::memory::MemoryPoolImpl::maybeReserve",
      std::function<void(memory::MemoryPoolImpl*)>(
          ([&](memory::MemoryPoolImpl* pool) {
            ASSERT_TRUE(op != nullptr);
            const std::string re(".*Aggregation");
            if (!RE2::FullMatch(pool->name(), re)) {
              return;
            }
            if (!injectOnce.exchange(false)) {
              return;
            }
            ASSERT_TRUE(op->canReclaim());
            uint64_t reclaimableBytes{0};
            const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
            ASSERT_TRUE(reclaimable);
            ASSERT_GT(reclaimableBytes, 0);
            auto* driver = op->operatorCtx()->driver();
            TestSuspendedSection suspendedSection(driver);
            testWait.notify();
            driverWait.wait(driverWaitKey);
          })));

  std::thread taskThread([&]() {
    AssertQueryBuilder(PlanBuilder()
                           .values(batches)
                           .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                           .planNode())
        .queryCtx(queryCtx)
        .spillDirectory(tempDirectory->getPath())
        .config(QueryConfig::kSpillEnabled, true)
        .config(QueryConfig::kAggregationSpillEnabled, true)
        .maxDrivers(1)
        .assertResults(expectedResult);
  });

  testWait.wait(testWaitKey);
  ASSERT_TRUE(op != nullptr);
  auto task = op->operatorCtx()->task();
  auto taskPauseWait = task->requestPause();
  taskPauseWait.wait();

  uint64_t reclaimableBytes{0};
  const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
  ASSERT_TRUE(op->canReclaim());
  ASSERT_TRUE(reclaimable);
  ASSERT_GT(reclaimableBytes, 0);

  const auto usedMemory = op->pool()->usedBytes();
  {
    memory::ScopedMemoryArbitrationContext ctx(op->pool());
    op->pool()->reclaim(
        folly::Random::oneIn(2) ? 0 : folly::Random::rand32(rng_),
        0,
        reclaimerStats_);
  }
  ASSERT_GT(reclaimerStats_.reclaimExecTimeUs, 0);
  ASSERT_GE(reclaimerStats_.reclaimedBytes, 0);
  reclaimerStats_.reset();
  // The hash table itself in the grouping set is not cleared so it still
  // uses some memory.
  ASSERT_LT(op->pool()->usedBytes(), usedMemory);

  driverWait.notify();
  Task::resume(task);
  taskThread.join();

  auto stats = task->taskStats().pipelineStats;
  ASSERT_GT(stats[0].operatorStats[1].spilledBytes, 0);
  ASSERT_EQ(stats[0].operatorStats[1].spilledPartitions, 8);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
  ASSERT_EQ(reclaimerStats_, memory::MemoryReclaimer::Stats{});
}

DEBUG_ONLY_TEST_F(AggregationTest, reclaimDuringAllocation) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), VARCHAR()});
  auto batches = makeVectors(rowType, 1000, 10);

  const std::vector<bool> enableSpillings = {false, true};
  for (const auto enableSpilling : enableSpillings) {
    SCOPED_TRACE(fmt::format("enableSpilling {}", enableSpilling));

    auto tempDirectory = exec::test::TempDirectoryPath::create();
    auto queryCtx = core::QueryCtx::create(executor_.get());
    queryCtx->testingOverrideMemoryPool(
        memory::memoryManager()->addRootPool(queryCtx->queryId(), kMaxBytes));
    auto expectedResult =
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                .planNode())
            .queryCtx(queryCtx)
            .copyResults(pool_.get());

    folly::EventCount driverWait;
    auto driverWaitKey = driverWait.prepareWait();
    folly::EventCount testWait;
    auto testWaitKey = testWait.prepareWait();

    Operator* op;
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* testOp) {
          if (testOp->operatorType() != "Aggregation") {
            ASSERT_FALSE(testOp->canReclaim());
            return;
          }
          op = testOp;
        })));

    std::atomic_bool injectOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::common::memory::MemoryPoolImpl::allocateNonContiguous",
        std::function<void(memory::MemoryPoolImpl*)>(
            ([&](memory::MemoryPoolImpl* pool) {
              ASSERT_TRUE(op != nullptr);
              const std::string re(".*Aggregation");
              if (!RE2::FullMatch(pool->name(), re)) {
                return;
              }
              if (!injectOnce.exchange(false)) {
                return;
              }
              ASSERT_EQ(op->canReclaim(), enableSpilling);
              uint64_t reclaimableBytes{0};
              const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
              ASSERT_EQ(reclaimable, enableSpilling);
              if (enableSpilling) {
                ASSERT_GT(reclaimableBytes, 0);
              } else {
                ASSERT_EQ(reclaimableBytes, 0);
              }
              auto* driver = op->operatorCtx()->driver();
              TestSuspendedSection suspendedSection(driver);
              testWait.notify();
              driverWait.wait(driverWaitKey);
            })));

    std::thread taskThread([&]() {
      if (enableSpilling) {
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                .planNode())
            .queryCtx(queryCtx)
            .spillDirectory(tempDirectory->getPath())
            .config(QueryConfig::kSpillEnabled, true)
            .config(QueryConfig::kAggregationSpillEnabled, true)
            .maxDrivers(1)
            .assertResults(expectedResult);
      } else {
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                .planNode())
            .queryCtx(queryCtx)
            .maxDrivers(1)
            .assertResults(expectedResult);
      }
    });

    testWait.wait(testWaitKey);
    ASSERT_TRUE(op != nullptr);
    auto task = op->operatorCtx()->task();
    auto taskPauseWait = task->requestPause();
    taskPauseWait.wait();

    uint64_t reclaimableBytes{0};
    const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
    ASSERT_EQ(op->canReclaim(), enableSpilling);
    ASSERT_EQ(reclaimable, enableSpilling);

    if (enableSpilling) {
      ASSERT_GT(reclaimableBytes, 0);
    } else {
      ASSERT_EQ(reclaimableBytes, 0);
    }
    VELOX_ASSERT_THROW(
        op->reclaim(
            folly::Random::oneIn(2) ? 0 : folly::Random::rand32(rng_),
            reclaimerStats_),
        "");

    driverWait.notify();
    Task::resume(task);

    taskThread.join();

    auto stats = task->taskStats().pipelineStats;
    ASSERT_EQ(stats[0].operatorStats[1].spilledBytes, 0);
    ASSERT_EQ(stats[0].operatorStats[1].spilledPartitions, 0);
    OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
  }
  ASSERT_EQ(reclaimerStats_, memory::MemoryReclaimer::Stats{0});
}

DEBUG_ONLY_TEST_F(AggregationTest, reclaimDuringOutputProcessing) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), INTEGER()});
  auto batches = makeVectors(rowType, 1000, 10);

  const std::vector<bool> enableSpillings = {false, true};
  for (const auto enableSpilling : enableSpillings) {
    SCOPED_TRACE(fmt::format("enableSpilling {}", enableSpilling));

    auto tempDirectory = exec::test::TempDirectoryPath::create();
    auto queryCtx = core::QueryCtx::create(executor_.get());
    queryCtx->testingOverrideMemoryPool(memory::memoryManager()->addRootPool(
        queryCtx->queryId(), kMaxBytes, memory::MemoryReclaimer::create()));
    auto expectedResult =
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                .planNode())
            .queryCtx(queryCtx)
            .copyResults(pool_.get());

    std::atomic_bool driverWaitFlag{true};
    folly::EventCount driverWait;
    std::atomic_bool testWaitFlag{true};
    folly::EventCount testWait;

    std::atomic_bool injectNoMoreInputOnce{true};
    Operator* op{nullptr};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::noMoreInput",
        std::function<void(Operator*)>(([&](Operator* testOp) {
          if (testOp->operatorType() != "Aggregation") {
            ASSERT_FALSE(testOp->canReclaim());
            return;
          }
          if (!injectNoMoreInputOnce.exchange(false)) {
            return;
          }
          op = testOp;
          ASSERT_EQ(op->canReclaim(), enableSpilling);
          uint64_t reclaimableBytes{0};
          const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
          ASSERT_EQ(reclaimable, enableSpilling);
          if (enableSpilling) {
            ASSERT_GT(reclaimableBytes, 0);
          } else {
            ASSERT_EQ(reclaimableBytes, 0);
          }
          testWaitFlag = false;
          testWait.notifyAll();
          driverWait.await([&]() { return !driverWaitFlag.load(); });
        })));

    std::thread taskThread([&]() {
      if (enableSpilling) {
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                .planNode())
            .queryCtx(queryCtx)
            .spillDirectory(tempDirectory->getPath())
            .config(QueryConfig::kSpillEnabled, true)
            .config(QueryConfig::kAggregationSpillEnabled, true)
            .maxDrivers(1)
            .assertResults(expectedResult);
      } else {
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                .planNode())
            .queryCtx(queryCtx)
            .maxDrivers(1)
            .assertResults(expectedResult);
      }
    });

    testWait.await([&]() { return !testWaitFlag.load(); });
    ASSERT_TRUE(op != nullptr);

    auto task = op->operatorCtx()->task();
    auto taskPauseWait = task->requestPause();
    driverWaitFlag = false;
    driverWait.notifyAll();
    taskPauseWait.wait();

    uint64_t reclaimableBytes{0};
    const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
    ASSERT_EQ(op->canReclaim(), enableSpilling);
    ASSERT_EQ(reclaimable, enableSpilling);
    if (enableSpilling) {
      ASSERT_GT(reclaimableBytes, 0);
      const auto usedMemory = op->pool()->usedBytes();
      memory::ScopedMemoryArbitrationContext ctx(op->pool());
      op->pool()->reclaim(
          folly::Random::oneIn(2) ? 0 : folly::Random::rand32(rng_),
          0,
          reclaimerStats_);
      ASSERT_EQ(reclaimerStats_.numNonReclaimableAttempts, 0);
      ASSERT_GT(usedMemory, op->pool()->usedBytes());
      ASSERT_GT(reclaimerStats_.reclaimedBytes, 0);
      ASSERT_GT(reclaimerStats_.reclaimExecTimeUs, 0);
      reclaimerStats_.reset();
    } else {
      ASSERT_EQ(reclaimableBytes, 0);
      memory::ScopedMemoryArbitrationContext ctx(op->pool());
      VELOX_ASSERT_THROW(
          op->reclaim(
              folly::Random::oneIn(2) ? 0 : folly::Random::rand32(rng_),
              reclaimerStats_),
          "");
    }

    Task::resume(task);

    taskThread.join();

    auto stats = task->taskStats().pipelineStats;
    if (enableSpilling) {
      ASSERT_GT(stats[0].operatorStats[1].spilledBytes, 0);
      ASSERT_EQ(stats[0].operatorStats[1].spilledPartitions, 1);
    } else {
      ASSERT_EQ(stats[0].operatorStats[1].spilledBytes, 0);
      ASSERT_EQ(stats[0].operatorStats[1].spilledPartitions, 0);
    }

    OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
  }
  ASSERT_EQ(reclaimerStats_, memory::MemoryReclaimer::Stats{0});
}

DEBUG_ONLY_TEST_F(AggregationTest, reclaimDuringNonReclaimableSection) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), INTEGER()});
  auto batches = makeVectors(rowType, 1000, 10);

  struct {
    bool enableSpilling;
    bool nonReclaimableInput;

    std::string debugString() const {
      return fmt::format(
          "enableSpilling {}, nonReclaimableInput {}",
          enableSpilling,
          nonReclaimableInput);
    }
  } testSettings[] = {
      {true, false}, {true, true}, {false, false}, {false, true}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(fmt::format("testData {}", testData.debugString()));

    auto tempDirectory = exec::test::TempDirectoryPath::create();
    auto queryCtx = core::QueryCtx::create(executor_.get());
    queryCtx->testingOverrideMemoryPool(
        memory::memoryManager()->addRootPool(queryCtx->queryId(), kMaxBytes));
    auto expectedResult =
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                .planNode())
            .queryCtx(queryCtx)
            .copyResults(pool_.get());

    std::atomic<Driver*> driver{nullptr};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal",
        std::function<void(Driver*)>(
            [&](Driver* testDriver) { driver = testDriver; }));

    std::atomic_bool driverWaitFlag{true};
    folly::EventCount driverWait;
    std::atomic_bool testWaitFlag{true};
    folly::EventCount testWait;

    std::atomic_bool injectNonReclaimableSectionOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::GroupingSet::addInputForActiveRows",
        std::function<void(GroupingSet*)>(([&](GroupingSet* groupSet) {
          if (!testData.nonReclaimableInput) {
            return;
          }
          if (groupSet->testingPool().usedBytes() == 0) {
            return;
          }
          if (!injectNonReclaimableSectionOnce.exchange(false)) {
            return;
          }
          ASSERT_TRUE(driver != nullptr);
          ASSERT_EQ(
              driver.load()->task()->enterSuspended(driver.load()->state()),
              StopReason::kNone);

          testWaitFlag = false;
          testWait.notifyAll();

          driverWait.await([&]() { return !driverWaitFlag.load(); });
          ASSERT_EQ(
              driver.load()->task()->leaveSuspended(driver.load()->state()),
              StopReason::kNone);
        })));
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::GroupingSet::getOutput",
        std::function<void(GroupingSet*)>(([&](GroupingSet* groupSet) {
          if (testData.nonReclaimableInput) {
            return;
          }
          if (!injectNonReclaimableSectionOnce.exchange(false)) {
            return;
          }
          ASSERT_TRUE(driver != nullptr);
          ASSERT_EQ(
              driver.load()->task()->enterSuspended(driver.load()->state()),
              StopReason::kNone);

          testWaitFlag = false;
          testWait.notifyAll();

          driverWait.await([&]() { return !driverWaitFlag.load(); });

          ASSERT_EQ(
              driver.load()->task()->leaveSuspended(driver.load()->state()),
              StopReason::kNone);
        })));

    core::PlanNodeId aggregationPlanNodeId;
    std::thread taskThread([&]() {
      if (testData.enableSpilling) {
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                .capturePlanNodeId(aggregationPlanNodeId)
                .planNode())
            .queryCtx(queryCtx)
            .spillDirectory(tempDirectory->getPath())
            .config(QueryConfig::kSpillEnabled, true)
            .config(QueryConfig::kAggregationSpillEnabled, true)
            .maxDrivers(1)
            .assertResults(expectedResult);
      } else {
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                .capturePlanNodeId(aggregationPlanNodeId)
                .planNode())
            .queryCtx(queryCtx)
            .maxDrivers(1)
            .assertResults(expectedResult);
      }
    });

    testWait.await([&]() { return !testWaitFlag.load(); });
    ASSERT_TRUE(driver.load() != nullptr);

    auto task = driver.load()->task();
    auto taskPauseWait = task->requestPause();
    taskPauseWait.wait();

    auto* op = driver.load()->findOperator(aggregationPlanNodeId);
    ASSERT_TRUE(op != nullptr);

    uint64_t reclaimableBytes{0};
    const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
    ASSERT_EQ(op->canReclaim(), testData.enableSpilling);
    ASSERT_EQ(reclaimable, testData.enableSpilling);
    if (testData.enableSpilling) {
      ASSERT_GT(reclaimableBytes, 0);
    } else {
      ASSERT_EQ(reclaimableBytes, 0);
    }
    VELOX_ASSERT_THROW(
        op->reclaim(
            folly::Random::oneIn(2) ? 0 : folly::Random::rand32(rng_),
            reclaimerStats_),
        "");
    ASSERT_EQ(reclaimerStats_.numNonReclaimableAttempts, 0);

    driverWaitFlag = false;
    driverWait.notifyAll();

    Task::resume(task);

    taskThread.join();

    auto stats = task->taskStats().pipelineStats;
    ASSERT_EQ(stats[0].operatorStats[1].spilledBytes, 0);
    ASSERT_EQ(stats[0].operatorStats[1].spilledPartitions, 0);

    OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
    reclaimerStats_.reset();
  }
}

DEBUG_ONLY_TEST_F(AggregationTest, reclaimWithEmptyAggregationTable) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), INTEGER()});
  auto batches = makeVectors(rowType, 1000, 10);

  const std::vector<bool> enableSpillings = {false, true};
  for (const auto enableSpilling : enableSpillings) {
    SCOPED_TRACE(fmt::format("enableSpilling {}", enableSpilling));

    auto tempDirectory = exec::test::TempDirectoryPath::create();
    auto queryCtx = core::QueryCtx::create(executor_.get());
    queryCtx->testingOverrideMemoryPool(
        memory::memoryManager()->addRootPool(queryCtx->queryId(), kMaxBytes));
    auto expectedResult =
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                .planNode())
            .queryCtx(queryCtx)
            .copyResults(pool_.get());

    folly::EventCount driverWait;
    auto driverWaitKey = driverWait.prepareWait();
    folly::EventCount testWait;
    auto testWaitKey = testWait.prepareWait();

    core::PlanNodeId aggregationPlanNodeId;
    auto aggregationPlan =
        PlanBuilder()
            .values(batches)
            .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
            .capturePlanNodeId(aggregationPlanNodeId)
            .planNode();

    std::atomic_bool injectOnce{true};
    Operator* op;
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal",
        std::function<void(Driver*)>(([&](Driver* driver) {
          if (driver->findOperator(aggregationPlanNodeId) == nullptr) {
            return;
          }
          if (!injectOnce.exchange(false)) {
            return;
          }
          op = driver->findOperator(aggregationPlanNodeId);
          testWait.notify();
          driverWait.wait(driverWaitKey);
        })));

    std::thread taskThread([&]() {
      if (enableSpilling) {
        AssertQueryBuilder(nullptr)
            .plan(aggregationPlan)
            .queryCtx(queryCtx)
            .spillDirectory(tempDirectory->getPath())
            .config(QueryConfig::kSpillEnabled, true)
            .config(QueryConfig::kAggregationSpillEnabled, true)
            .maxDrivers(1)
            .assertResults(expectedResult);
      } else {
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                .planNode())
            .queryCtx(queryCtx)
            .maxDrivers(1)
            .assertResults(expectedResult);
      }
    });

    testWait.wait(testWaitKey);
    ASSERT_TRUE(op != nullptr);
    auto task = op->operatorCtx()->task();
    auto taskPauseWait = task->requestPause();
    driverWait.notify();
    taskPauseWait.wait();

    uint64_t reclaimableBytes{0};
    const bool reclaimable = op->reclaimableBytes(reclaimableBytes);
    ASSERT_EQ(op->canReclaim(), enableSpilling);
    ASSERT_EQ(reclaimable, enableSpilling);
    if (enableSpilling) {
      ASSERT_EQ(reclaimableBytes, 0);
      const auto usedMemory = op->pool()->usedBytes();
      op->pool()->reclaim(
          folly::Random::oneIn(2) ? 0 : folly::Random::rand32(rng_),
          0,
          reclaimerStats_);
      ASSERT_EQ(reclaimerStats_, memory::MemoryReclaimer::Stats{});
      // No reclaim as the operator has started output processing.
      ASSERT_EQ(usedMemory, op->pool()->usedBytes());
    } else {
      ASSERT_EQ(reclaimableBytes, 0);
      VELOX_ASSERT_THROW(
          op->reclaim(
              folly::Random::oneIn(2) ? 0 : folly::Random::rand32(rng_),
              reclaimerStats_),
          "");
    }

    Task::resume(task);

    taskThread.join();

    auto stats = task->taskStats().pipelineStats;
    ASSERT_EQ(stats[0].operatorStats[1].spilledBytes, 0);
    ASSERT_EQ(stats[0].operatorStats[1].spilledPartitions, 0);
    OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
  }
  ASSERT_EQ(reclaimerStats_, memory::MemoryReclaimer::Stats{});
}

namespace {
void abortPool(memory::MemoryPool* pool) {
  try {
    VELOX_FAIL("Memory pool manually aborted");
  } catch (const VeloxException&) {
    pool->abort(std::current_exception());
  }
}
} // namespace

DEBUG_ONLY_TEST_F(AggregationTest, abortDuringOutputProcessing) {
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), INTEGER()});
  auto batches = makeVectors(rowType, 1000, 10);

  struct {
    bool abortFromRootMemoryPool;
    int numDrivers;

    std::string debugString() const {
      return fmt::format(
          "abortFromRootMemoryPool {} numDrivers {}",
          abortFromRootMemoryPool,
          numDrivers);
    }
  } testSettings[] = {{true, 1}, {false, 1}, {true, 4}, {false, 4}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto expectedResult =
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                .planNode())
            .copyResults(pool_.get());

    std::atomic_bool injectOnce{true};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::noMoreInput",
        std::function<void(Operator*)>(([&](Operator* op) {
          if (op->operatorType() != "Aggregation") {
            return;
          }
          if (!injectOnce.exchange(false)) {
            return;
          }
          auto* driver = op->operatorCtx()->driver();
          ASSERT_EQ(
              driver->task()->enterSuspended(driver->state()),
              StopReason::kNone);
          testData.abortFromRootMemoryPool ? abortPool(op->pool()->root())
                                           : abortPool(op->pool());
          // We can't directly reclaim memory from this hash build operator as
          // its driver thread is running and in suspension state.
          ASSERT_GT(op->pool()->root()->usedBytes(), 0);
          ASSERT_EQ(
              driver->task()->leaveSuspended(driver->state()),
              StopReason::kAlreadyTerminated);
          VELOX_MEM_POOL_ABORTED("Memory pool aborted");
        })));

    VELOX_ASSERT_THROW(
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                .planNode())
            .maxDrivers(testData.numDrivers)
            .assertResults(expectedResult),
        "Memory pool manually aborted");
    waitForAllTasksToBeDeleted();
  }
}

DEBUG_ONLY_TEST_F(AggregationTest, abortDuringInputgProcessing) {
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), INTEGER()});
  auto batches = makeVectors(rowType, 1000, 10);

  struct {
    bool abortFromRootMemoryPool;
    int numDrivers;

    std::string debugString() const {
      return fmt::format(
          "abortFromRootMemoryPool {} numDrivers {}",
          abortFromRootMemoryPool,
          numDrivers);
    }
  } testSettings[] = {{true, 1}, {false, 1}, {true, 4}, {false, 4}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto expectedResult =
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                .planNode())
            .copyResults(pool_.get());

    std::atomic_int numInputs{0};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(Operator*)>(([&](Operator* op) {
          if (op->operatorType() != "Aggregation") {
            return;
          }
          if (++numInputs != 2) {
            return;
          }
          auto* driver = op->operatorCtx()->driver();
          ASSERT_EQ(
              driver->task()->enterSuspended(driver->state()),
              StopReason::kNone);
          testData.abortFromRootMemoryPool ? abortPool(op->pool()->root())
                                           : abortPool(op->pool());
          // We can't directly reclaim memory from this hash build operator as
          // its driver thread is running and in suspension state.
          ASSERT_GT(op->pool()->root()->usedBytes(), 0);
          ASSERT_EQ(
              driver->task()->leaveSuspended(driver->state()),
              StopReason::kAlreadyTerminated);
          ASSERT_TRUE(op->pool()->aborted());
          ASSERT_TRUE(op->pool()->root()->aborted());
          VELOX_MEM_POOL_ABORTED("Memory pool aborted");
        })));

    VELOX_ASSERT_THROW(
        AssertQueryBuilder(
            PlanBuilder()
                .values(batches)
                .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                .planNode())
            .maxDrivers(testData.numDrivers)
            .assertResults(expectedResult),
        "Memory pool manually aborted");
    waitForAllTasksToBeDeleted();
  }
}

TEST_F(AggregationTest, noAggregationsNoGroupingKeys) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3}),
  });

  auto plan = PlanBuilder()
                  .values({data})
                  .partialAggregation({}, {})
                  .finalAggregation()
                  .planNode();

  auto result = AssertQueryBuilder(plan).copyResults(pool());

  // 1 row.
  ASSERT_EQ(result->size(), 1);
  // Zero columns.
  ASSERT_EQ(result->type()->size(), 0);
}

// Reproduces hang in partial distinct aggregation described in
// https://github.com/facebookincubator/velox/issues/7967 .
TEST_F(AggregationTest, distinctHang) {
  static const int64_t kMin = std::numeric_limits<int32_t>::min();
  static const int64_t kMax = std::numeric_limits<int32_t>::max();
  auto data = makeRowVector({
      makeFlatVector<int64_t>(
          5'000,
          [](auto row) {
            if (row % 2 == 0) {
              return kMin + row;
            } else {
              return kMax - row;
            }
          }),
      makeFlatVector<int64_t>(
          5'000,
          [](auto row) {
            if (row % 2 == 0) {
              return kMin - row;
            } else {
              return kMax + row;
            }
          }),
  });

  auto newData = makeRowVector({
      makeFlatVector<int64_t>(
          5'000, [](auto row) { return kMin + row + 5'000; }),
      makeFlatVector<int64_t>(5'000, [](auto row) { return kMin - row; }),
  });

  createDuckDbTable({data, newData});

  core::PlanNodeId aggNodeId;
  auto plan = PlanBuilder()
                  .values({data, newData, data})
                  .partialAggregation({"c0", "c1"}, {})
                  .capturePlanNodeId(aggNodeId)
                  .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .config(QueryConfig::kMaxPartialAggregationMemory, 400000)
      .assertResults("SELECT distinct c0, c1 FROM tmp");
}

// Trigger memory pool allocation at HashAggregation::populateAggregateInputs by
// aggregating null constant. Ensure the allocation happens outside of
// HashAggregation's constructor.
TEST_F(AggregationTest, memoryPoolAllocationAtInit) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2}),
  });
  createDuckDbTable({data});
  auto plan = PlanBuilder()
                  .values({data})
                  .aggregation(
                      {"c0"},
                      {"sum(cast(NULL as INT))"},
                      {},
                      core::AggregationNode::Step::kPartial,
                      false)
                  .planNode();

  assertQuery(plan, "SELECT c0, cast(NULL as INT) FROM tmp GROUP BY c0");
}

DEBUG_ONLY_TEST_F(AggregationTest, reclaimEmptyInput) {
  constexpr int64_t kMaxBytes = 1LL << 30; // 1GB
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), VARCHAR()});
  const int32_t numBatches = 5;
  auto batches = makeVectors(rowType, numBatches, 100);

  std::atomic_bool injectReclaimOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Values::getOutput",
      std::function<void(const exec::Values*)>([&](const exec::Values* values) {
        if (!injectReclaimOnce.exchange(false)) {
          return;
        }
        auto* driver = values->operatorCtx()->driver();
        auto task = values->operatorCtx()->task();
        // Shrink all the capacity before reclaim.
        memory::memoryManager()->arbitrator()->shrinkCapacity(
            task->pool()->root(), 0);
        {
          MemoryReclaimer::Stats stats;
          TestSuspendedSection suspendedSection(driver);
          task->pool()->reclaim(kMaxBytes, 0, stats);
          ASSERT_EQ(stats.numNonReclaimableAttempts, 0);
          ASSERT_GE(stats.reclaimExecTimeUs, 0);
          ASSERT_EQ(stats.reclaimedBytes, 0);
          ASSERT_GT(stats.reclaimWaitTimeUs, 0);
        }
      }));

  auto tempDirectory = exec::test::TempDirectoryPath::create();
  auto queryCtx = core::QueryCtx::create(executor_.get());
  queryCtx->testingOverrideMemoryPool(memory::memoryManager()->addRootPool(
      queryCtx->queryId(), kMaxBytes, memory::MemoryReclaimer::create()));
  core::PlanNodeId aggNodeId;
  auto task =
      AssertQueryBuilder(
          PlanBuilder()
              .values(batches)
              // Set fake filter to ensure empty input to aggregation operator.
              .filter("c0 != c0")
              .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
              .capturePlanNodeId(aggNodeId)
              .planNode(),
          duckDbQueryRunner_)
          .spillDirectory(tempDirectory->getPath())
          .queryCtx(queryCtx)
          .config(QueryConfig::kSpillEnabled, true)
          .config(QueryConfig::kAggregationSpillEnabled, true)
          .assertEmptyResults();
  auto taskStats = exec::toPlanStats(task->taskStats());
  ASSERT_EQ(taskStats.at(aggNodeId).spilledBytes, 0);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

DEBUG_ONLY_TEST_F(AggregationTest, reclaimEmptyOutput) {
  constexpr int64_t kMaxBytes = 4LL << 30; // 4GB
  auto rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), VARCHAR()});
  auto batches = makeVectors(rowType, 100, 5);

  auto expectedResult =
      AssertQueryBuilder(PlanBuilder()
                             .values(batches)
                             .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                             .planNode())
          .copyResults(pool_.get());

  std::atomic_int numGetOutput{0};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::getOutput",
      std::function<void(Operator*)>(([&](Operator* op) {
        if (op->operatorType() != "Aggregation") {
          return;
        }
        // Inject reclaim after the aggregation operator has received all the
        // inputs.
        if (!op->testingNoMoreInput()) {
          return;
        }
        // Inject reclaim after the aggregation operator has produced all the
        // output and before it has finished.
        if (++numGetOutput != 2) {
          return;
        }
        auto* driver = op->operatorCtx()->driver();
        auto task = op->operatorCtx()->task();
        // Shrink all the capacity before reclaim.
        memory::memoryManager()->arbitrator()->shrinkCapacity(
            task->pool()->root(), 0);
        {
          MemoryReclaimer::Stats stats;
          TestSuspendedSection suspendedSection(driver);
          memory::ScopedMemoryArbitrationContext ctx(op->pool());
          task->pool()->reclaim(kMaxBytes, 0, stats);
          ASSERT_EQ(stats.numNonReclaimableAttempts, 0);
          ASSERT_GT(stats.reclaimExecTimeUs, 0);
          // We expect to reclaim the memory from the hash table.
          ASSERT_GT(stats.reclaimedBytes, 0);
          ASSERT_GT(stats.reclaimWaitTimeUs, 0);
        }
      })));

  auto tempDirectory = exec::test::TempDirectoryPath::create();
  auto queryCtx = core::QueryCtx::create(executor_.get());
  queryCtx->testingOverrideMemoryPool(memory::memoryManager()->addRootPool(
      queryCtx->queryId(), kMaxBytes, memory::MemoryReclaimer::create()));
  core::PlanNodeId aggNodeId;
  auto task =
      AssertQueryBuilder(PlanBuilder()
                             .values(batches)
                             .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                             .capturePlanNodeId(aggNodeId)
                             .planNode())
          .spillDirectory(tempDirectory->getPath())
          .queryCtx(queryCtx)
          .config(QueryConfig::kSpillEnabled, true)
          .config(QueryConfig::kAggregationSpillEnabled, true)
          // Set the output query configs to ensure fetch the result in one
          // output batch.
          .config(QueryConfig::kPreferredOutputBatchBytes, 1UL << 30)
          .config(QueryConfig::kMaxOutputBatchRows, 1024)
          .assertResults(expectedResult);
  // Since the spilling is triggered after the aggregation operator has produced
  // all the output, we don't expect any spilled data.
  auto taskStats = exec::toPlanStats(task->taskStats());
  ASSERT_EQ(taskStats.at(aggNodeId).spilledBytes, 0);
  OperatorTestBase::deleteTaskAndCheckSpillDirectory(task);
}

TEST_F(AggregationTest, maxSpillBytes) {
  const auto rowType =
      ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), VARCHAR()});
  const auto vectors = createVectors(rowType, 128, 1 << 20);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId aggregationNodeId;
  const auto plan = PlanBuilder(planNodeIdGenerator)
                        .values(vectors)
                        .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                        .capturePlanNodeId(aggregationNodeId)
                        .planNode();
  auto spillDirectory = exec::test::TempDirectoryPath::create();

  struct {
    int32_t maxSpilledBytes;
    bool expectedExceedLimit;
    std::string debugString() const {
      return fmt::format("maxSpilledBytes {}", maxSpilledBytes);
    }
  } testSettings[] = {{1 << 30, false}, {1, true}, {0, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto queryCtx = core::QueryCtx::create(executor_.get());
    try {
      TestScopedSpillInjection scopedSpillInjection(100);
      AssertQueryBuilder(plan)
          .spillDirectory(spillDirectory->getPath())
          .queryCtx(queryCtx)
          .config(core::QueryConfig::kSpillEnabled, true)
          .config(core::QueryConfig::kAggregationSpillEnabled, true)
          .config(QueryConfig::kMaxSpillBytes, testData.maxSpilledBytes)
          .copyResults(pool_.get());
      ASSERT_FALSE(testData.expectedExceedLimit);
    } catch (const VeloxRuntimeError& e) {
      ASSERT_TRUE(testData.expectedExceedLimit);
      ASSERT_NE(
          e.message().find("Query exceeded per-query local spill limit of 1B"),
          std::string::npos);
      ASSERT_EQ(
          e.errorCode(), facebook::velox::error_code::kSpillLimitExceeded);
    }
    waitForAllTasksToBeDeleted();
  }
}

DEBUG_ONLY_TEST_F(AggregationTest, reclaimFromAggregation) {
  const int numInputs = 8;
  std::vector<RowVectorPtr> vectors =
      createVectors(numInputs, rowType_, fuzzerOpts_);
  createDuckDbTable(vectors);
  for (const auto maxSpillRunRows : std::vector<uint32_t>({32, 1UL << 30})) {
    SCOPED_TRACE(fmt::format("maxSpillRunRows {}", maxSpillRunRows));

    std::atomic_int inputCount{0};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(exec::Operator*)>(([&](exec::Operator* op) {
          if (op->operatorCtx()->operatorType() != "Aggregation") {
            return;
          }
          // Inject spill in the middle of aggregation input processing.
          if (++inputCount != numInputs / 2) {
            return;
          }
          testingRunArbitration(op->pool());
        })));

    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    core::PlanNodeId aggrNodeId;
    auto task =
        AssertQueryBuilder(duckDbQueryRunner_)
            .spillDirectory(spillDirectory->getPath())
            .config(core::QueryConfig::kSpillEnabled, true)
            .config(core::QueryConfig::kAggregationSpillEnabled, true)
            .config(
                core::QueryConfig::kMaxSpillRunRows,
                std::to_string(maxSpillRunRows))
            .plan(PlanBuilder()
                      .values(vectors)
                      .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                      .capturePlanNodeId(aggrNodeId)
                      .planNode())
            .assertResults(
                "SELECT c0, c1, array_agg(c2) FROM tmp GROUP BY c0, c1");
    auto taskStats = exec::toPlanStats(task->taskStats());
    auto& planStats = taskStats.at(aggrNodeId);
    ASSERT_GT(planStats.spilledBytes, 0);
    // The actual ime resolution is millisecond so we might see zero nanos
    // reporting in unit test.
    ASSERT_GE(
        planStats
            .customStats[memory::SharedArbitrator::kMemoryArbitrationWallNanos]
            .sum,
        0);
    task.reset();
    waitForAllTasksToBeDeleted();
  }
}

DEBUG_ONLY_TEST_F(AggregationTest, reclaimFromDistinctAggregation) {
  const int numInputs = 32;
  std::vector<RowVectorPtr> vectors =
      createVectors(numInputs, rowType_, fuzzerOpts_);
  createDuckDbTable(vectors);
  for (const auto maxSpillRunRows : std::vector<uint32_t>({32, 1UL << 30})) {
    SCOPED_TRACE(fmt::format("maxSpillRunRows {}", maxSpillRunRows));

    std::atomic_int inputCount{0};
    SCOPED_TESTVALUE_SET(
        "facebook::velox::exec::Driver::runInternal::addInput",
        std::function<void(exec::Operator*)>(([&](exec::Operator* op) {
          if (op->operatorCtx()->operatorType() != "Aggregation") {
            return;
          }
          // Inject spill at the end of aggregation input processing.
          if (++inputCount != numInputs / 2) {
            return;
          }
          testingRunArbitration(op->pool());
        })));

    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    core::PlanNodeId aggrNodeId;
    auto task = AssertQueryBuilder(duckDbQueryRunner_)
                    .spillDirectory(spillDirectory->getPath())
                    .config(core::QueryConfig::kSpillEnabled, true)
                    .config(core::QueryConfig::kAggregationSpillEnabled, true)
                    .config(
                        core::QueryConfig::kMaxSpillRunRows,
                        std::to_string(maxSpillRunRows))
                    .plan(PlanBuilder()
                              .values(vectors)
                              .singleAggregation({"c0"}, {})
                              .capturePlanNodeId(aggrNodeId)
                              .planNode())
                    .assertResults("SELECT distinct c0 FROM tmp");
    auto taskStats = exec::toPlanStats(task->taskStats());
    auto& planStats = taskStats.at(aggrNodeId);
    ASSERT_GT(planStats.spilledBytes, 0);
    task.reset();
    waitForAllTasksToBeDeleted();
  }
}

DEBUG_ONLY_TEST_F(AggregationTest, reclaimFromAggregationOnNoMoreInput) {
  std::vector<RowVectorPtr> vectors = createVectors(8, rowType_, fuzzerOpts_);
  createDuckDbTable(vectors);
  const auto spillDirectory = exec::test::TempDirectoryPath::create();

  std::atomic<bool> injectNoMoreInputOnce{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::noMoreInput",
      std::function<void(Operator*)>(([&](Operator* op) {
        if (op->operatorType() != "Aggregation") {
          return;
        }
        if (!injectNoMoreInputOnce.exchange(false)) {
          return;
        }
        testingRunArbitration(op->pool());
      })));

  {
    auto task =
        AssertQueryBuilder(duckDbQueryRunner_)
            .spillDirectory(spillDirectory->getPath())
            .config(core::QueryConfig::kSpillEnabled, true)
            .config(core::QueryConfig::kAggregationSpillEnabled, true)
            .maxDrivers(1)
            .plan(PlanBuilder()
                      .values(vectors)
                      .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                      .planNode())
            .assertResults(
                "SELECT c0, c1, array_agg(c2) FROM tmp GROUP BY c0, c1");
    auto stats = task->taskStats().pipelineStats;
    ASSERT_GT(stats[0].operatorStats[1].spilledBytes, 0);
  }
  waitForAllTasksToBeDeleted();
}

DEBUG_ONLY_TEST_F(AggregationTest, reclaimFromAggregationDuringOutput) {
  const int numVectors = 32;
  std::vector<RowVectorPtr> vectors;
  VectorFuzzer fuzzer(fuzzerOpts_, pool());
  int numRows{0};
  for (int i = 0; i < numVectors; ++i) {
    vectors.push_back(fuzzer.fuzzRow(rowType_));
    numRows += vectors.back()->size();
  }

  createDuckDbTable(vectors);
  const auto spillDirectory = exec::test::TempDirectoryPath::create();
  std::atomic_int numInputs{0};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::Driver::runInternal::getOutput",
      std::function<void(Operator*)>(([&](Operator* op) {
        if (op->operatorType() != "Aggregation") {
          return;
        }
        if (++numInputs != 5) {
          return;
        }
        testingRunArbitration(op->pool());
      })));
  {
    auto task =
        AssertQueryBuilder(duckDbQueryRunner_)
            .spillDirectory(spillDirectory->getPath())
            .config(core::QueryConfig::kSpillEnabled, true)
            .config(core::QueryConfig::kAggregationSpillEnabled, true)
            .config(core::QueryConfig::kPreferredOutputBatchRows, numRows / 10)
            .maxDrivers(1)
            //.queryCtx(aggregationQueryCtx)
            .plan(PlanBuilder()
                      .values(vectors)
                      .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                      .planNode())
            .assertResults(
                "SELECT c0, c1, array_agg(c2) FROM tmp GROUP BY c0, c1");
    auto stats = task->taskStats().pipelineStats;
    ASSERT_GT(stats[0].operatorStats[1].spilledBytes, 0);
  }
  waitForAllTasksToBeDeleted();
}

TEST_F(AggregationTest, reclaimFromCompletedAggregation) {
  std::vector<RowVectorPtr> vectors = createVectors(8, rowType_, fuzzerOpts_);
  createDuckDbTable(vectors);
  const auto spillDirectory = exec::test::TempDirectoryPath::create();

  folly::EventCount arbitrationWait;
  std::atomic_bool arbitrationWaitFlag{true};
  std::thread aggregationThread([&]() {
    auto task =
        AssertQueryBuilder(duckDbQueryRunner_)
            .plan(PlanBuilder()
                      .values(vectors)
                      .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
                      .planNode())
            .assertResults(
                "SELECT c0, c1, array_agg(c2) FROM tmp GROUP BY c0, c1");
    waitForTaskCompletion(task.get());
    arbitrationWaitFlag = false;
    arbitrationWait.notifyAll();
  });
  arbitrationWait.await([&] { return !arbitrationWaitFlag.load(); });

  memory::testingRunArbitration();
  aggregationThread.join();
  waitForAllTasksToBeDeleted();
}

TEST_F(AggregationTest, ignoreNullKeys) {
  // Some keys are null.
  auto data = makeRowVector({
      makeNullableFlatVector<int32_t>(
          {std::nullopt, 1, std::nullopt, 2, std::nullopt, 1, 2}),
      makeFlatVector<int32_t>({-1, 1, -2, 2, -3, 3, 4}),
  });

  auto makePlan = [&](bool ignoreNullKeys) {
    return PlanBuilder()
        .values({data})
        .aggregation(
            {"c0"},
            {"sum(c1)"},
            {},
            core::AggregationNode::Step::kPartial,
            ignoreNullKeys)
        .planNode();
  };

  auto expected = makeRowVector({
      makeFlatVector<int32_t>({1, 2}),
      makeFlatVector<int64_t>({4, 6}),
  });
  AssertQueryBuilder(makePlan(true)).assertResults(expected);

  expected = makeRowVector({
      makeNullableFlatVector<int32_t>({std::nullopt, 1, 2}),
      makeFlatVector<int64_t>({-6, 4, 6}),
  });
  AssertQueryBuilder(makePlan(false)).assertResults(expected);

  // All keys are null.
  data = makeRowVector({
      makeAllNullFlatVector<int32_t>(3),
      makeFlatVector<int32_t>({1, 2, 3}),
  });

  AssertQueryBuilder(makePlan(true)).assertEmptyResults();
}

// Verify that ORDER BY clause is ignored for aggregates that are not order
// sensitive.
TEST_F(AggregationTest, ignoreOrderBy) {
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 1, 2, 2, 1, 2, 1}),
      makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7}),
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60, 70}),
      makeFlatVector<int32_t>({11, 44, 22, 55, 33, 66, 77}),
  });

  createDuckDbTable({data});

  // Sorted aggregations over same inputs.
  auto plan =
      PlanBuilder()
          .values({data})
          .partialAggregation(
              {"c0"}, {"sum(c1 ORDER BY c2 DESC)", "avg(c1 ORDER BY c3)"})
          .finalAggregation()
          .planNode();

  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .assertResults("SELECT c0, sum(c1), avg(c1) FROM tmp GROUP BY 1");
}

class TestAccumulator {
 public:
  ~TestAccumulator() {
    VELOX_FAIL("Destructor should not be called.");
  }
};

class TestAggregate : public Aggregate {
 public:
  explicit TestAggregate(TypePtr resultType) : Aggregate(resultType) {}

  void addRawInput(
      char** /*groups*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {
    VELOX_UNSUPPORTED("This shouldn't get called.");
  }

  void extractValues(
      char** /*groups*/,
      int32_t /*numGroups*/,
      VectorPtr* /*result*/) override {
    VELOX_UNSUPPORTED("This shouldn't get called.");
  }

  void addIntermediateResults(
      char** /*groups*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {
    VELOX_UNSUPPORTED("This shouldn't get called.");
  }

  void addSingleGroupRawInput(
      char* /*group*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {
    VELOX_UNSUPPORTED("This shouldn't get called.");
  }

  void addSingleGroupIntermediateResults(
      char* /*group*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {
    VELOX_UNSUPPORTED("This shouldn't get called.");
  }

  void extractAccumulators(
      char** /*groups*/,
      int32_t /*numGroups*/,
      VectorPtr* /*result*/) override {
    VELOX_UNSUPPORTED("This shouldn't get called.");
  }

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(TestAccumulator);
  }

  bool destroyCalled = false;

 protected:
  void initializeNewGroupsInternal(
      char** /*groups*/,
      folly::Range<const vector_size_t*> /*indices*/) override {
    VELOX_UNSUPPORTED("This shouldn't get called.");
  }

  void destroyInternal(folly::Range<char**> groups) override {
    destroyCalled = true;
    destroyAccumulators<TestAccumulator>(groups);
  }
};

TEST_F(AggregationTest, destroyAfterPartialInitialization) {
  TestAggregate agg(INTEGER());

  Accumulator accumulator(
      true, // isFixedSize
      sizeof(TestAccumulator), // fixedSize
      true, // usesExternalMemory, this is set to force RowContainer.clear() to
            // call eraseRows.
      1, // alignment
      INTEGER(), // spillType,
      [](folly::Range<char**>, VectorPtr&) {
        VELOX_UNSUPPORTED("This shouldn't get called.");
      },
      [&agg](folly::Range<char**> groups) { agg.destroy(groups); });

  RowContainer rows(
      {}, // keyTypes
      false, // nullableKeys
      {accumulator},
      {}, // dependentTypes
      false, // hasNext
      false, // isJoinBuild
      false, // hasProbedFlag
      false, // hasNormalizedKeys
      pool());
  const auto rowColumn = rows.columnAt(0);
  agg.setOffsets(
      rowColumn.offset(),
      rowColumn.nullByte(),
      rowColumn.nullMask(),
      rowColumn.initializedByte(),
      rowColumn.initializedMask(),
      rows.rowSizeOffset());
  rows.newRow();
  rows.clear();

  ASSERT_TRUE(agg.destroyCalled);
}

DEBUG_ONLY_TEST_F(
    AggregationTest,
    uninitializedDistinctAggrWithExternalMemAggrDuringAbort) {
  const auto createInput =
      [&](int32_t startKey, uint32_t numGroups, uint32_t numElementsPerGroup) {
        return makeRowVector({
            makeFlatVector<int32_t>([&]() {
              std::vector<int32_t> keys;
              for (auto i = 0; i < numGroups; ++i) {
                for (auto j = 0; j < numElementsPerGroup; ++j) {
                  keys.push_back(startKey + i);
                }
              }
              return keys;
            }()),
            makeFlatVector<int32_t>(
                numGroups * numElementsPerGroup,
                [&](auto row) { return startKey; }),
        });
      };

  std::vector<RowVectorPtr> inputs;
  inputs.emplace_back(createInput(0, 10000, 10));
  createDuckDbTable(inputs);

  GroupingSet* groupingSet{nullptr};

  std::atomic_bool groupingSetExtracted{false};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::GroupingSet::addInputForActiveRows",
      std::function<void(GroupingSet*)>([&](GroupingSet* _groupingSet) {
        if (!groupingSetExtracted.exchange(true)) {
          groupingSet = _groupingSet;
        }
      }));

  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::MemoryPoolImpl::reserveThreadSafe",
      std::function<void(void*)>([&](void* /*unused*/) {
        if (groupingSet == nullptr) {
          return;
        }
        if (groupingSet->numRows() > 0) {
          VELOX_FAIL("Inject allocation failure.");
        }
      }));

  auto plan = PlanBuilder()
                  .values(inputs)
                  .singleAggregation({"c0"}, {"array_agg(distinct c1)"})
                  .planNode();

  VELOX_ASSERT_THROW(
      assertQuery(
          plan, "SELECT c0, array_agg(distinct c1) FROM tmp GROUP BY c0"),
      "Inject allocation failure.");
}

TEST_F(AggregationTest, nanKeys) {
  // Some keys are NaNs.
  auto kNaN = std::numeric_limits<double>::quiet_NaN();
  auto kSNaN = std::numeric_limits<double>::signaling_NaN();
  // Columns reused across test cases.
  auto c0 = makeFlatVector<double>({kNaN, 1, kNaN, 2, kSNaN, 1, 2});
  auto c1 = makeFlatVector<int32_t>({1, 1, 1, 1, 1, 1, 1});
  // Expected result columns reused across test cases. A deduplicated version of
  // c0 and c1.
  auto e0 = makeFlatVector<double>({1, 2, kNaN});
  auto e1 = makeFlatVector<int32_t>({1, 1, 1});

  auto testDistinctAgg = [&](std::vector<std::string> aggKeys,
                             std::vector<VectorPtr> inputCols,
                             std::vector<VectorPtr> expectedCols) {
    auto plan = PlanBuilder()
                    .values({makeRowVector(inputCols)})
                    .singleAggregation(aggKeys, {}, {})
                    .planNode();
    AssertQueryBuilder(plan).assertResults(makeRowVector(expectedCols));
  };

  // Test with a primitive type key.
  testDistinctAgg({"c0"}, {c0}, {e0});
  // Multiple key columns.
  testDistinctAgg({"c0", "c1"}, {c0, c1}, {e0, e1});

  // Test with a complex type key.
  testDistinctAgg({"c0"}, {makeRowVector({c0, c1})}, {makeRowVector({e0, e1})});
  // Multiple key columns.
  testDistinctAgg(
      {"c0", "c1"},
      {makeRowVector({c0, c1}), c1},
      {makeRowVector({e0, e1}), e1});
}
} // namespace facebook::velox::exec::test
