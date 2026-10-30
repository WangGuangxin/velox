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
#include "velox/core/PlanFragment.h"
#include "velox/exec/Task.h"
#include "velox/functions/prestosql/aggregates/CountAggregate.h"
#include "velox/functions/prestosql/aggregates/SumAggregate.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>
#include <map>

namespace facebook::velox::exec::test {

class GroupingKeyReuseTest : public testing::Test,
                             public velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    aggregate::prestosql::registerCountAggregate({"batch_count"}, false, true);
    aggregate::prestosql::registerSumAggregate({"batch_sum"}, false, true);
  }

  core::TypedExprPtr field(const TypePtr& type, const std::string& name) {
    return std::make_shared<core::FieldAccessTypedExpr>(type, name);
  }

  core::TypedExprPtr null(const TypePtr& type) {
    return std::make_shared<core::ConstantTypedExpr>(
        type, variant::null(type->kind()));
  }

  core::TypedExprPtr gid(int64_t value) {
    return std::make_shared<core::ConstantTypedExpr>(BIGINT(), variant(value));
  }

  using Settings = std::unordered_map<std::string, std::string>;

  Settings baseSettings(bool enabled, uint32_t batchRows = 64) {
    return {
        {core::QueryConfig::kGroupingSetsKeyReuseEnabled,
         enabled ? "true" : "false"},
        {core::QueryConfig::kPreferredOutputBatchRows,
         std::to_string(batchRows)},
    };
  }

  std::map<std::string, int> executeWith(
      const core::PlanNodePtr& plan,
      Settings settings) {
    auto context =
        core::QueryCtx::create(nullptr, core::QueryConfig(std::move(settings)));
    auto task = Task::create(
        "grouping-key-reuse-test",
        core::PlanFragment(plan),
        0,
        context,
        Task::ExecutionMode::kSerial);
    std::map<std::string, int> result;
    while (auto output = task->next()) {
      // Dictionary debug strings contain indices. Compare materialized values
      // so encoding differences do not masquerade as result differences.
      auto materialized =
          BaseVector::create(output->type(), output->size(), pool());
      materialized->copy(output.get(), 0, 0, output->size());
      for (auto row = 0; row < output->size(); ++row) {
        ++result[materialized->toString(row)];
      }
    }
    EXPECT_TRUE(task->isFinished());
    lastReusedRows_ = 0;
    for (const auto& pipeline : task->taskStats().pipelineStats) {
      for (const auto& op : pipeline.operatorStats) {
        if (auto it = op.runtimeStats.find("groupingKeyRowsReused");
            it != op.runtimeStats.end()) {
          lastReusedRows_ += it->second.sum;
        }
      }
    }
    return result;
  }

  std::map<std::string, int> execute(
      const core::PlanNodePtr& plan,
      bool enabled,
      uint32_t batchRows = 64,
      bool forcePartialFlush = false,
      bool forceGeneric = false) {
    auto settings = baseSettings(enabled, batchRows);
    if (forcePartialFlush) {
      settings[core::QueryConfig::kMaxPartialAggregationMemory] = "1";
      settings[core::QueryConfig::kMaxExtendedPartialAggregationMemory] = "1";
    }
    if (forceGeneric) {
      settings[core::QueryConfig::kHashAdaptivityEnabled] = "false";
    }
    return executeWith(plan, std::move(settings));
  }

  uint64_t lastReusedRows_{0};

  std::shared_ptr<core::ExpandNode> rollup(
      const std::vector<RowVectorPtr>& input) {
    auto values = std::make_shared<core::ValuesNode>("values", input);
    auto key = field(BIGINT(), "k");
    auto text = field(VARCHAR(), "s");
    auto amount = field(BIGINT(), "v");
    return std::make_shared<core::ExpandNode>(
        "expand",
        std::vector<std::vector<core::TypedExprPtr>>{
            // A real rollup keeps each key in several grouping sets, which is
            // what gives the key contribution cache anything to reuse.
            {key, text, amount, gid(0)},
            {key, text, amount, gid(1)},
            {key, null(VARCHAR()), amount, gid(2)},
            {null(BIGINT()), null(VARCHAR()), amount, gid(3)},
            // Duplicate grouping set must preserve its multiplicity.
            {key, null(VARCHAR()), amount, gid(2)},
        },
        std::vector<std::string>{"k", "s", "v", "gid"},
        values);
  }

  // Keys grow without bound, so the table leaves array/normalized-key mode
  // partway through and reassigns value-ID multipliers.
  RowVectorPtr wideData(vector_size_t count, int64_t base) {
    return makeRowVector(
        {"k", "s", "v"},
        {
            makeFlatVector<int64_t>(
                count, [&](auto row) { return base + row * 1'000'003; }),
            makeFlatVector<std::string>(
                count,
                [&](auto row) {
                  return "key-" + std::to_string(base + row);
                }),
            makeFlatVector<int64_t>(count, [](auto row) { return row; }),
        });
  }

  core::PlanNodePtr countByKeys(const core::PlanNodePtr& source) {
    return std::make_shared<core::AggregationNode>(
        "aggregate",
        core::AggregationNode::Step::kSingle,
        std::vector<core::FieldAccessTypedExprPtr>{
            std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "k"),
            std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "s"),
            std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "gid")},
        std::vector<core::FieldAccessTypedExprPtr>{},
        std::vector<std::string>{"n"},
        std::vector<core::AggregationNode::Aggregate>{
            {std::make_shared<core::CallTypedExpr>(
                 BIGINT(), std::vector<core::TypedExprPtr>{}, "batch_count"),
             {}}},
        false,
        false,
        source);
  }

  RowVectorPtr data(vector_size_t count) {
    return makeRowVector(
        {"k", "s", "v"},
        {
            makeFlatVector<int64_t>(
                count,
                [](auto row) { return row % 17; },
                velox::test::VectorMaker::nullEvery(11)),
            makeFlatVector<std::string>(
                count,
                [](auto row) {
                  return "long-group-key-" + std::to_string(row % 13);
                },
                velox::test::VectorMaker::nullEvery(7)),
            makeFlatVector<int64_t>(
                count, [](auto row) { return row % 23 - 11; }),
        });
  }
};

TEST_F(GroupingKeyReuseTest, expandNullsDuplicateSetsAndBatchBoundaries) {
  auto plan = rollup({data(127), data(19), data(0)});
  const auto expected = execute(plan, false);
  for (auto batchRows : {4, 7, 64, 8192}) {
    EXPECT_EQ(execute(plan, true, batchRows), expected);
  }
}

TEST_F(GroupingKeyReuseTest, aggregateCountsAndSums) {
  auto input = rollup({data(1024), data(819)});
  auto plan = std::make_shared<core::AggregationNode>(
      "aggregate",
      core::AggregationNode::Step::kSingle,
      std::vector<core::FieldAccessTypedExprPtr>{
          std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "k"),
          std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "s"),
          std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "gid")},
      std::vector<core::FieldAccessTypedExprPtr>{},
      std::vector<std::string>{"n", "total"},
      std::vector<core::AggregationNode::Aggregate>{
          {std::make_shared<core::CallTypedExpr>(
               BIGINT(), std::vector<core::TypedExprPtr>{}, "batch_count"),
           {}},
          {std::make_shared<core::CallTypedExpr>(
               BIGINT(),
               std::vector<core::TypedExprPtr>{field(BIGINT(), "v")},
               "batch_sum"),
           {BIGINT()}}},
      false,
      false,
      input);
  EXPECT_EQ(execute(plan, false), execute(plan, true));
}

TEST_F(GroupingKeyReuseTest, unsupportedProjectionShapesRetainResults) {
  auto values = std::make_shared<core::ValuesNode>(
      "values", std::vector<RowVectorPtr>{data(101)});
  auto mixed = std::make_shared<core::ExpandNode>(
      "mixed",
      std::vector<std::vector<core::TypedExprPtr>>{
          {field(BIGINT(), "k")}, {gid(7)}},
      std::vector<std::string>{"a"},
      values);
  EXPECT_EQ(execute(mixed, false), execute(mixed, true));
  auto differentFields = std::make_shared<core::ExpandNode>(
      "fields",
      std::vector<std::vector<core::TypedExprPtr>>{
          {field(BIGINT(), "k")}, {field(BIGINT(), "v")}},
      std::vector<std::string>{"a"},
      values);
  EXPECT_EQ(execute(differentFields, false), execute(differentFields, true));
  // More sets than the output row target also uses the original path.
  auto plan = rollup({data(11)});
  EXPECT_EQ(execute(plan, false, 2), execute(plan, true, 2));
}

TEST_F(GroupingKeyReuseTest, decimalAndDictionaryInputs) {
  const auto decimal = DECIMAL(30, 4);
  auto amounts = makeFlatVector<int128_t>(
      128,
      [](auto row) { return (int128_t(row) << 65) + row; },
      nullptr,
      decimal);
  auto wrapped = BaseVector::wrapInDictionary(
      makeNulls(257, velox::test::VectorMaker::nullEvery(7)),
      makeIndices(257, [](auto row) { return row % 128; }),
      257,
      amounts);
  auto values = std::make_shared<core::ValuesNode>(
      "values", std::vector<RowVectorPtr>{makeRowVector({"d"}, {wrapped})});
  auto plan = std::make_shared<core::ExpandNode>(
      "decimal-expand",
      std::vector<std::vector<core::TypedExprPtr>>{
          {field(decimal, "d"), gid(0)},
          {null(decimal), gid(1)},
          {field(decimal, "d"), gid(2)}},
      std::vector<std::string>{"d", "gid"},
      values);
  EXPECT_EQ(execute(plan, false), execute(plan, true, 35));
}

TEST_F(GroupingKeyReuseTest, partialFlushAndFinalAggregation) {
  auto input = rollup({data(257), data(131)});
  std::vector<core::FieldAccessTypedExprPtr> keys{
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "k"),
      std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "s"),
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "gid")};
  auto partial = std::make_shared<core::AggregationNode>(
      "partial",
      core::AggregationNode::Step::kPartial,
      keys,
      std::vector<core::FieldAccessTypedExprPtr>{},
      std::vector<std::string>{"n"},
      std::vector<core::AggregationNode::Aggregate>{
          {std::make_shared<core::CallTypedExpr>(
               BIGINT(), std::vector<core::TypedExprPtr>{}, "batch_count"),
           {}}},
      false,
      false,
      input);
  auto final = std::make_shared<core::AggregationNode>(
      "final",
      core::AggregationNode::Step::kFinal,
      keys,
      std::vector<core::FieldAccessTypedExprPtr>{},
      std::vector<std::string>{"n"},
      std::vector<core::AggregationNode::Aggregate>{
          {std::make_shared<core::CallTypedExpr>(
               BIGINT(),
               std::vector<core::TypedExprPtr>{field(BIGINT(), "n")},
               "batch_count"),
           {}}},
      false,
      false,
      partial);
  EXPECT_EQ(execute(final, false, 64, true), execute(final, true, 64, true));
}

TEST_F(GroupingKeyReuseTest, genericAggregationReusesExpandContributions) {
  auto input = rollup({data(127), data(19)});
  auto projected = std::make_shared<core::ProjectNode>(
      "pass-through",
      std::vector<std::string>{"k", "s", "v", "gid"},
      std::vector<core::TypedExprPtr>{
          field(BIGINT(), "k"),
          field(VARCHAR(), "s"),
          field(BIGINT(), "v"),
          field(BIGINT(), "gid")},
      input);
  auto plan = std::make_shared<core::AggregationNode>(
      "aggregate",
      core::AggregationNode::Step::kSingle,
      std::vector<core::FieldAccessTypedExprPtr>{
          std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "k"),
          std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "s"),
          std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "gid")},
      std::vector<core::FieldAccessTypedExprPtr>{},
      std::vector<std::string>{"n"},
      std::vector<core::AggregationNode::Aggregate>{
          {std::make_shared<core::CallTypedExpr>(
               BIGINT(), std::vector<core::TypedExprPtr>{}, "batch_count"),
           {}}},
      false,
      false,
      projected);
  const auto expected = execute(plan, false, 64, false, true);
  EXPECT_EQ(execute(plan, true, 64, false, true), expected);
  EXPECT_GT(lastReusedRows_, 0);
}

TEST_F(GroupingKeyReuseTest, computedGroupingKeyDisablesCrossBatchCache) {
  auto expanded = rollup({data(127), data(19)});
  auto input = std::make_shared<core::ProjectNode>(
      "project",
      std::vector<std::string>{"k", "s", "v", "gid"},
      std::vector<core::TypedExprPtr>{
          gid(7),
          field(VARCHAR(), "s"),
          field(BIGINT(), "v"),
          field(BIGINT(), "gid")},
      expanded);
  auto plan = std::make_shared<core::AggregationNode>(
      "aggregate",
      core::AggregationNode::Step::kSingle,
      std::vector<core::FieldAccessTypedExprPtr>{
          std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "k"),
          std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "s"),
          std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "gid")},
      std::vector<core::FieldAccessTypedExprPtr>{},
      std::vector<std::string>{"n"},
      std::vector<core::AggregationNode::Aggregate>{
          {std::make_shared<core::CallTypedExpr>(
               BIGINT(), std::vector<core::TypedExprPtr>{}, "batch_count"),
           {}}},
      false,
      false,
      input);
  const auto expected = execute(plan, false, 64, false, true);
  EXPECT_EQ(execute(plan, true, 64, false, true), expected);
  EXPECT_EQ(lastReusedRows_, 0);
}
TEST_F(GroupingKeyReuseTest, valueIdModeReusesContributions) {
  // Adaptivity left on, so the keys map to value IDs rather than hashes.
  auto plan = countByKeys(rollup({data(127), data(19)}));
  const auto expected = execute(plan, false);
  EXPECT_EQ(execute(plan, true), expected);
  EXPECT_GT(lastReusedRows_, 0);
}

TEST_F(GroupingKeyReuseTest, hashModeChangeInvalidatesContributions) {
  // Each batch adds keys the value-ID dictionary has not seen, so the table
  // changes hash mode mid-stream and the multipliers behind every cached
  // contribution change with it.
  std::vector<RowVectorPtr> batches;
  for (int64_t batch = 0; batch < 24; ++batch) {
    batches.push_back(wideData(512, batch * 1'000'000));
  }
  auto plan = countByKeys(rollup(batches));
  EXPECT_EQ(execute(plan, true), execute(plan, false));
}

TEST_F(GroupingKeyReuseTest, budgetTooSmallFallsBackToComputing) {
  auto plan = countByKeys(rollup({data(127)}));
  const auto expected = execute(plan, false);
  auto settings = baseSettings(true);
  settings[core::QueryConfig::kGroupingSetsKeyReuseMaxBytes] = "0";
  EXPECT_EQ(executeWith(plan, settings), expected);
  EXPECT_EQ(lastReusedRows_, 0);
}

} // namespace facebook::velox::exec::test
