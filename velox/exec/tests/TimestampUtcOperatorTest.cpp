/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
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

#include "velox/common/file/FileSystems.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Spill.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"

namespace facebook::velox::exec {
namespace {

using namespace facebook::velox::exec::test;

struct RunOptions {
  // Compares the rows in order. Otherwise, compares them as multisets.
  bool ordered{false};
  int32_t maxDrivers{1};
  std::unordered_map<std::string, std::string> configs;
  // Spills every operator that supports spilling and checks that it did.
  bool spill{false};
};

// Makes a plan over the input batches.
using PlanFactory = std::function<core::PlanNodePtr(
    const std::vector<RowVectorPtr>& input,
    const std::shared_ptr<core::PlanNodeIdGenerator>& planNodeIdGenerator)>;

// Verifies that operators handle TIMESTAMP UTC the same way as TIMESTAMP.
//
// TIMESTAMP UTC shares the physical type of TIMESTAMP. Each test runs a plan
// twice over the same values, typed TIMESTAMP and then TIMESTAMP UTC. It checks
// that the results have the same values, and that the TIMESTAMP UTC run keeps
// TIMESTAMP UTC wherever the TIMESTAMP run produces TIMESTAMP, including in
// nested types. The session time zone is not UTC, so an operator that adjusted
// TIMESTAMP UTC values to it would produce different values.
class TimestampUtcOperatorTest : public OperatorTestBase {
 protected:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
    filesystems::registerLocalFileSystem();
    window::prestosql::registerAllWindowFunctions();
  }

  // Returns 'type' with every TIMESTAMP replaced by TIMESTAMP UTC.
  static TypePtr toTimestampUtc(const TypePtr& type) {
    if (type->equivalent(*TIMESTAMP())) {
      return TIMESTAMP_UTC();
    }
    switch (type->kind()) {
      case TypeKind::ARRAY:
        return ARRAY(toTimestampUtc(type->childAt(0)));
      case TypeKind::MAP:
        return MAP(
            toTimestampUtc(type->childAt(0)), toTimestampUtc(type->childAt(1)));
      case TypeKind::ROW: {
        const auto& rowType = type->asRow();
        std::vector<TypePtr> children;
        children.reserve(rowType.size());
        for (const auto& child : rowType.children()) {
          children.push_back(toTimestampUtc(child));
        }
        return ROW(
            std::vector<std::string>(rowType.names()), std::move(children));
      }
      default:
        return type;
    }
  }

  static bool containsTimestamp(const TypePtr& type) {
    if (type->equivalent(*TIMESTAMP())) {
      return true;
    }
    for (auto i = 0; i < type->size(); ++i) {
      if (containsTimestamp(type->childAt(i))) {
        return true;
      }
    }
    return false;
  }

  // Returns 'size' timestamps with microsecond precision. One in three rows
  // takes an edge value; the others are spread over 1900-2100. 'seed' shifts
  // the sequence, so different columns hold different values.
  static std::vector<std::optional<Timestamp>> makeTimestamps(
      vector_size_t size,
      int32_t seed) {
    // 2024-03-10 02:30:00 and 2024-11-03 01:30:00.123456 do not exist and are
    // ambiguous in America/Los_Angeles. The others are one microsecond before
    // the epoch, 1900-01-01, 9999-12-31 23:59:59.999999, 0001-01-01 and
    // 2024-01-01.
    static const std::vector<std::optional<Timestamp>> kEdgeValues = {
        Timestamp(1'710'037'800, 0),
        Timestamp(-1, 999'999'000),
        std::nullopt,
        Timestamp(1'730'597'400, 123'456'000),
        Timestamp(1'710'037'800, 0),
        Timestamp(-2'208'988'800, 0),
        Timestamp(253'402'300'799, 999'999'000),
        Timestamp(-62'135'596'800, 0),
        Timestamp(1'704'067'200, 0),
    };
    static constexpr int64_t kMicrosAt1900 = -2'208'988'800'000'000L;

    std::vector<std::optional<Timestamp>> timestamps;
    timestamps.reserve(size);
    for (auto i = 0; i < size; ++i) {
      const auto edgeIndex = (i + seed) % (kEdgeValues.size() * 3);
      if (edgeIndex < kEdgeValues.size()) {
        timestamps.push_back(kEdgeValues[edgeIndex]);
        continue;
      }
      const int64_t thousandSeconds = (i * 7'919 + seed * 104'729) % 6'311'433;
      const int64_t micros = (i * 31 + seed) % 1'000'000;
      timestamps.push_back(
          Timestamp::fromMicros(
              kMicrosAt1900 + thousandSeconds * 1'000'000'000L + micros));
    }
    return timestamps;
  }

  // Returns batches with columns id INTEGER, k INTEGER, ts T, ts2 T and
  // arr ARRAY(T), where T is 'timestampType'. The values are the same for every
  // 'timestampType'.
  std::vector<RowVectorPtr> makeInput(const TypePtr& timestampType) {
    static constexpr vector_size_t kBatchSize = 300;
    static constexpr int32_t kNumBatches = 3;
    std::vector<RowVectorPtr> batches;
    batches.reserve(kNumBatches);
    for (auto batch = 0; batch < kNumBatches; ++batch) {
      const auto timestamps = makeTimestamps(kBatchSize, batch * 5);
      const auto otherTimestamps = makeTimestamps(kBatchSize, batch * 5 + 4);
      std::vector<std::vector<std::optional<Timestamp>>> arrays;
      arrays.reserve(kBatchSize);
      for (auto i = 0; i < kBatchSize; ++i) {
        arrays.push_back({timestamps[i], otherTimestamps[i]});
      }
      batches.push_back(makeRowVector(
          {"id", "k", "ts", "ts2", "arr"},
          {
              makeFlatVector<int32_t>(
                  kBatchSize,
                  [&](auto row) { return batch * kBatchSize + row; }),
              makeFlatVector<int32_t>(
                  kBatchSize, [](auto row) { return row % 7; }),
              makeNullableFlatVector<Timestamp>(timestamps, timestampType),
              makeNullableFlatVector<Timestamp>(otherTimestamps, timestampType),
              makeNullableArrayVector<Timestamp>(arrays, ARRAY(timestampType)),
          }));
    }
    return batches;
  }

  static std::vector<std::string> toStrings(
      const RowVectorPtr& result,
      bool ordered) {
    std::vector<std::string> rows;
    rows.reserve(result->size());
    for (auto i = 0; i < result->size(); ++i) {
      rows.push_back(result->toString(i));
    }
    if (!ordered) {
      std::sort(rows.begin(), rows.end());
    }
    return rows;
  }

  // Runs the plan over input of 'timestampType'. Returns the results and the
  // number of bytes spilled.
  std::pair<RowVectorPtr, uint64_t> run(
      const TypePtr& timestampType,
      const PlanFactory& makePlan,
      const RunOptions& options) {
    const auto plan = makePlan(
        makeInput(timestampType),
        std::make_shared<core::PlanNodeIdGenerator>());
    AssertQueryBuilder builder(plan);
    builder.maxDrivers(options.maxDrivers)
        .config(core::QueryConfig::kSessionTimezone, "America/Los_Angeles")
        .config(core::QueryConfig::kAdjustTimestampToTimezone, "true")
        .configs(options.configs);

    std::shared_ptr<TempDirectoryPath> spillDirectory;
    std::unique_ptr<TestScopedSpillInjection> spillInjection;
    if (options.spill) {
      spillDirectory = TempDirectoryPath::create();
      spillInjection = std::make_unique<TestScopedSpillInjection>(100);
      builder.spillDirectory(spillDirectory->getPath())
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kAggregationSpillEnabled, "true")
          .config(core::QueryConfig::kJoinSpillEnabled, "true")
          .config(core::QueryConfig::kOrderBySpillEnabled, "true")
          .config(core::QueryConfig::kWindowSpillEnabled, "true")
          .config(core::QueryConfig::kRowNumberSpillEnabled, "true")
          .config(core::QueryConfig::kTopNRowNumberSpillEnabled, "true");
    }

    std::shared_ptr<Task> task;
    auto result = builder.copyResults(pool(), task);
    uint64_t spilledBytes{0};
    for (const auto& [_, stats] : toPlanStats(task->taskStats())) {
      spilledBytes += stats.spilledBytes;
    }
    return {result, spilledBytes};
  }

  void assertSameAsTimestamp(
      const PlanFactory& makePlan,
      const RunOptions& options = {}) {
    const auto [expected, expectedSpilledBytes] =
        run(TIMESTAMP(), makePlan, options);
    const auto [actual, actualSpilledBytes] =
        run(TIMESTAMP_UTC(), makePlan, options);

    ASSERT_GT(expected->size(), 0);
    ASSERT_TRUE(containsTimestamp(expected->type()))
        << "Plan output must contain TIMESTAMP: "
        << expected->type()->toString();
    ASSERT_TRUE(toTimestampUtc(expected->type())->equivalent(*actual->type()))
        << "Expected type: " << toTimestampUtc(expected->type())->toString()
        << ", actual type: " << actual->type()->toString();
    ASSERT_EQ(
        toStrings(expected, options.ordered),
        toStrings(actual, options.ordered));

    if (options.spill) {
      ASSERT_GT(actualSpilledBytes, 0);
      // Spilling must not change the results.
      auto noSpillOptions = options;
      noSpillOptions.spill = false;
      const auto [noSpill, _] = run(TIMESTAMP(), makePlan, noSpillOptions);
      ASSERT_EQ(
          toStrings(noSpill, options.ordered),
          toStrings(actual, options.ordered));
    }
  }

  // Returns the input projected to the build-side columns of a join.
  static PlanBuilder buildSide(
      const std::vector<RowVectorPtr>& input,
      const std::shared_ptr<core::PlanNodeIdGenerator>& planNodeIdGenerator) {
    return std::move(
        PlanBuilder(planNodeIdGenerator)
            .values(input)
            .project(
                {"id AS b_id", "ts AS b_ts", "ts2 AS b_ts2", "arr AS b_arr"}));
  }
};

TEST_F(TimestampUtcOperatorTest, orderBy) {
  for (const auto& keys : std::vector<std::vector<std::string>>{
           {"ts", "id"},
           {"ts DESC NULLS FIRST", "id"},
           {"arr", "id"},
       }) {
    SCOPED_TRACE(folly::join(", ", keys));
    for (const auto* prefixSortMaxBytes : {"0", "1024"}) {
      SCOPED_TRACE(
          fmt::format("Prefix sort max bytes: {}", prefixSortMaxBytes));
      for (bool spill : {false, true}) {
        SCOPED_TRACE(fmt::format("Spill: {}", spill));
        assertSameAsTimestamp(
            [&](const auto& input, const auto& planNodeIdGenerator) {
              return PlanBuilder(planNodeIdGenerator)
                  .values(input)
                  .orderBy(keys, false)
                  .planNode();
            },
            {.ordered = true,
             .configs =
                 {{core::QueryConfig::kPrefixSortNormalizedKeyMaxBytes,
                   prefixSortMaxBytes},
                  {core::QueryConfig::kSpillPrefixSortEnabled, "true"}},
             .spill = spill});
      }
    }
  }
}

TEST_F(TimestampUtcOperatorTest, topN) {
  assertSameAsTimestamp(
      [](const auto& input, const auto& planNodeIdGenerator) {
        return PlanBuilder(planNodeIdGenerator)
            .values(input)
            .topN({"ts DESC", "id"}, 50, false)
            .planNode();
      },
      {.ordered = true});
}

TEST_F(TimestampUtcOperatorTest, limit) {
  assertSameAsTimestamp(
      [](const auto& input, const auto& planNodeIdGenerator) {
        return PlanBuilder(planNodeIdGenerator)
            .values(input)
            .orderBy({"ts2", "id"}, false)
            .limit(10, 100, false)
            .planNode();
      },
      {.ordered = true});
}

TEST_F(TimestampUtcOperatorTest, hashAggregation) {
  for (bool spill : {false, true}) {
    SCOPED_TRACE(fmt::format("Spill: {}", spill));
    assertSameAsTimestamp(
        [](const auto& input, const auto& planNodeIdGenerator) {
          return PlanBuilder(planNodeIdGenerator)
              .values(input)
              .singleAggregation(
                  {"ts"}, {"count(1)", "min(ts2)", "max(ts2)", "count(ts2)"})
              .planNode();
        },
        {.spill = spill});
  }
  {
    // 'arbitrary' depends on the input order, which spilling changes.
    SCOPED_TRACE("Array of TIMESTAMP UTC");
    assertSameAsTimestamp(
        [](const auto& input, const auto& planNodeIdGenerator) {
          return PlanBuilder(planNodeIdGenerator)
              .values(input)
              .singleAggregation({"ts"}, {"arbitrary(arr)"})
              .planNode();
        });
  }
  {
    SCOPED_TRACE("Global aggregation");
    assertSameAsTimestamp(
        [](const auto& input, const auto& planNodeIdGenerator) {
          return PlanBuilder(planNodeIdGenerator)
              .values(input)
              .singleAggregation({}, {"min(ts)", "max(ts)", "max(ts2)"})
              .planNode();
        });
  }
}

TEST_F(TimestampUtcOperatorTest, streamingAggregation) {
  assertSameAsTimestamp([](const auto& input, const auto& planNodeIdGenerator) {
    return PlanBuilder(planNodeIdGenerator)
        .values(input)
        .orderBy({"ts"}, false)
        .streamingAggregation(
            {"ts"},
            {"count(1)", "max(ts2)"},
            {},
            core::AggregationNode::Step::kSingle,
            false)
        .planNode();
  });
}

TEST_F(TimestampUtcOperatorTest, hashJoin) {
  using core::JoinType;
  for (auto joinType :
       {JoinType::kInner, JoinType::kLeft, JoinType::kRight, JoinType::kFull}) {
    SCOPED_TRACE(fmt::format("Join type: {}", joinType));
    for (const auto* filter : {"", "(id + b_id) % 3 = 0"}) {
      SCOPED_TRACE(fmt::format("Filter: '{}'", filter));
      for (bool spill : {false, true}) {
        SCOPED_TRACE(fmt::format("Spill: {}", spill));
        assertSameAsTimestamp(
            [&](const auto& input, const auto& planNodeIdGenerator) {
              return PlanBuilder(planNodeIdGenerator)
                  .values(input)
                  .hashJoin(
                      {"ts"},
                      {"b_ts2"},
                      buildSide(input, planNodeIdGenerator).planNode(),
                      filter,
                      {"id", "ts", "arr", "b_id", "b_ts2", "b_arr"},
                      joinType)
                  .planNode();
            },
            {.spill = spill});
      }
    }
  }
  for (auto joinType : {JoinType::kLeftSemiFilter, JoinType::kAnti}) {
    SCOPED_TRACE(fmt::format("Join type: {}", joinType));
    assertSameAsTimestamp(
        [&](const auto& input, const auto& planNodeIdGenerator) {
          return PlanBuilder(planNodeIdGenerator)
              .values(input)
              .hashJoin(
                  {"ts"},
                  {"b_ts2"},
                  buildSide(input, planNodeIdGenerator).planNode(),
                  "",
                  {"id", "ts", "arr"},
                  joinType)
              .planNode();
        });
  }
}

TEST_F(TimestampUtcOperatorTest, mergeJoin) {
  for (auto joinType : {core::JoinType::kInner, core::JoinType::kLeft}) {
    SCOPED_TRACE(fmt::format("Join type: {}", joinType));
    assertSameAsTimestamp(
        [&](const auto& input, const auto& planNodeIdGenerator) {
          auto build = buildSide(input, planNodeIdGenerator)
                           .orderBy({"b_ts2"}, false)
                           .planNode();
          return PlanBuilder(planNodeIdGenerator)
              .values(input)
              .orderBy({"ts"}, false)
              .mergeJoin(
                  {"ts"},
                  {"b_ts2"},
                  build,
                  "",
                  {"id", "ts", "b_id", "b_ts2", "b_arr"},
                  joinType)
              .planNode();
        });
  }
}

TEST_F(TimestampUtcOperatorTest, nestedLoopJoin) {
  for (auto joinType :
       {core::JoinType::kInner, core::JoinType::kLeft, core::JoinType::kFull}) {
    SCOPED_TRACE(fmt::format("Join type: {}", joinType));
    assertSameAsTimestamp(
        [&](const auto& input, const auto& planNodeIdGenerator) {
          return PlanBuilder(planNodeIdGenerator)
              .values({input[0]})
              .nestedLoopJoin(
                  buildSide({input[1]}, planNodeIdGenerator).planNode(),
                  "(id + b_id) % 7 = 0",
                  {"id", "ts", "b_id", "b_ts2", "b_arr"},
                  joinType)
              .planNode();
        });
  }
}

TEST_F(TimestampUtcOperatorTest, window) {
  for (bool spill : {false, true}) {
    SCOPED_TRACE(fmt::format("Spill: {}", spill));
    assertSameAsTimestamp(
        [](const auto& input, const auto& planNodeIdGenerator) {
          // A window node has one window specification.
          return PlanBuilder(planNodeIdGenerator)
              .values(input)
              .window({
                  "row_number() over (partition by k order by ts, id)",
                  "rank() over (partition by k order by ts, id)",
                  "percent_rank() over (partition by k order by ts, id)",
                  "min(ts2) over (partition by k order by ts, id "
                  "range between unbounded preceding and current row)",
              })
              .window({"dense_rank() over (partition by k order by ts desc)"})
              .window({
                  "lag(ts) over (partition by k order by id)",
                  "lead(arr) over (partition by k order by id)",
                  "nth_value(ts2, 2) over (partition by k order by id "
                  "rows between unbounded preceding and current row)",
              })
              .window({"max(ts) over (partition by k)"})
              .window({"count(ts) over (partition by ts)"})
              .planNode();
        },
        {.spill = spill});
  }
}

TEST_F(TimestampUtcOperatorTest, rowNumber) {
  for (bool spill : {false, true}) {
    SCOPED_TRACE(fmt::format("Spill: {}", spill));
    assertSameAsTimestamp(
        [](const auto& input, const auto& planNodeIdGenerator) {
          return PlanBuilder(planNodeIdGenerator)
              .values(input)
              .rowNumber({"ts"})
              .planNode();
        },
        {.spill = spill});
  }
}

TEST_F(TimestampUtcOperatorTest, topNRowNumber) {
  for (bool spill : {false, true}) {
    SCOPED_TRACE(fmt::format("Spill: {}", spill));
    assertSameAsTimestamp(
        [](const auto& input, const auto& planNodeIdGenerator) {
          return PlanBuilder(planNodeIdGenerator)
              .values(input)
              .topNRowNumber({"k"}, {"ts DESC", "id"}, 3, true)
              .planNode();
        },
        {.spill = spill});
  }
}

TEST_F(TimestampUtcOperatorTest, localPartition) {
  assertSameAsTimestamp(
      [](const auto& input, const auto& planNodeIdGenerator) {
        return PlanBuilder(planNodeIdGenerator)
            .localPartition(
                {"ts"},
                {PlanBuilder(planNodeIdGenerator)
                     .values(input, true)
                     .planNode()})
            .planNode();
      },
      {.maxDrivers = 4});
}

TEST_F(TimestampUtcOperatorTest, localMerge) {
  assertSameAsTimestamp(
      [](const auto& input, const auto& planNodeIdGenerator) {
        return PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {"ts", "id"},
                {PlanBuilder(planNodeIdGenerator)
                     .values(input, true)
                     .orderBy({"ts", "id"}, true)
                     .planNode()})
            .planNode();
      },
      {.ordered = true, .maxDrivers = 4});
}

} // namespace
} // namespace facebook::velox::exec
