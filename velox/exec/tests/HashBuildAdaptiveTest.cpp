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
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>
#include <map>

namespace facebook::velox::exec::test {
class HashBuildAdaptiveTest : public testing::Test,
                              public velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  struct Result {
    std::map<std::string, int> rows;
    uint64_t analysisRows{0};
    uint64_t abandonedDictionaries{0};
    uint64_t batchStoredRows{0};
    uint64_t scalarStoredRows{0};
  };

  core::HashJoinNodePtr join(
      VectorPtr probe,
      const std::vector<VectorPtr>& builds,
      core::JoinType type,
      bool nullAware = false,
      bool nullAsValue = false) {
    auto left = std::make_shared<core::ValuesNode>(
        "probe", std::vector<RowVectorPtr>{makeRowVector({"k"}, {probe})});
    std::vector<RowVectorPtr> inputs;
    for (auto& build : builds) {
      inputs.push_back(makeRowVector({"bk"}, {build}));
    }
    auto right = std::make_shared<core::ValuesNode>("build", inputs);
    return std::make_shared<core::HashJoinNode>(
        "join",
        type,
        nullAware,
        std::vector<core::FieldAccessTypedExprPtr>{
            std::make_shared<core::FieldAccessTypedExpr>(probe->type(), "k")},
        std::vector<core::FieldAccessTypedExprPtr>{
            std::make_shared<core::FieldAccessTypedExpr>(probe->type(), "bk")},
        nullptr,
        left,
        right,
        left->outputType(),
        false,
        nullAsValue);
  }

  Result run(
      const core::HashJoinNodePtr& plan,
      bool enabled,
      int dedupPct,
      bool batchStore = false) {
    auto context = core::QueryCtx::create(
        nullptr,
        core::QueryConfig({
            {core::QueryConfig::kHashJoinAdaptiveValueIdsEnabled,
             enabled ? "true" : "false"},
            {core::QueryConfig::kHashJoinAdaptiveValueIdsMinRows, "32"},
            {core::QueryConfig::kHashJoinAdaptiveValueIdsMinDistinctPct, "80"},
            {core::QueryConfig::kHashBuildBatchStoreEnabled,
             batchStore ? "true" : "false"},
            {core::QueryConfig::kAbandonDedupHashMapMinPct,
             std::to_string(dedupPct)},
            {core::QueryConfig::kPreferredOutputBatchRows, "17"},
        }));
    auto task = Task::create(
        "adaptive-append-test",
        core::PlanFragment(plan),
        0,
        context,
        Task::ExecutionMode::kSerial);
    Result result;
    while (auto output = task->next()) {
      auto materialized =
          BaseVector::create(output->type(), output->size(), pool());
      materialized->copy(output.get(), 0, 0, output->size());
      for (auto row = 0; row < output->size(); ++row) {
        ++result.rows[materialized->toString(row)];
      }
    }
    EXPECT_TRUE(task->isFinished());
    for (auto& pipeline : task->taskStats().pipelineStats) {
      for (auto& op : pipeline.operatorStats) {
        if (auto it = op.runtimeStats.find("valueIdAnalysisRows");
            it != op.runtimeStats.end()) {
          result.analysisRows += it->second.sum;
        }
        if (auto it = op.runtimeStats.find("valueIdAbandonedDictionaries");
            it != op.runtimeStats.end()) {
          result.abandonedDictionaries += it->second.sum;
        }
        if (auto it = op.runtimeStats.find("hashBuildBatchStoredRows");
            it != op.runtimeStats.end()) {
          result.batchStoredRows += it->second.sum;
        }
        if (auto it = op.runtimeStats.find("hashBuildScalarStoredRows");
            it != op.runtimeStats.end()) {
          result.scalarStoredRows += it->second.sum;
        }
      }
    }
    return result;
  }
};

TEST_F(HashBuildAdaptiveTest, dedupEligibleAppendIntegerJoins) {
  auto probe = makeFlatVector<int64_t>(
      257,
      [](auto row) { return row * 2; },
      velox::test::VectorMaker::nullEvery(11));
  auto build = makeFlatVector<int64_t>(
      129,
      [](auto row) { return row; },
      velox::test::VectorMaker::nullEvery(13));
  for (auto type : {core::JoinType::kLeftSemiFilter, core::JoinType::kAnti}) {
    auto plan = join(probe, {build, build, build}, type);
    ASSERT_TRUE(plan->canDropDuplicates());
    auto original = run(plan, false, 0);
    auto adapted = run(plan, true, 0);
    EXPECT_EQ(adapted.rows, original.rows);
    EXPECT_GT(adapted.analysisRows, 0);
    EXPECT_GT(adapted.abandonedDictionaries, 0);
    EXPECT_EQ(original.analysisRows, 0);
    EXPECT_FALSE(adapted.rows.empty());
  }
}

TEST_F(HashBuildAdaptiveTest, dictionaryStringsAndNullAsValue) {
  auto probe = makeFlatVector<std::string>(257, [](auto row) {
    return "long-adaptive-key-" + std::to_string(row * 2);
  });
  auto base = makeFlatVector<std::string>(
      129,
      [](auto row) { return "long-adaptive-key-" + std::to_string(row); },
      velox::test::VectorMaker::nullEvery(13));
  auto indices = makeIndices(258, [](auto row) { return row % 129; });
  auto build = BaseVector::wrapInDictionary(nullptr, indices, 258, base);
  for (bool nullAsValue : {false, true}) {
    auto plan = join(
        probe,
        {build, base},
        core::JoinType::kLeftSemiFilter,
        false,
        nullAsValue);
    auto adapted = run(plan, true, 0);
    EXPECT_EQ(adapted.rows, run(plan, false, 0).rows);
    EXPECT_GT(adapted.analysisRows, 0);
  }
}

TEST_F(HashBuildAdaptiveTest, actualDedupAndCountingRetainLegacyAnalysis) {
  auto probe = makeFlatVector<int64_t>({1, 2, 2, 3, 129});
  auto build = makeFlatVector<int64_t>(129, [](auto row) { return row; });
  for (auto type : {core::JoinType::kLeftSemiFilter, core::JoinType::kAnti}) {
    auto plan = join(probe, {build, build}, type);
    auto adapted = run(plan, true, 100);
    EXPECT_EQ(adapted.rows, run(plan, false, 100).rows);
    EXPECT_EQ(adapted.analysisRows, 0);
  }
  for (auto type :
       {core::JoinType::kCountingLeftSemiFilter,
        core::JoinType::kCountingAnti}) {
    auto plan = join(probe, {build, build}, type);
    auto adapted = run(plan, true, 0);
    EXPECT_EQ(adapted.rows, run(plan, false, 0).rows);
    EXPECT_EQ(adapted.analysisRows, 0);
  }
}

TEST_F(HashBuildAdaptiveTest, nullAwareAntiPreservesEmptyOutput) {
  auto probe = makeFlatVector<int64_t>({1, 2, 3, 129});
  auto build = makeNullableFlatVector<int64_t>({1, 2, std::nullopt});
  auto plan = join(probe, {build}, core::JoinType::kAnti, true);
  EXPECT_TRUE(run(plan, false, 0).rows.empty());
  EXPECT_TRUE(run(plan, true, 0).rows.empty());
}

TEST_F(HashBuildAdaptiveTest, batchStoreDenseSparseAndSmallInputs) {
  auto probe = makeFlatVector<int64_t>(257, [](auto row) { return row * 2; });
  auto dense = makeFlatVector<int64_t>(129, [](auto row) { return row; });
  auto sparse = makeFlatVector<int64_t>(
      129,
      [](auto row) { return row; },
      velox::test::VectorMaker::nullEvery(13));
  auto small = makeFlatVector<int64_t>({1, 2, 3});
  for (auto type : {core::JoinType::kLeftSemiFilter, core::JoinType::kAnti}) {
    for (auto input : {dense, sparse, small}) {
      auto plan = join(probe, {input, input}, type);
      auto batched = run(plan, false, 0, true);
      EXPECT_EQ(batched.rows, run(plan, false, 0, false).rows);
      if (input == dense) {
        EXPECT_EQ(batched.batchStoredRows, 258);
        EXPECT_EQ(batched.scalarStoredRows, 0);
      } else {
        EXPECT_EQ(batched.batchStoredRows, 0);
        EXPECT_GT(batched.scalarStoredRows, 0);
      }
    }
  }
}

TEST_F(HashBuildAdaptiveTest, batchStoreNullableOuterKeys) {
  auto probe = makeFlatVector<int64_t>(257, [](auto row) { return row * 2; });
  auto build = makeFlatVector<int64_t>(
      129,
      [](auto row) { return row; },
      velox::test::VectorMaker::nullEvery(13));
  for (auto type : {core::JoinType::kFull, core::JoinType::kRight}) {
    auto plan = join(probe, {build, build}, type);
    auto batched = run(plan, false, 0, true);
    EXPECT_EQ(batched.rows, run(plan, false, 0, false).rows);
    EXPECT_EQ(batched.batchStoredRows, 258);
  }
}
} // namespace facebook::velox::exec::test
