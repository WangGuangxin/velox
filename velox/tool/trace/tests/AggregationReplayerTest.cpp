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

#include <boost/random/uniform_int_distribution.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>

#include "folly/experimental/EventCount.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/hyperloglog/SparseHll.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/exec/PartitionFunction.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/TraceUtil.h"
#include "velox/exec/tests/utils/ArbitratorTestUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/tool/trace/AggregationReplayer.h"
#include "velox/tool/trace/TableWriterReplayer.h"
#include "velox/tool/trace/TraceReplayRunner.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::common;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::common::testutil;
using namespace facebook::velox::common::hll;

namespace facebook::velox::tool::trace::test {
class AggregationReplayerTest : public HiveConnectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    HiveConnectorTestBase::SetUpTestCase();
    filesystems::registerLocalFileSystem();
    if (!isRegisteredVectorSerde()) {
      serializer::presto::PrestoVectorSerde::registerVectorSerde();
    }
    Type::registerSerDe();
    common::Filter::registerSerDe();
    connector::hive::HiveTableHandle::registerSerDe();
    connector::hive::LocationHandle::registerSerDe();
    connector::hive::HiveColumnHandle::registerSerDe();
    connector::hive::HiveInsertTableHandle::registerSerDe();
    connector::hive::HiveInsertFileNameGenerator::registerSerDe();
    core::PlanNode::registerSerDe();
    velox::exec::trace::registerDummySourceSerDe();
    core::ITypedExpr::registerSerDe();
    registerPartitionFunctionSerDe();
  }

  struct PlanWithName {
    const std::string name;
    const core::PlanNodePtr plan;

    PlanWithName(std::string _name, core::PlanNodePtr _plan)
        : name(std::move(_name)), plan(std::move(_plan)) {}
  };

  std::vector<TypePtr> generateKeyTypes(int32_t numKeys) {
    std::vector<TypePtr> types;
    types.reserve(numKeys);
    for (auto i = 0; i < numKeys; ++i) {
      types.push_back(vectorFuzzer_.randType(0 /*maxDepth*/));
    }
    return types;
  }

  std::vector<RowVectorPtr> generateInput(
      const std::vector<std::string>& keyNames,
      const std::vector<TypePtr>& keyTypes) {
    std::vector<std::string> names = keyNames;
    std::vector<TypePtr> types = keyTypes;

    // Add up to 3 payload columns.
    const auto numPayload = randInt(1, 3);
    for (auto i = 0; i < numPayload; ++i) {
      names.push_back(fmt::format("c{}", i + keyNames.size()));
      types.push_back(vectorFuzzer_.randType(2 /*maxDepth*/));
    }

    const auto inputType = ROW(std::move(names), std::move(types));
    std::vector<RowVectorPtr> input;
    for (auto i = 0; i < 10; ++i) {
      input.push_back(vectorFuzzer_.fuzzInputRow(inputType));
    }
    return input;
  }

  std::vector<std::string> makeNames(const std::string& prefix, size_t n) {
    std::vector<std::string> names;
    names.reserve(n);
    for (auto i = 0; i < n; ++i) {
      names.push_back(fmt::format("{}{}", prefix, i));
    }
    return names;
  }

  std::vector<PlanWithName> aggregatePlans(
      const RowTypePtr& rowType,
      const std::string& prefix = "") {
    const std::vector<std::string> aggregates{
        fmt::format("{}count(1)", prefix),
        fmt::format("{}min(c1)", prefix),
        fmt::format("{}count(c2),", prefix)};
    std::vector<PlanWithName> plans;
    // Single aggregation plan.
    plans.emplace_back(
        "Single",
        PlanBuilder()
            .tableScan(rowType)
            .singleAggregation(groupingKeys_, aggregates, {})
            .capturePlanNodeId(traceNodeId_)
            .planNode());
    // Partial -> final aggregation plan.
    plans.emplace_back(
        "Partial-Final",
        PlanBuilder()
            .tableScan(rowType)
            .partialAggregation(groupingKeys_, aggregates, {})
            .capturePlanNodeId(traceNodeId_)
            .finalAggregation()
            .planNode());
    // Partial -> intermediate -> final aggregation plan.
    plans.emplace_back(
        "Partial-Intermediate-Final",
        PlanBuilder()
            .tableScan(rowType)
            .partialAggregation(groupingKeys_, aggregates, {})
            .capturePlanNodeId(traceNodeId_)
            .intermediateAggregation()
            .finalAggregation()
            .planNode());
    return plans;
  }

  std::vector<PlanWithName> streamingAggregatePlans(
      const RowTypePtr& rowType,
      const std::string& prefix = "") {
    const std::vector<std::string> aggregates{
        fmt::format("{}count(1)", prefix),
        fmt::format("{}min(c1)", prefix),
        fmt::format("{}count(c2),", prefix)};
    std::vector<PlanWithName> plans;
    // Single aggregation plan.
    plans.emplace_back(
        "Single",
        PlanBuilder()
            .tableScan(rowType)
            .streamingAggregation(
                groupingKeys_,
                aggregates,
                {},
                core::AggregationNode::Step::kSingle,
                false)
            .capturePlanNodeId(traceNodeId_)
            .planNode());
    // Partial -> final aggregation plan.
    plans.emplace_back(
        "Partial-Final",
        PlanBuilder()
            .tableScan(rowType)
            .streamingAggregation(
                groupingKeys_,
                aggregates,
                {},
                core::AggregationNode::Step::kPartial,
                false)
            .capturePlanNodeId(traceNodeId_)
            .finalAggregation()
            .planNode());
    // Partial -> intermediate -> final aggregation plan.
    plans.emplace_back(
        "Partial-Intermediate-Final",
        PlanBuilder()
            .tableScan(rowType)
            .streamingAggregation(
                groupingKeys_,
                aggregates,
                {},
                core::AggregationNode::Step::kPartial,
                false)
            .capturePlanNodeId(traceNodeId_)
            .intermediateAggregation()
            .finalAggregation()
            .planNode());
    return plans;
  }

  int32_t randInt(int32_t min, int32_t max) {
    return boost::random::uniform_int_distribution<int32_t>(min, max)(rng_);
  }

  static VectorFuzzer::Options getFuzzerOptions() {
    VectorFuzzer::Options opts;
    opts.vectorSize = 1000;
    opts.stringVariableLength = true;
    opts.stringLength = 100;
    opts.nullRatio = 0.2;
    return opts;
  }

  core::PlanNodeId traceNodeId_;
  VectorFuzzer vectorFuzzer_{getFuzzerOptions(), pool()};
  std::mt19937 rng_;
  const std::vector<TypePtr> keyTypes_{generateKeyTypes(2)};
  const std::vector<std::string> groupingKeys_{
      makeNames("c", keyTypes_.size())};
};

TEST_F(AggregationReplayerTest, hashAggregationTest) {
  for (const auto& prefix : std::vector<std::string>{"", "test."}) {
    const auto data = generateInput(groupingKeys_, keyTypes_);
    const auto planWithNames =
        aggregatePlans(asRowType(data[0]->type()), prefix);
    const auto sourceFilePath = TempFilePath::create();
    writeToFile(sourceFilePath->getPath(), data);

    if (!prefix.empty()) {
      functions::prestosql::registerAllScalarFunctions(prefix);
      aggregate::prestosql::registerAllAggregateFunctions(prefix);
      FLAGS_function_prefix = prefix;
    }

    for (const auto& planWithName : planWithNames) {
      SCOPED_TRACE(planWithName.name);
      const auto& plan = planWithName.plan;
      const auto testDir = TempDirectoryPath::create();
      const auto traceRoot =
          fmt::format("{}/{}", testDir->getPath(), "traceRoot");
      std::shared_ptr<Task> task;
      auto results =
          AssertQueryBuilder(plan)
              .config(core::QueryConfig::kQueryTraceEnabled, true)
              .config(core::QueryConfig::kQueryTraceDir, traceRoot)
              .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
              .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
              .config(core::QueryConfig::kQueryTraceNodeId, traceNodeId_)
              .split(makeHiveConnectorSplit(sourceFilePath->getPath()))
              .copyResults(pool(), task);

      const auto replayingResult = AggregationReplayer(
                                       traceRoot,
                                       task->queryCtx()->queryId(),
                                       task->taskId(),
                                       traceNodeId_,
                                       "Aggregation",
                                       "",
                                       0,
                                       executor_.get())
                                       .run();
      assertEqualResults({results}, {replayingResult});

      FLAGS_root_dir = traceRoot;
      FLAGS_query_id = task->queryCtx()->queryId();
      FLAGS_task_id = task->taskId();
      FLAGS_node_id = traceNodeId_;
      FLAGS_summary = true;
      {
        TraceReplayRunner runner;
        runner.init();
        runner.run();
      }

      FLAGS_task_id = task->taskId();
      FLAGS_driver_ids = "";
      FLAGS_summary = false;
      {
        TraceReplayRunner runner;
        runner.init();
        runner.run();
      }
    }
  }
}

TEST_F(AggregationReplayerTest, streamingAggregateTest) {
  for (const auto& prefix : std::vector<std::string>{"", "test."}) {
    const auto data = generateInput(groupingKeys_, keyTypes_);
    const auto planWithNames =
        streamingAggregatePlans(asRowType(data[0]->type()), prefix);
    const auto sourceFilePath = TempFilePath::create();
    writeToFile(sourceFilePath->getPath(), data);

    if (!prefix.empty()) {
      functions::prestosql::registerAllScalarFunctions(prefix);
      aggregate::prestosql::registerAllAggregateFunctions(prefix);
      FLAGS_function_prefix = prefix;
    }

    for (const auto& planWithName : planWithNames) {
      SCOPED_TRACE(planWithName.name);
      const auto& plan = planWithName.plan;
      const auto testDir = TempDirectoryPath::create();
      const auto traceRoot =
          fmt::format("{}/{}", testDir->getPath(), "traceRoot");
      std::shared_ptr<Task> task;
      auto results =
          AssertQueryBuilder(plan)
              .config(core::QueryConfig::kQueryTraceEnabled, true)
              .config(core::QueryConfig::kQueryTraceDir, traceRoot)
              .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
              .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
              .config(core::QueryConfig::kQueryTraceNodeId, traceNodeId_)
              .split(makeHiveConnectorSplit(sourceFilePath->getPath()))
              .copyResults(pool(), task);

      const auto replayingResult = AggregationReplayer(
                                       traceRoot,
                                       task->queryCtx()->queryId(),
                                       task->taskId(),
                                       traceNodeId_,
                                       "Aggregation",
                                       "",
                                       0,
                                       executor_.get())
                                       .run();
      assertEqualResults({results}, {replayingResult});

      FLAGS_root_dir = traceRoot;
      FLAGS_query_id = task->queryCtx()->queryId();
      FLAGS_task_id = task->taskId();
      FLAGS_node_id = traceNodeId_;
      FLAGS_summary = true;
      {
        TraceReplayRunner runner;
        runner.init();
        runner.run();
      }

      FLAGS_task_id = task->taskId();
      FLAGS_driver_ids = "";
      FLAGS_summary = false;
      {
        TraceReplayRunner runner;
        runner.init();
        runner.run();
      }
    }
  }
}
} // namespace facebook::velox::tool::trace::test
