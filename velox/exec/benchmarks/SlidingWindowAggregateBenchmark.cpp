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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/common/memory/Memory.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/Cursor.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

// Measures aggregate window functions over sliding ROWS frames
// ('k PRECEDING AND CURRENT ROW'), where both frame bounds advance with the
// current row. Sweeps the frame width to find where the two-stack sliding
// window path starts to beat recomputing each frame from scratch.

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {

constexpr int32_t kRowsPerVector{10'000};
constexpr int32_t kNumVectors{10};
constexpr int32_t kRowsPerPartition{50'000};

class SlidingWindowAggregateBenchmark : public VectorTestBase {
 public:
  void addBenchmarks() {
    data_ = makeData();
    for (const auto* aggregate : {"sum", "avg", "min"}) {
      for (const int32_t numPreceding :
           {1, 4, 8, 16, 32, 64, 128, 256, 512, 1'024, 2'048, 4'096}) {
        addBenchmark(aggregate, numPreceding);
      }
    }
  }

 private:
  struct TestCase {
    core::PlanNodePtr plan;
  };

  std::vector<RowVectorPtr> makeData() {
    std::vector<RowVectorPtr> data;
    data.reserve(kNumVectors);
    for (auto vectorIndex = 0; vectorIndex < kNumVectors; ++vectorIndex) {
      const auto firstRow = vectorIndex * kRowsPerVector;
      data.push_back(makeRowVector(
          {"p", "s", "v"},
          {
              makeFlatVector<int32_t>(
                  kRowsPerVector,
                  [firstRow](auto row) {
                    return (firstRow + row) / kRowsPerPartition;
                  }),
              makeFlatVector<int32_t>(
                  kRowsPerVector,
                  [firstRow](auto row) {
                    return (firstRow + row) % kRowsPerPartition;
                  }),
              makeFlatVector<int64_t>(
                  kRowsPerVector, [](auto row) { return row % 97; }),
          }));
    }
    return data;
  }

  void addBenchmark(const std::string& aggregate, int32_t numPreceding) {
    auto testCase = std::make_unique<TestCase>();
    testCase->plan =
        exec::test::PlanBuilder()
            .values(data_)
            .streamingWindow({fmt::format(
                "{}(v) over (partition by p order by s rows between {} "
                "preceding and current row)",
                aggregate,
                numPreceding)})
            .planNode();

    const auto* test = testCase.get();
    folly::addBenchmark(
        __FILE__,
        fmt::format("{}_preceding{}", aggregate, numPreceding),
        [test](unsigned iterations) {
          uint64_t numResultRows{0};
          for (auto i = 0; i < iterations; ++i) {
            std::unique_ptr<TaskCursor> cursor;
            BENCHMARK_SUSPEND {
              CursorParameters params;
              params.planNode = test->plan;
              params.serialExecution = true;
              cursor = TaskCursor::create(params);
            }
            numResultRows += cursor->countRows();
          }
          folly::doNotOptimizeAway(numResultRows);
          return iterations;
        });

    cases_.push_back(std::move(testCase));
  }

  std::vector<RowVectorPtr> data_;
  std::vector<std::unique_ptr<TestCase>> cases_;
};

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  aggregate::prestosql::registerAllAggregateFunctions();
  window::prestosql::registerAllWindowFunctions();

  SlidingWindowAggregateBenchmark benchmark;
  benchmark.addBenchmarks();
  folly::runBenchmarks();
  return 0;
}
