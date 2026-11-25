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
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using facebook::velox::RowVector;
using facebook::velox::RowVectorPtr;
using facebook::velox::vector_size_t;
using facebook::velox::core::PlanNodePtr;
using facebook::velox::exec::test::AssertQueryBuilder;
using facebook::velox::exec::test::PlanBuilder;
using facebook::velox::memory::MemoryManager;
using facebook::velox::test::VectorTestBase;

namespace {

// Measures Unnest of an array column with one replicated column. Small arrays
// stress the per-input-row work; large arrays stress the per-element work.
class UnnestBenchmark : public VectorTestBase {
 public:
  // Registers a query with input construction outside the timed execution.
  // Every eighth input row holds an empty array so that the marker column, when
  // requested, has rows to mark.
  void addBenchmark(
      vector_size_t numElementsPerRow,
      bool withOrdinality,
      bool withMarker) {
    constexpr vector_size_t kNumElements = 1'000'000;
    constexpr vector_size_t kBatchSize = 4'096;
    const vector_size_t numRows = kNumElements / numElementsPerRow;
    auto data = makeRowVector({
        makeFlatVector<int64_t>(numRows, [](auto row) { return row; }),
        makeArrayVector<int64_t>(
            numRows,
            [numElementsPerRow](auto row) {
              return row % 8 == 7 ? 0 : numElementsPerRow;
            },
            [](auto index) { return index; }),
    });
    std::vector<RowVectorPtr> batches;
    for (vector_size_t offset = 0; offset < numRows; offset += kBatchSize) {
      batches.push_back(
          std::dynamic_pointer_cast<RowVector>(data->slice(
              offset, std::min<vector_size_t>(kBatchSize, numRows - offset))));
    }
    const auto planIndex = plans_.size();
    plans_.push_back(
        PlanBuilder()
            .values(batches)
            .unnest(
                {"c0"},
                {"c1"},
                withOrdinality ? std::optional<std::string>("ordinality")
                               : std::nullopt,
                withMarker ? std::optional<std::string>("marker")
                           : std::nullopt)
            .planNode());
    folly::addBenchmark(
        __FILE__,
        fmt::format(
            "elements{}{}{}",
            numElementsPerRow,
            withOrdinality ? "_ordinality" : "",
            withMarker ? "_marker" : ""),
        [this, planIndex]() {
          const auto count = AssertQueryBuilder(plans_[planIndex])
                                 .maxDrivers(1)
                                 .countResults();
          folly::doNotOptimizeAway(count);
          return 1;
        });
  }

 private:
  // Releases input vectors before the base class destroys their memory pool.
  std::vector<PlanNodePtr> plans_;
};

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  MemoryManager::initialize(MemoryManager::Options{});
  UnnestBenchmark benchmark;
  for (vector_size_t numElementsPerRow : {1, 4, 64}) {
    benchmark.addBenchmark(numElementsPerRow, false, false);
    benchmark.addBenchmark(numElementsPerRow, true, false);
    benchmark.addBenchmark(numElementsPerRow, true, true);
  }
  folly::runBenchmarks();
  return 0;
}
