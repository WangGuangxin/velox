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

#include "velox/dwio/common/ColumnVisitors.h"

#include <folly/Benchmark.h>
#include <folly/Random.h>
#include <folly/init/Init.h>

#include <vector>

using namespace facebook::velox;

namespace {

constexpr int32_t kNumValues = 64 << 10;

template <typename T>
std::vector<T> makeValues() {
  std::vector<T> values(kNumValues);
  for (auto& value : values) {
    value = static_cast<T>(folly::Random::rand32() & 0xFFFF);
  }
  return values;
}

// Loads every value as a dictionary index, one batch of int32 lanes at a time.
// Each benchmark iteration is one pass over kNumValues values.
template <typename T>
void loadAllIndices(unsigned iters, const std::vector<T>& values) {
  constexpr int32_t kNumLanes = xsimd::batch<int32_t>::size;
  for (unsigned iter = 0; iter < iters; ++iter) {
    // Hides the input from the optimizer so that each pass reloads it.
    const T* data = values.data();
    folly::makeUnpredictable(data);
    xsimd::batch<int32_t> sum(0);
    for (int32_t i = 0; i < kNumValues; i += kNumLanes) {
      sum += dwio::common::loadIndices(data + i);
    }
    folly::doNotOptimizeAway(sum);
  }
}

const std::vector<int16_t> int16Values = makeValues<int16_t>();
const std::vector<int32_t> int32Values = makeValues<int32_t>();
const std::vector<int64_t> int64Values = makeValues<int64_t>();
const std::vector<double> doubleValues = makeValues<double>();

} // namespace

BENCHMARK(loadIndicesInt16, iters) {
  loadAllIndices(iters, int16Values);
}

BENCHMARK(loadIndicesInt32, iters) {
  loadAllIndices(iters, int32Values);
}

BENCHMARK(loadIndicesInt64, iters) {
  loadAllIndices(iters, int64Values);
}

BENCHMARK(loadIndicesDouble, iters) {
  loadAllIndices(iters, doubleValues);
}

int main(int argc, char* argv[]) {
  folly::Init init{&argc, &argv};
  folly::runBenchmarks();
  return 0;
}
