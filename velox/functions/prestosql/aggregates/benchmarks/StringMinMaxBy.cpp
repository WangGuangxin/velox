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
#include <string>
#include <vector>

#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

static constexpr int32_t kNumVectors = 100;
static constexpr int32_t kRowsPerVector = 10'000;

namespace {

class StringMinMaxByBenchmark : public OperatorTestBase {
 public:
  StringMinMaxByBenchmark() {
    OperatorTestBase::SetUp();

    VectorFuzzer::Options opts;
    opts.vectorSize = kRowsPerVector;
    opts.nullRatio = 0;
    opts.stringVariableLength = false;
    VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);

    vectors_.reserve(kNumVectors);
    for (int32_t i = 0; i < kNumVectors; ++i) {
      std::vector<VectorPtr> children;

      // Low-cardinality grouping key (100 groups).
      children.emplace_back(
          makeFlatVector<int32_t>(
              kRowsPerVector, [](auto row) { return row % 100; }));

      // High-cardinality grouping key (10,000 groups).
      children.emplace_back(
          makeFlatVector<int32_t>(
              kRowsPerVector, [](auto row) { return row % 10'000; }));

      // Numeric column (BIGINT).
      children.emplace_back(fuzzer.fuzzFlat(BIGINT()));

      // Short (inline, 8 bytes) string columns.
      opts.stringLength = 8;
      fuzzer.setOptions(opts);
      children.emplace_back(fuzzer.fuzzFlat(VARCHAR()));
      children.emplace_back(fuzzer.fuzzFlat(VARCHAR()));

      // Long (non-inline, 48 bytes) string columns.
      opts.stringLength = 48;
      fuzzer.setOptions(opts);
      children.emplace_back(fuzzer.fuzzFlat(VARCHAR()));
      children.emplace_back(fuzzer.fuzzFlat(VARCHAR()));

      std::vector<std::string> names = {
          "k_low",
          "k_high",
          "num",
          "short_v",
          "short_c",
          "long_v",
          "long_c",
      };
      vectors_.emplace_back(makeRowVector(names, children));
    }
  }

  ~StringMinMaxByBenchmark() override {
    vectors_.clear();
    OperatorTestBase::TearDown();
  }

  void TestBody() override {}

  void run(
      const std::vector<std::string>& keys,
      const std::vector<std::string>& aggregates) {
    folly::BenchmarkSuspender suspender;

    auto plan = PlanBuilder()
                    .values(vectors_)
                    .partialAggregation(keys, aggregates)
                    .finalAggregation()
                    .planFragment();

    auto task = exec::Task::create(
        "t",
        std::move(plan),
        0,
        core::QueryCtx::create(executor_.get()),
        exec::Task::ExecutionMode::kSerial,
        exec::Consumer{});

    vector_size_t numResultRows = 0;
    suspender.dismiss();

    while (auto result = task->next()) {
      numResultRows += result->size();
    }

    folly::doNotOptimizeAway(numResultRows);
  }

 private:
  std::vector<RowVectorPtr> vectors_;
};

std::unique_ptr<StringMinMaxByBenchmark> benchmark;

void runSingle(
    uint32_t,
    const std::vector<std::string>& keys,
    const std::string& agg) {
  benchmark->run(keys, {agg});
}

// String comparison key, numeric value: min_by(bigint, varchar).
BENCHMARK_NAMED_PARAM(
    runSingle,
    k_low_min_by_num_short,
    {"k_low"},
    "min_by(num, short_c)");
BENCHMARK_NAMED_PARAM(
    runSingle,
    k_low_max_by_num_long,
    {"k_low"},
    "max_by(num, long_c)");
BENCHMARK_DRAW_LINE();

// Numeric comparison key, string value: min_by(varchar, bigint).
BENCHMARK_NAMED_PARAM(
    runSingle,
    k_low_min_by_short_num,
    {"k_low"},
    "min_by(short_v, num)");
BENCHMARK_NAMED_PARAM(
    runSingle,
    k_low_max_by_long_num,
    {"k_low"},
    "max_by(long_v, num)");
BENCHMARK_DRAW_LINE();

// Both string value and string comparison key: min_by(varchar, varchar).
BENCHMARK_NAMED_PARAM(
    runSingle,
    k_low_min_by_short_short,
    {"k_low"},
    "min_by(short_v, short_c)");
BENCHMARK_NAMED_PARAM(
    runSingle,
    k_low_max_by_long_long,
    {"k_low"},
    "max_by(long_v, long_c)");
BENCHMARK_NAMED_PARAM(
    runSingle,
    k_high_min_by_short_short,
    {"k_high"},
    "min_by(short_v, short_c)");
BENCHMARK_NAMED_PARAM(
    runSingle,
    k_high_max_by_long_long,
    {"k_high"},
    "max_by(long_v, long_c)");

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  OperatorTestBase::SetUpTestCase();
  benchmark = std::make_unique<StringMinMaxByBenchmark>();
  folly::runBenchmarks();
  benchmark.reset();
  OperatorTestBase::TearDownTestCase();
  return 0;
}
