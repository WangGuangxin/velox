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

#include "velox/core/Expressions.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/sparksql/registration/Register.h"

using namespace facebook::velox;

namespace {

constexpr vector_size_t kNumRows{1'000};

// JSON with a few short fields, such as an event record.
std::string makeSmallJson(vector_size_t row) {
  return fmt::format(
      R"({{"id": {}, "name": "user{}", "active": true, "score": {}.5}})",
      row,
      row,
      row % 100);
}

// JSON with the target nested four levels deep.
std::string makeNestedJson(vector_size_t row) {
  return fmt::format(
      R"({{"a": {{"b": {{"c": [0, {{"d": {}, "e": "x"}}]}}}}, "f": {}}})",
      row,
      row);
}

// JSON of about 1 KB with 40 fields.
std::string makeLargeJson(vector_size_t row) {
  std::string json{"{"};
  for (auto i = 0; i < 40; ++i) {
    if (i > 0) {
      json += ", ";
    }
    json += fmt::format(R"("field{}": "value_{}_{}")", i, row, i);
  }
  json += "}";
  return json;
}

class GetJsonObjectBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  GetJsonObjectBenchmark() : FunctionBenchmarkBase() {
    functions::sparksql::registerFunctions("");
  }

  // Evaluates get_json_object over 'kNumRows' rows built by 'makeJson'. The
  // path is a constant if 'constantPath' is true, otherwise a column.
  void run(
      const std::function<std::string(vector_size_t)>& makeJson,
      const std::string& path,
      bool constantPath) {
    folly::BenchmarkSuspender suspender;
    const auto data = vectorMaker_.rowVector({
        vectorMaker_.flatVector<std::string>(kNumRows, makeJson),
        vectorMaker_.flatVector<std::string>(
            kNumRows, [&](vector_size_t /*row*/) { return path; }),
    });
    core::TypedExprPtr pathExpr;
    if (constantPath) {
      pathExpr =
          std::make_shared<core::ConstantTypedExpr>(VARCHAR(), Variant(path));
    } else {
      pathExpr = std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c1");
    }
    const auto typedExpr = std::make_shared<const core::CallTypedExpr>(
        VARCHAR(),
        "get_json_object",
        std::make_shared<core::FieldAccessTypedExpr>(VARCHAR(), "c0"),
        pathExpr);
    exec::ExprSet exprSet({typedExpr}, &execCtx_);
    suspender.dismiss();

    for (auto i = 0; i < 100; ++i) {
      folly::doNotOptimizeAway(evaluate(exprSet, data));
    }
  }
};

BENCHMARK(smallJsonConstantPath) {
  GetJsonObjectBenchmark().run(makeSmallJson, "$.name", true);
}

BENCHMARK_RELATIVE(smallJsonColumnPath) {
  GetJsonObjectBenchmark().run(makeSmallJson, "$.name", false);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(nestedJsonConstantPath) {
  GetJsonObjectBenchmark().run(makeNestedJson, "$.a.b.c[1].d", true);
}

BENCHMARK_RELATIVE(nestedJsonColumnPath) {
  GetJsonObjectBenchmark().run(makeNestedJson, "$.a.b.c[1].d", false);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(largeJsonFirstFieldConstantPath) {
  GetJsonObjectBenchmark().run(makeLargeJson, "$.field0", true);
}

BENCHMARK_RELATIVE(largeJsonFirstFieldColumnPath) {
  GetJsonObjectBenchmark().run(makeLargeJson, "$.field0", false);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(largeJsonLastFieldConstantPath) {
  GetJsonObjectBenchmark().run(makeLargeJson, "$.field39", true);
}

BENCHMARK_RELATIVE(largeJsonLastFieldColumnPath) {
  GetJsonObjectBenchmark().run(makeLargeJson, "$.field39", false);
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  folly::runBenchmarks();
  return 0;
}
