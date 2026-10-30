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

#include <iostream>
#include <map>

#include "velox/core/PlanFragment.h"
#include "velox/exec/Task.h"
#include "velox/functions/prestosql/aggregates/CountAggregate.h"
#include "velox/functions/prestosql/aggregates/SumAggregate.h"
#include "velox/vector/FlatVector.h"

DEFINE_int32(batch_size, 10'000, "Rows per Expand input batch");
DEFINE_int32(num_batches, 32, "Expand input batches per iteration");
DEFINE_int32(
    key_cardinality,
    100,
    "Distinct values per string key, which sets how many groups the "
    "aggregation builds");

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

// Key layouts, matching GroupingKeyReuseBenchmark so the whole-query figures
// can be read against the key-work figures it reports.
enum class KeyShape {
  // Flat strings, hashed once per row. The shape the cache targets.
  kFlatVarchar,
  // Dictionary-encoded strings over a small base, which VectorHasher already
  // reduces to one computation per distinct base value.
  kDictionaryVarchar,
  // Few distinct values, so the probe runs on value IDs.
  kValueIdBigint,
};

int32_t cardinality(KeyShape shape) {
  switch (shape) {
    case KeyShape::kFlatVarchar:
    case KeyShape::kDictionaryVarchar:
      return FLAGS_key_cardinality;
    case KeyShape::kValueIdBigint:
      return 1'000;
  }
  VELOX_UNREACHABLE();
}

TypePtr keyType(KeyShape shape) {
  if (shape == KeyShape::kValueIdBigint) {
    return BIGINT();
  }
  return VARCHAR();
}

// Runs a whole CUBE query, rather than the group probe alone, so that the
// key work the cache saves is measured against everything else a real
// aggregation does: the Expand operator, the accumulators, and output.
class GroupingSetsQueryBenchmark {
 public:
  static constexpr int32_t kNumKeys = 3;
  static constexpr int32_t kNumGroupingSets = 1 << kNumKeys;

  explicit GroupingSetsQueryBenchmark(KeyShape shape)
      : shape_(shape), pool_(memory::memoryManager()->addLeafPool()) {}

  // Returns the number of input rows the Expand fanned out.
  uint64_t run(bool reuse) {
    folly::BenchmarkSuspender suspender;
    auto plan = makePlan(makeBatches());
    suspender.dismiss();

    auto task = makeTask(plan, reuse);
    uint64_t numOutputRows = 0;
    while (auto output = task->next()) {
      numOutputRows += output->size();
    }

    suspender.rehire();
    VELOX_CHECK(task->isFinished());
    VELOX_CHECK_GT(numOutputRows, 0);
    return static_cast<uint64_t>(FLAGS_num_batches) * FLAGS_batch_size *
        kNumGroupingSets;
  }

  // Prints the groups produced and the rows the cache served, so a timing can
  // be read against what the query actually did.
  void describe(std::string_view name) {
    auto plan = makePlan(makeBatches());
    uint64_t numOutputRows = 0;
    std::map<std::string, uint64_t> stats;
    for (const bool reuse : {false, true}) {
      auto task = makeTask(plan, reuse);
      uint64_t rows = 0;
      while (auto output = task->next()) {
        rows += output->size();
      }
      if (!reuse) {
        numOutputRows = rows;
      } else {
        VELOX_CHECK_EQ(rows, numOutputRows, "Reuse changed the result size");
        for (const auto& pipeline : task->taskStats().pipelineStats) {
          for (const auto& op : pipeline.operatorStats) {
            for (const auto* key :
                 {"groupingKeyRowsReused", "groupingKeyRowsComputed"}) {
              if (auto it = op.runtimeStats.find(key);
                  it != op.runtimeStats.end()) {
                stats[key] += it->second.sum;
              }
            }
          }
        }
      }
    }
    std::cout << fmt::format(
                     "{:<20} outputRows={:<9} keyRowsReused={:<9} "
                     "keyRowsComputed={}",
                     name,
                     numOutputRows,
                     stats["groupingKeyRowsReused"],
                     stats["groupingKeyRowsComputed"])
              << std::endl;
  }

 private:
  std::shared_ptr<Task> makeTask(const core::PlanNodePtr& plan, bool reuse) {
    std::unordered_map<std::string, std::string> settings{
        {core::QueryConfig::kGroupingSetsKeyReuseEnabled,
         reuse ? "true" : "false"},
    };
    auto context =
        core::QueryCtx::create(nullptr, core::QueryConfig(std::move(settings)));
    return Task::create(
        "grouping-sets-query-benchmark",
        core::PlanFragment(plan),
        0,
        context,
        Task::ExecutionMode::kSerial);
  }

  core::TypedExprPtr field(const TypePtr& type, const std::string& name) const {
    return std::make_shared<core::FieldAccessTypedExpr>(type, name);
  }

  // Builds Values -> Expand -> Aggregation, the plan a CUBE over 'kNumKeys'
  // keys compiles to.
  core::PlanNodePtr makePlan(const std::vector<RowVectorPtr>& input) const {
    const auto type = keyType(shape_);
    core::PlanNodePtr source =
        std::make_shared<core::ValuesNode>("values", input);

    std::vector<std::vector<core::TypedExprPtr>> projections;
    for (auto set = 0; set < kNumGroupingSets; ++set) {
      std::vector<core::TypedExprPtr> projection;
      for (auto key = 0; key < kNumKeys; ++key) {
        // Grouping set 'set' keeps key 'key' when its bit is set.
        projection.push_back(
            (set & (1 << key)) != 0
                ? field(type, fmt::format("k{}", key))
                : std::make_shared<core::ConstantTypedExpr>(
                      type, variant::null(type->kind())));
      }
      projection.push_back(field(BIGINT(), "v"));
      projection.push_back(std::make_shared<core::ConstantTypedExpr>(
          BIGINT(), variant(static_cast<int64_t>(set))));
      projections.push_back(std::move(projection));
    }
    std::vector<std::string> names{"k0", "k1", "k2", "v", "gid"};
    source = std::make_shared<core::ExpandNode>(
        "expand", std::move(projections), std::move(names), source);

    std::vector<core::FieldAccessTypedExprPtr> groupingKeys;
    for (auto key = 0; key < kNumKeys; ++key) {
      groupingKeys.push_back(std::make_shared<core::FieldAccessTypedExpr>(
          type, fmt::format("k{}", key)));
    }
    groupingKeys.push_back(
        std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "gid"));

    std::vector<core::AggregationNode::Aggregate> aggregates{
        {std::make_shared<core::CallTypedExpr>(
             BIGINT(),
             std::vector<core::TypedExprPtr>{},
             "grouping_sets_benchmark_count"),
         {}},
        {std::make_shared<core::CallTypedExpr>(
             BIGINT(),
             std::vector<core::TypedExprPtr>{field(BIGINT(), "v")},
             "grouping_sets_benchmark_sum"),
         {BIGINT()}},
    };
    return std::make_shared<core::AggregationNode>(
        "aggregate",
        core::AggregationNode::Step::kSingle,
        std::move(groupingKeys),
        std::vector<core::FieldAccessTypedExprPtr>{},
        std::vector<std::string>{"n", "total"},
        std::move(aggregates),
        false,
        false,
        source);
  }

  // Keys are derived from the same row index so that the number of distinct
  // key combinations stays at the cardinality of one column, keeping the
  // table small enough that key work, not table growth, dominates.
  VectorPtr makeKey(int32_t key, int64_t batch) const {
    const auto numRows = FLAGS_batch_size;
    const auto distinct = cardinality(shape_);
    // Spread over batches as well as rows so that 'key_cardinality' above a
    // batch's worth of rows still produces that many distinct values.
    const auto valueAt = [&](vector_size_t row) {
      const int64_t value = row + batch * FLAGS_batch_size + key * 7;
      return value % distinct;
    };
    if (shape_ == KeyShape::kValueIdBigint) {
      auto values = BaseVector::create<FlatVector<int64_t>>(
          BIGINT(), numRows, pool_.get());
      for (auto row = 0; row < numRows; ++row) {
        values->set(row, valueAt(row));
      }
      return values;
    }
    if (shape_ == KeyShape::kFlatVarchar) {
      auto values = BaseVector::create<FlatVector<StringView>>(
          VARCHAR(), numRows, pool_.get());
      for (auto row = 0; row < numRows; ++row) {
        const auto name =
            fmt::format("grouping-key-value-{:012d}", valueAt(row));
        values->set(row, StringView(name));
      }
      return values;
    }
    auto base = BaseVector::create<FlatVector<StringView>>(
        VARCHAR(), distinct, pool_.get());
    for (auto value = 0; value < distinct; ++value) {
      const auto name = fmt::format("grouping-key-value-{:012d}", value);
      base->set(value, StringView(name));
    }
    auto indices = AlignedBuffer::allocate<vector_size_t>(numRows, pool_.get());
    auto* rawIndices = indices->asMutable<vector_size_t>();
    for (auto row = 0; row < numRows; ++row) {
      rawIndices[row] = valueAt(row);
    }
    return BaseVector::wrapInDictionary(nullptr, indices, numRows, base);
  }

  // Returns the input to the Expand, which fans each batch out itself.
  std::vector<RowVectorPtr> makeBatches() const {
    std::vector<TypePtr> types(kNumKeys, keyType(shape_));
    types.push_back(BIGINT());
    const auto rowType = ROW({"k0", "k1", "k2", "v"}, std::move(types));
    std::vector<RowVectorPtr> batches;
    for (int64_t batch = 0; batch < FLAGS_num_batches; ++batch) {
      std::vector<VectorPtr> children;
      for (auto key = 0; key < kNumKeys; ++key) {
        children.push_back(makeKey(key, batch));
      }
      auto values = BaseVector::create<FlatVector<int64_t>>(
          BIGINT(), FLAGS_batch_size, pool_.get());
      for (auto row = 0; row < FLAGS_batch_size; ++row) {
        values->set(row, row);
      }
      children.push_back(values);
      batches.push_back(
          std::make_shared<RowVector>(
              pool_.get(),
              rowType,
              nullptr,
              FLAGS_batch_size,
              std::move(children)));
    }
    return batches;
  }

  const KeyShape shape_;
  const std::shared_ptr<memory::MemoryPool> pool_;
};

void addBenchmarks(std::string_view name, KeyShape shape) {
  folly::addBenchmark(
      __FILE__, fmt::format("reuseOff_{}", name), [shape]() {
        return GroupingSetsQueryBenchmark(shape).run(false);
      });
  folly::addBenchmark(
      __FILE__, fmt::format("%reuseOn_{}", name), [shape]() {
        return GroupingSetsQueryBenchmark(shape).run(true);
      });
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  aggregate::prestosql::registerCountAggregate(
      {"grouping_sets_benchmark_count"}, false, true);
  aggregate::prestosql::registerSumAggregate(
      {"grouping_sets_benchmark_sum"}, false, true);

  GroupingSetsQueryBenchmark(KeyShape::kFlatVarchar).describe("flatVarchar");
  GroupingSetsQueryBenchmark(KeyShape::kDictionaryVarchar)
      .describe("dictionaryVarchar");
  GroupingSetsQueryBenchmark(KeyShape::kValueIdBigint)
      .describe("valueIdBigint");

  addBenchmarks("flatVarchar", KeyShape::kFlatVarchar);
  addBenchmarks("dictionaryVarchar", KeyShape::kDictionaryVarchar);
  addBenchmarks("valueIdBigint", KeyShape::kValueIdBigint);
  folly::runBenchmarks();
  return 0;
}
