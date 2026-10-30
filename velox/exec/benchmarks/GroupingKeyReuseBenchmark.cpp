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

#include "velox/exec/GroupingKeyContributions.h"
#include "velox/exec/HashTable.h"
#include "velox/exec/VectorHasher.h"
#include "velox/vector/FlatVector.h"

DEFINE_int32(batch_size, 10'000, "Rows per Expand input batch");
DEFINE_int32(num_batches, 32, "Expand input batches per iteration");

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

// Grouping keys are three columns of one of these shapes.
enum class KeyShape {
  // Dictionary-encoded strings over a small base. VectorHasher hashes the
  // base once per distinct value rather than once per row, so this is where
  // a per-row cache costs more than it saves.
  kDictionaryVarchar,
  // Flat strings, hashed once per row.
  kFlatVarchar,
  // Few distinct values, so the probe runs on value IDs.
  kValueIdBigint,
  // More distinct values than VectorHasher keeps, so the probe hashes.
  kHashedBigint,
};

int32_t cardinality(KeyShape shape) {
  switch (shape) {
    case KeyShape::kDictionaryVarchar:
    case KeyShape::kFlatVarchar:
      return 100;
    case KeyShape::kValueIdBigint:
      return 1'000;
    case KeyShape::kHashedBigint:
      return 150'000;
  }
  VELOX_UNREACHABLE();
}

// Measures the grouping key work an aggregation does under an Expand.
//
// Expand emits one batch per grouping set and hands each the same key vector,
// replacing only the keys that set drops with a null constant. Here that is
// a CUBE over three keys: eight grouping sets, each key real in four of them.
// Only the probing is timed; the batches and the table are built outside it.
class GroupingKeyReuseBenchmark {
 public:
  static constexpr int32_t kNumKeys = 3;
  static constexpr int32_t kNumGroupingSets = 1 << kNumKeys;

  explicit GroupingKeyReuseBenchmark(KeyShape shape)
      : shape_(shape), pool_(memory::memoryManager()->addLeafPool()) {}

  // Returns the number of probed rows.
  uint64_t run(bool reuse, bool probeGroups) {
    folly::BenchmarkSuspender suspender;
    auto batches = makeBatches();
    auto table = makeTable();
    // One entry per hasher, grouping set id included.
    GroupingKeyContributions contributions(
        kNumKeys + 1, pool_.get(), GroupingKeyContributions::kDefaultMaxBytes);
    if (reuse) {
      table->setKeyContributions(&contributions);
    }
    HashLookup lookup(table->hashers(), pool_.get());
    SelectivityVector rows(FLAGS_batch_size);
    suspender.dismiss();

    for (const auto& batch : batches) {
      rows.resize(batch->size());
      rows.setAll();
      table->prepareForGroupProbe(
          lookup, batch, rows, BaseHashTable::kNoSpillInputStartPartitionBit);
      if (probeGroups) {
        table->groupProbe(
            lookup, BaseHashTable::kNoSpillInputStartPartitionBit);
      }
    }

    suspender.rehire();
    return batches.size() * FLAGS_batch_size;
  }

  // Prints what the probe actually did, so the timings below can be read
  // against the path each key shape takes.
  void describe(std::string_view name) {
    auto batches = makeBatches();
    auto table = makeTable();
    GroupingKeyContributions contributions(
        kNumKeys + 1, pool_.get(), GroupingKeyContributions::kDefaultMaxBytes);
    table->setKeyContributions(&contributions);
    HashLookup lookup(table->hashers(), pool_.get());
    SelectivityVector rows(FLAGS_batch_size);
    for (const auto& batch : batches) {
      rows.resize(batch->size());
      rows.setAll();
      table->prepareForGroupProbe(
          lookup, batch, rows, BaseHashTable::kNoSpillInputStartPartitionBit);
      table->groupProbe(lookup, BaseHashTable::kNoSpillInputStartPartitionBit);
    }
    const auto total =
        contributions.numReusedRows() + contributions.numComputedRows();
    std::cout << fmt::format(
                     "{:<20} mode={:<14} groups={:<8} cachedKeyRows={} "
                     "reused={}",
                     name,
                     BaseHashTable::modeString(table->hashMode()),
                     table->numDistinct(),
                     total,
                     contributions.numReusedRows())
              << std::endl;
  }

 private:
  TypePtr keyType() const {
    switch (shape_) {
      case KeyShape::kDictionaryVarchar:
      case KeyShape::kFlatVarchar:
        return VARCHAR();
      case KeyShape::kValueIdBigint:
      case KeyShape::kHashedBigint:
        return BIGINT();
    }
    VELOX_UNREACHABLE();
  }

  std::unique_ptr<BaseHashTable> makeTable() {
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    for (auto channel = 0; channel < kNumKeys + 1; ++channel) {
      // The last key is the grouping set id Expand projects as a constant.
      hashers.push_back(std::make_unique<VectorHasher>(
          channel < kNumKeys ? keyType() : BIGINT(), channel));
    }
    return HashTable<false>::createForAggregation(
        std::move(hashers), {}, pool_.get());
  }

  // Builds one key column. Keys are derived from the same row index so that
  // the number of distinct key combinations stays at the cardinality of one
  // column, keeping the table small enough that hashing, not table growth,
  // dominates.
  VectorPtr makeKey(int32_t key, int64_t batch) {
    const auto numRows = FLAGS_batch_size;
    const auto distinct = cardinality(shape_);
    const auto valueAt = [&](vector_size_t row) {
      return (row + key * 7 + batch * 13) % distinct;
    };
    if (shape_ == KeyShape::kValueIdBigint ||
        shape_ == KeyShape::kHashedBigint) {
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

  // Returns 'num_batches' input batches fanned out over the grouping sets,
  // sharing one key vector per input batch exactly as Expand does.
  std::vector<RowVectorPtr> makeBatches() {
    std::vector<TypePtr> types(kNumKeys, keyType());
    types.push_back(BIGINT());
    const auto rowType = ROW(std::move(types));
    std::vector<RowVectorPtr> batches;
    for (int64_t batch = 0; batch < FLAGS_num_batches; ++batch) {
      std::vector<VectorPtr> keys;
      for (auto key = 0; key < kNumKeys; ++key) {
        keys.push_back(makeKey(key, batch));
      }
      for (auto set = 0; set < kNumGroupingSets; ++set) {
        std::vector<VectorPtr> children;
        for (auto key = 0; key < kNumKeys; ++key) {
          // Grouping set 'set' keeps key 'key' when its bit is set.
          children.push_back(
              (set & (1 << key)) != 0
                  ? keys[key]
                  : BaseVector::createNullConstant(
                        keyType(), FLAGS_batch_size, pool_.get()));
        }
        children.push_back(BaseVector::createConstant(
            BIGINT(), variant(static_cast<int64_t>(set)), FLAGS_batch_size,
            pool_.get()));
        batches.push_back(std::make_shared<RowVector>(
            pool_.get(),
            rowType,
            nullptr,
            FLAGS_batch_size,
            std::move(children)));
      }
    }
    return batches;
  }

  const KeyShape shape_;
  const std::shared_ptr<memory::MemoryPool> pool_;
};

void describe(std::string_view name, KeyShape shape) {
  GroupingKeyReuseBenchmark(shape).describe(name);
}

void addBenchmarks(std::string_view name, KeyShape shape) {
  // 'keys' times only the key work the cache targets. 'probe' adds the group
  // probe, which the cache does not change, for a realistic ratio.
  for (const bool probeGroups : {false, true}) {
    const auto suffix =
        fmt::format("{}_{}", probeGroups ? "probe" : "keys", name);
    folly::addBenchmark(__FILE__, "reuseOff_" + suffix, [shape, probeGroups]() {
      return GroupingKeyReuseBenchmark(shape).run(false, probeGroups);
    });
    folly::addBenchmark(
        __FILE__, "%reuseOn_" + suffix, [shape, probeGroups]() {
          return GroupingKeyReuseBenchmark(shape).run(true, probeGroups);
        });
  }
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  describe("dictionaryVarchar", KeyShape::kDictionaryVarchar);
  describe("flatVarchar", KeyShape::kFlatVarchar);
  describe("valueIdBigint", KeyShape::kValueIdBigint);
  describe("hashedBigint", KeyShape::kHashedBigint);
  addBenchmarks("dictionaryVarchar", KeyShape::kDictionaryVarchar);
  addBenchmarks("flatVarchar", KeyShape::kFlatVarchar);
  addBenchmarks("valueIdBigint", KeyShape::kValueIdBigint);
  addBenchmarks("hashedBigint", KeyShape::kHashedBigint);
  folly::runBenchmarks();
  return 0;
}
