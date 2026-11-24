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

// Measures PrefixSort on string sort keys whose longest value is longer than
// the default 16-byte string prefix but at most 64 bytes, the range where such
// a key is encoded in full. The data sets cover both sides: keys that tie on
// their first 16 bytes, where the full encoding avoids comparing through the
// row container, and keys that are already decided by their first 16 bytes,
// where the full encoding only makes each prefix entry wider.

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <random>

#include "velox/exec/PrefixSort.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

constexpr int32_t kNumRows{1'000'000};

// The default query config values.
const common::PrefixSortConfig kSortConfig{128, 128, 16};

std::string randomString(std::mt19937& rng, int32_t length) {
  static constexpr std::string_view kAlphabet{
      "0123456789abcdefghijklmnopqrstuvwxyz"};
  std::uniform_int_distribution<int32_t> distribution(0, kAlphabet.size() - 1);
  std::string value(length, ' ');
  for (auto& character : value) {
    character = kAlphabet[distribution(rng)];
  }
  return value;
}

// Returns a random version 4 style UUID string, 36 bytes.
std::string randomUuid(std::mt19937& rng) {
  static constexpr std::string_view kHexDigits{"0123456789abcdef"};
  std::uniform_int_distribution<int32_t> distribution(0, 15);
  std::string value(36, '-');
  for (auto i = 0; i < value.size(); ++i) {
    if (i != 8 && i != 13 && i != 18 && i != 23) {
      value[i] = kHexDigits[distribution(rng)];
    }
  }
  return value;
}

VectorPtr makeVarcharVector(
    const std::vector<std::string>& values,
    memory::MemoryPool* pool) {
  auto vector = BaseVector::create<FlatVector<StringView>>(
      VARCHAR(), values.size(), pool);
  for (auto i = 0; i < values.size(); ++i) {
    vector->set(i, StringView(values[i]));
  }
  return vector;
}

VectorPtr makeBigintVector(std::mt19937& rng, memory::MemoryPool* pool) {
  auto vector =
      BaseVector::create<FlatVector<int64_t>>(BIGINT(), kNumRows, pool);
  for (auto i = 0; i < kNumRows; ++i) {
    vector->set(i, static_cast<int64_t>(rng()));
  }
  return vector;
}

std::vector<std::string> makeStrings(
    const std::function<std::string(int32_t)>& makeValue) {
  std::vector<std::string> values;
  values.reserve(kNumRows);
  for (auto i = 0; i < kNumRows; ++i) {
    values.push_back(makeValue(i));
  }
  return values;
}

// Sort keys stored in a RowContainer, as SortBuffer stores them.
class SortInput {
 public:
  SortInput(const std::vector<VectorPtr>& keys, memory::MemoryPool* pool) {
    std::vector<TypePtr> keyTypes;
    for (const auto& key : keys) {
      keyTypes.push_back(key->type());
      compareFlags_.push_back(
          {true, true, false, CompareFlags::NullHandlingMode::kNullAsValue});
    }
    rowContainer_ = std::make_unique<RowContainer>(keyTypes, pool);
    rows_.resize(kNumRows);
    for (auto i = 0; i < kNumRows; ++i) {
      rows_[i] = rowContainer_->newRow();
    }
    for (auto column = 0; column < keys.size(); ++column) {
      DecodedVector decoded(*keys[column]);
      rowContainer_->store(
          decoded, folly::Range(rows_.data(), kNumRows), column);
    }
  }

  const RowContainer* rowContainer() const {
    return rowContainer_.get();
  }

  const std::vector<char*>& rows() const {
    return rows_;
  }

  const std::vector<CompareFlags>& compareFlags() const {
    return compareFlags_;
  }

 private:
  std::unique_ptr<RowContainer> rowContainer_;
  std::vector<char*> rows_;
  std::vector<CompareFlags> compareFlags_;
};

// Prints the prefix layout PrefixSort picks for 'input' and the memory it
// reserves, so that the timings can be read against the entry width.
void printLayout(
    const std::string& name,
    const SortInput& input,
    memory::MemoryPool* pool) {
  const auto* rowContainer = input.rowContainer();
  const auto& keyTypes = rowContainer->keyTypes();
  std::vector<std::optional<uint32_t>> maxStringLengths;
  std::vector<bool> columnHasNulls;
  for (auto i = 0; i < keyTypes.size(); ++i) {
    std::optional<uint32_t> maxStringLength;
    if (keyTypes[i]->kind() == TypeKind::VARCHAR) {
      maxStringLength = rowContainer->columnStats(i).maxBytes();
    }
    maxStringLengths.push_back(maxStringLength);
    columnHasNulls.push_back(rowContainer->columnHasNulls(i));
  }
  const auto layout = PrefixSortLayout::generate(
      keyTypes,
      columnHasNulls,
      input.compareFlags(),
      kSortConfig.maxNormalizedKeyBytes,
      kSortConfig.maxStringPrefixLength,
      maxStringLengths);
  const auto requiredBytes = PrefixSort::maxRequiredBytes(
      rowContainer, input.compareFlags(), kSortConfig, pool);
  LOG(INFO) << fmt::format(
      "{}: entrySize {}, numNormalizedKeys {}, keysComparedInRowContainer {}, "
      "maxRequiredBytes {}",
      name,
      layout.entrySize,
      layout.numNormalizedKeys,
      layout.numKeys - layout.nonPrefixSortStartIndex,
      requiredBytes);
}

void addBenchmark(
    const std::string& name,
    std::vector<VectorPtr> keys,
    memory::MemoryPool* pool,
    std::vector<std::unique_ptr<SortInput>>& inputs) {
  auto input = std::make_unique<SortInput>(keys, pool);
  printLayout(name, *input, pool);
  folly::addBenchmark(
      __FILE__, name, [input = input.get(), pool](unsigned iterations) {
        for (auto i = 0; i < iterations; ++i) {
          std::vector<char*, memory::StlAllocator<char*>> rows(*pool);
          {
            folly::BenchmarkSuspender suspender;
            rows.assign(input->rows().begin(), input->rows().end());
          }
          PrefixSort::sort(
              input->rowContainer(),
              input->compareFlags(),
              kSortConfig,
              pool,
              rows);
        }
        return iterations;
      });
  inputs.push_back(std::move(input));
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  auto rootPool = memory::memoryManager()->addRootPool();
  auto pool = rootPool->addLeafChild("leaf");

  std::mt19937 rng(42);
  // Holds the string bytes that the key vectors point to until they are
  // stored.
  std::vector<std::vector<std::string>> strings;
  std::vector<std::unique_ptr<SortInput>> inputs;

  // Keys that tie on their first 16 bytes: 40-byte URLs sharing a 29-byte
  // host and path prefix.
  strings.push_back(makeStrings([&](int32_t /*row*/) {
    return "https://www.example.com/item/" + randomString(rng, 11);
  }));
  addBenchmark(
      "url40",
      {makeVarcharVector(strings.back(), pool.get())},
      pool.get(),
      inputs);

  // 1'000 distinct 32-byte strings followed by a BIGINT that breaks the ties.
  {
    std::vector<std::string> distinct;
    for (auto i = 0; i < 1'000; ++i) {
      distinct.push_back(randomString(rng, 32));
    }
    std::uniform_int_distribution<int32_t> pick(0, distinct.size() - 1);
    strings.push_back(
        makeStrings([&](int32_t /*row*/) { return distinct[pick(rng)]; }));
  }
  addBenchmark(
      "lowCardinality32_bigint",
      {makeVarcharVector(strings.back(), pool.get()),
       makeBigintVector(rng, pool.get())},
      pool.get(),
      inputs);

  // Keys decided by their first 16 bytes: random UUIDs.
  strings.push_back(
      makeStrings([&](int32_t /*row*/) { return randomUuid(rng); }));
  addBenchmark(
      "uuid36",
      {makeVarcharVector(strings.back(), pool.get())},
      pool.get(),
      inputs);

  // The same UUIDs followed by a BIGINT that the prefix now also holds.
  addBenchmark(
      "uuid36_bigint",
      {makeVarcharVector(strings.back(), pool.get()),
       makeBigintVector(rng, pool.get())},
      pool.get(),
      inputs);

  // Distinct 8 to 16-byte strings and one 60-byte string in 1'000. The
  // longest value sets the width of every entry.
  {
    std::uniform_int_distribution<int32_t> length(8, 16);
    strings.push_back(makeStrings([&](int32_t row) {
      return randomString(rng, row % 1'000 == 0 ? 60 : length(rng));
    }));
  }
  addBenchmark(
      "short16_rareLong60",
      {makeVarcharVector(strings.back(), pool.get())},
      pool.get(),
      inputs);

  // Two distinct 40 to 60-byte string keys, which together fill the 128-byte
  // normalized key budget.
  for (auto key = 0; key < 2; ++key) {
    std::uniform_int_distribution<int32_t> length(40, 60);
    strings.push_back(makeStrings(
        [&](int32_t /*row*/) { return randomString(rng, length(rng)); }));
  }
  addBenchmark(
      "twoRandom60",
      {makeVarcharVector(strings[strings.size() - 2], pool.get()),
       makeVarcharVector(strings.back(), pool.get())},
      pool.get(),
      inputs);

  folly::runBenchmarks();
  return 0;
}
