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
#include "velox/exec/HashTable.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <numeric>
#include <tuple>

DECLARE_bool(velox_hash_join_fixed_width_output_enabled);

namespace facebook::velox::exec::test {
class HashJoinFixedWidthOutputTest : public testing::Test,
                                     public velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }
};

TEST_F(HashJoinFixedWidthOutputTest, exactBatchBoundariesMissesAndResume) {
  gflags::FlagSaver saver;
  std::vector<std::unique_ptr<VectorHasher>> hashers;
  hashers.push_back(VectorHasher::create(BIGINT(), 0));
  auto table = HashTable<true>::createForJoin(
      std::move(hashers),
      {BIGINT(), VARCHAR()},
      true,
      false,
      false,
      1000,
      pool());
  auto build = makeRowVector(
      {makeFlatVector<int64_t>(180, [](auto i) { return i % 5; }),
       makeFlatVector<int64_t>(180, [](auto i) { return i; }),
       makeFlatVector<std::string>(
           180, [](auto i) { return std::string(i % 80 + 1, 'a' + i % 20); })});
  SelectivityVector buildRows(build->size());
  std::vector<DecodedVector> decoded(3);
  for (int col = 0; col < 3; ++col) {
    decoded[col].decode(*build->childAt(col), buildRows);
  }
  for (int i = 0; i < build->size(); ++i) {
    auto* row = table->rows()->newRow();
    for (int col = 0; col < 3; ++col) {
      table->rows()->store(decoded[col], i, row, col);
    }
  }
  table->prepareJoinTable(
      {},
      BaseHashTable::kNoSpillInputStartPartitionBit,
      1000000,
      false,
      nullptr);
  ASSERT_TRUE(table->hasDuplicateKeys());
  auto probe = makeRowVector({makeFlatVector<int64_t>(
      99, [](auto i) { return i % 3 == 0 ? 100 + i : i % 5; })});
  SelectivityVector selected(probe->size());
  HashLookup lookup(table->hashers(), pool());
  table->prepareForJoinProbe(lookup, probe, selected, true);
  lookup.hits.resize(probe->size());
  std::fill(lookup.hits.begin(), lookup.hits.end(), nullptr);
  table->joinProbe(lookup);
  lookup.rows.resize(probe->size());
  std::iota(lookup.rows.begin(), lookup.rows.end(), 0);
  struct Output {
    std::vector<int32_t> batchSizes;
    std::vector<std::pair<int32_t, char*>> rows;
    bool operator==(const Output& other) const {
      return batchSizes == other.batchSizes && rows == other.rows;
    }
  };
  for (const auto [chains, ahead, prefetch] :
       {std::tuple{1, 1, false},
        std::tuple{8, 8, true},
        std::tuple{32, 1, true},
        std::tuple{1, 32, false}}) {
    for (const bool includeMisses : {false, true}) {
      for (int capacity : {1, 8, 64}) {
        for (uint64_t maxBytes :
             {uint64_t(0),
              uint64_t(1),
              uint64_t(15),
              uint64_t(16),
              uint64_t(511),
              uint64_t(512),
              ~uint64_t(0)}) {
          for (int projection = 0; projection < 5; ++projection) {
            auto run = [&](bool enabled) {
              FLAGS_velox_hash_join_fixed_width_output_enabled = enabled;
              std::vector<vector_size_t> varColumns;
              if (projection == 2 || projection == 4) {
                varColumns = {2};
              }
              const uint64_t fixedWidth = projection == 1 ? 0 : 8;
              std::optional<int64_t> estimate;
              if (projection == 3) {
                estimate = 16;
              }
              if (projection == 4) {
                estimate = 0;
              }
              BaseHashTable::JoinResultIterator iter(
                  std::move(varColumns),
                  fixedWidth,
                  estimate,
                  chains,
                  ahead,
                  prefetch);
              iter.reset(lookup);
              std::vector<vector_size_t> inputRows(capacity);
              std::vector<char*> hits(capacity);
              Output out;
              int calls = 0;
              while (!iter.atEnd()) {
                const auto n = table->listJoinResults(
                    iter,
                    includeMisses,
                    folly::Range(inputRows.data(), capacity),
                    folly::Range(hits.data(), capacity),
                    maxBytes);
                out.batchSizes.push_back(n);
                for (int i = 0; i < n; ++i) {
                  out.rows.emplace_back(inputRows[i], hits[i]);
                }
                if (++calls > 10000) {
                  throw std::runtime_error("Join iterator failed to progress");
                }
              }
              return out;
            };
            const auto expected = run(false);
            const auto actual = run(true);
            EXPECT_TRUE(expected == actual)
                << chains << '/' << ahead << '/' << prefetch
                << " capacity=" << capacity << " maxBytes=" << maxBytes
                << " projection=" << projection;
            EXPECT_FALSE(actual.rows.empty());
          }
        }
      }
    }
  }
}
} // namespace facebook::velox::exec::test
