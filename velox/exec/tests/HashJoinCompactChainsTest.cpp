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

DECLARE_bool(velox_hash_join_head_chain_drain_enabled);
DECLARE_bool(velox_hash_join_fixed_width_output_enabled);
DECLARE_bool(velox_hash_join_compact_chains_enabled);
DECLARE_int64(velox_hash_join_compact_chains_max_bytes);
DECLARE_bool(velox_hash_join_estimated_width_output_enabled);

namespace facebook::velox::exec::test {
class HashJoinCompactChainsTestHelper {
 public:
  template <bool ignoreNullKeys>
  static void modeAndRebuild(
      HashTable<ignoreNullKeys>* table,
      BaseHashTable::HashMode mode) {
    if (table->hashMode() != mode) {
      // Initial HASH/ARRAY/NORMALIZED build expects unlinked accumulated
      // rows. Reset links when this fixture explicitly rebuilds a table in
      // another mode; production HashBuild selects its mode before linking.
      RowContainerIterator iterator;
      char* rows[256];
      while (auto count = table->rows_->listRows(
                 &iterator, 256, RowContainer::kUnlimited, rows)) {
        for (int32_t i = 0; i < count; ++i) {
          table->nextRow(rows[i]) = nullptr;
        }
      }
      table->setHashMode(
          mode, 0, BaseHashTable::kNoSpillInputStartPartitionBit);
    }
    table->buildCompactJoinChains();
  }
};
class HashJoinCompactChainsTest : public testing::Test,
                                  public velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }
};

TEST_F(HashJoinCompactChainsTest, exactBatchBoundariesMissesAndResume) {
  gflags::FlagSaver saver;
  for (const auto mode :
       {BaseHashTable::HashMode::kArray,
        BaseHashTable::HashMode::kNormalizedKey,
        BaseHashTable::HashMode::kHash}) {
    FLAGS_velox_hash_join_compact_chains_enabled = true;
    FLAGS_velox_hash_join_compact_chains_max_bytes = 128LL << 20;
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
        {makeFlatVector<int64_t>(
             180, [](auto i) { return i < 160 ? i % 5 : 1000 + i; }),
         makeFlatVector<int64_t>(180, [](auto i) { return i; }),
         makeFlatVector<std::string>(180, [](auto i) {
           return std::string(i % 80 + 1, 'a' + i % 20);
         })});
    SelectivityVector buildRows(build->size());
    std::vector<DecodedVector> decoded(3);
    for (int col = 0; col < 3; ++col) {
      decoded[col].decode(*build->childAt(col), buildRows);
    }
    raw_vector<uint64_t> analyzed(pool());
    analyzed.resize(build->size());
    for (auto& hasher : table->hashers()) {
      hasher->decode(*build->childAt(hasher->channel()), buildRows);
      hasher->computeValueIds(buildRows, analyzed);
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
    HashJoinCompactChainsTestHelper::modeAndRebuild(table.get(), mode);
    ASSERT_EQ(table->hashMode(), mode);
    std::unordered_map<std::string, RuntimeMetric> stats;
    table->addRuntimeStats(stats);
    ASSERT_EQ(stats.at("hashtable.compactJoinChains").sum, 5);
    ASSERT_EQ(stats.at("hashtable.compactJoinRows").sum, 160);
    ASSERT_GT(stats.at("hashtable.compactJoinBytes").sum, 160 * sizeof(char*));
    auto probe = makeRowVector({makeFlatVector<int64_t>(99, [](auto i) {
      return i % 3 == 0 ? (i % 6 == 0 ? 1160 + i % 20 : 100 + i) : i % 5;
    })});
    SelectivityVector selected(probe->size());
    HashLookup lookup(table->hashers(), pool());
    table->prepareForJoinProbe(lookup, probe, selected, true);
    lookup.hits.resize(probe->size());
    std::fill(lookup.hits.begin(), lookup.hits.end(), nullptr);
    table->joinProbe(lookup);
    ASSERT_NE(lookup.hits[0], nullptr); // Singleton chain.
    ASSERT_EQ(lookup.hits[3], nullptr); // Miss.
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
            for (int projection = 0; projection < 6; ++projection) {
              auto run = [&](bool enabled) {
                FLAGS_velox_hash_join_head_chain_drain_enabled = enabled;
                FLAGS_velox_hash_join_fixed_width_output_enabled = enabled;
                FLAGS_velox_hash_join_estimated_width_output_enabled = enabled;
                FLAGS_velox_hash_join_compact_chains_enabled = enabled;
                std::vector<vector_size_t> varColumns;
                if (projection == 2 || projection == 4 || projection == 5) {
                  varColumns = {2};
                }
                const uint64_t fixedWidth = projection == 1 ? 0 : 8;
                std::optional<int64_t> estimate;
                if (projection == 3 || projection == 5) {
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
                    throw std::runtime_error(
                        "Join iterator failed to progress");
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
    stats.clear();
    table->addRuntimeStats(stats);
    EXPECT_GT(stats.at("hashtable.compactJoinSharedBulkRows").sum, 0);
    // The optional cap is honored on rebuild, with the original linked rows
    // intact; a mode rebuild and table clear both invalidate old postings.
    FLAGS_velox_hash_join_compact_chains_enabled = true;
    for (const int64_t budget : {int64_t(0), int64_t(1)}) {
      FLAGS_velox_hash_join_compact_chains_max_bytes = budget;
      HashJoinCompactChainsTestHelper::modeAndRebuild(table.get(), mode);
      stats.clear();
      table->addRuntimeStats(stats);
      EXPECT_EQ(stats.count("hashtable.compactJoinRows"), 0);
      EXPECT_EQ(table->rows()->numRows(), 180);
    }
    FLAGS_velox_hash_join_compact_chains_max_bytes = 128LL << 20;
    HashJoinCompactChainsTestHelper::modeAndRebuild(table.get(), mode);
    stats.clear();
    table->addRuntimeStats(stats);
    ASSERT_EQ(stats.at("hashtable.compactJoinRows").sum, 160);
    // Erase invalidates the index before any underlying row can be released.
    auto* victim = lookup.hits[1];
    ASSERT_NE(victim, nullptr);
    table->erase(folly::Range<char**>(&victim, 1));
    stats.clear();
    table->addRuntimeStats(stats);
    EXPECT_EQ(stats.count("hashtable.compactJoinRows"), 0);
    EXPECT_EQ(table->rows()->numRows(), 179);
    table->clear(false);
    stats.clear();
    table->addRuntimeStats(stats);
    EXPECT_EQ(stats.count("hashtable.compactJoinRows"), 0);
    EXPECT_EQ(table->rows()->numRows(), 0);
  }
}
TEST_F(HashJoinCompactChainsTest, nullKeysExactBoundariesAndFallback) {
  gflags::FlagSaver saver;
  for (const auto mode :
       {BaseHashTable::HashMode::kArray,
        BaseHashTable::HashMode::kNormalizedKey,
        BaseHashTable::HashMode::kHash}) {
    FLAGS_velox_hash_join_compact_chains_enabled = true;
    FLAGS_velox_hash_join_compact_chains_max_bytes = 128LL << 20;
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    hashers.push_back(VectorHasher::create(BIGINT(), 0));
    auto table = HashTable<false>::createForJoin(
        std::move(hashers),
        {BIGINT(), VARCHAR()},
        true,
        false,
        false,
        1000,
        pool());
    auto build = makeRowVector(
        {makeNullableFlatVector<int64_t>([] {
           std::vector<std::optional<int64_t>> keys(180);
           for (int i = 0; i < 180; ++i) {
             if (i % 5 != 0) {
               keys[i] = i % 5;
             }
           }
           return keys;
         }()),
         makeFlatVector<int64_t>(180, [](auto i) { return i; }),
         makeFlatVector<std::string>(180, [](auto i) {
           return std::string(i % 80 + 1, 'a' + i % 20);
         })});
    SelectivityVector buildRows(build->size());
    std::vector<DecodedVector> decoded(3);
    for (int col = 0; col < 3; ++col) {
      decoded[col].decode(*build->childAt(col), buildRows);
    }
    raw_vector<uint64_t> analyzed(pool());
    analyzed.resize(build->size());
    for (auto& hasher : table->hashers()) {
      hasher->decode(*build->childAt(hasher->channel()), buildRows);
      hasher->computeValueIds(buildRows, analyzed);
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
    HashJoinCompactChainsTestHelper::modeAndRebuild(table.get(), mode);
    ASSERT_EQ(table->hashMode(), mode);
    std::unordered_map<std::string, RuntimeMetric> stats;
    table->addRuntimeStats(stats);
    ASSERT_EQ(stats.at("hashtable.compactJoinChains").sum, 5);
    ASSERT_EQ(stats.at("hashtable.compactJoinRows").sum, 180);
    ASSERT_GT(stats.at("hashtable.compactJoinBytes").sum, 180 * sizeof(char*));
    auto probe = makeRowVector({makeNullableFlatVector<int64_t>([] {
      std::vector<std::optional<int64_t>> keys(99);
      for (int i = 0; i < 99; ++i) {
        if (i % 3 == 0) {
          keys[i] = 100 + i;
        } else if (i % 5 != 0) {
          keys[i] = i % 5;
        }
      }
      return keys;
    }())});
    SelectivityVector selected(probe->size());
    HashLookup lookup(table->hashers(), pool());
    for (auto& hasher : table->hashers()) {
      hasher->decode(*probe->childAt(hasher->channel()), selected);
    }
    table->prepareForJoinProbe(lookup, probe, selected, false);
    lookup.hits.resize(probe->size());
    std::fill(lookup.hits.begin(), lookup.hits.end(), nullptr);
    table->joinProbe(lookup);
    ASSERT_TRUE(probe->childAt(0)->isNullAt(5));
    ASSERT_NE(lookup.hits[5], nullptr); // A real long NULL-key chain.
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
            for (int projection = 0; projection < 6; ++projection) {
              auto run = [&](bool enabled) {
                FLAGS_velox_hash_join_head_chain_drain_enabled = enabled;
                FLAGS_velox_hash_join_fixed_width_output_enabled = enabled;
                FLAGS_velox_hash_join_estimated_width_output_enabled = enabled;
                FLAGS_velox_hash_join_compact_chains_enabled = enabled;
                std::vector<vector_size_t> varColumns;
                if (projection == 2 || projection == 4 || projection == 5) {
                  varColumns = {2};
                }
                const uint64_t fixedWidth = projection == 1 ? 0 : 8;
                std::optional<int64_t> estimate;
                if (projection == 3 || projection == 5) {
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
                    throw std::runtime_error(
                        "Join iterator failed to progress");
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
    stats.clear();
    table->addRuntimeStats(stats);
    EXPECT_GT(stats.at("hashtable.compactJoinSharedBulkRows").sum, 0);
    // The optional cap is honored on rebuild, with the original linked rows
    // intact; a mode rebuild and table clear both invalidate old postings.
    FLAGS_velox_hash_join_compact_chains_enabled = true;
    for (const int64_t budget : {int64_t(0), int64_t(1)}) {
      FLAGS_velox_hash_join_compact_chains_max_bytes = budget;
      HashJoinCompactChainsTestHelper::modeAndRebuild(table.get(), mode);
      stats.clear();
      table->addRuntimeStats(stats);
      EXPECT_EQ(stats.count("hashtable.compactJoinRows"), 0);
      EXPECT_EQ(table->rows()->numRows(), 180);
    }
    FLAGS_velox_hash_join_compact_chains_max_bytes = 128LL << 20;
    HashJoinCompactChainsTestHelper::modeAndRebuild(table.get(), mode);
    stats.clear();
    table->addRuntimeStats(stats);
    ASSERT_EQ(stats.at("hashtable.compactJoinRows").sum, 180);
    // Erase invalidates the index before any underlying row can be released.
    auto* victim = lookup.hits[1];
    ASSERT_NE(victim, nullptr);
    table->erase(folly::Range<char**>(&victim, 1));
    stats.clear();
    table->addRuntimeStats(stats);
    EXPECT_EQ(stats.count("hashtable.compactJoinRows"), 0);
    EXPECT_EQ(table->rows()->numRows(), 179);
    table->clear(false);
    stats.clear();
    table->addRuntimeStats(stats);
    EXPECT_EQ(stats.count("hashtable.compactJoinRows"), 0);
    EXPECT_EQ(table->rows()->numRows(), 0);
  }
}
} // namespace facebook::velox::exec::test
