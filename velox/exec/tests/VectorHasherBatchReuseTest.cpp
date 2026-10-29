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
#include "velox/exec/VectorHasher.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>
#include <limits>

namespace facebook::velox::exec::test {
class VectorHasherBatchReuseTest : public testing::Test,
                                   public velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void compareIds(const VectorPtr& input, bool selected, uint64_t multiplier) {
    VectorHasher legacy(input->type(), 0), reused(input->type(), 0);
    reused.setBatchReuseEnabled(true);
    legacy.enableValueIds(multiplier, VectorHasher::kNoLimit);
    reused.enableValueIds(multiplier, VectorHasher::kNoLimit);
    SelectivityVector rows(input->size());
    if (selected) {
      for (auto row = 0; row < input->size(); row += 7) {
        rows.setValid(row, false);
      }
      rows.updateBounds();
    }
    raw_vector<uint64_t> oldIds(input->size()), newIds(input->size());
    std::fill(oldIds.begin(), oldIds.end(), 123);
    std::fill(newIds.begin(), newIds.end(), 123);
    for (int batch = 0; batch < 2; ++batch) {
      legacy.decode(*input, rows);
      reused.decode(*input, rows);
      ASSERT_EQ(
          legacy.computeValueIds(rows, oldIds),
          reused.computeValueIds(rows, newIds));
      for (auto row = 0; row < input->size(); ++row) {
        EXPECT_EQ(oldIds[row], newIds[row]) << row;
      }
    }
    EXPECT_GT(reused.takeBatchReuseStats().valueIdReusedRows, 0);
  }
};

TEST_F(VectorHasherBatchReuseTest, typedRunsNullsSelectionsAndMultipliers) {
  constexpr auto count = 1024;
  auto nulls = velox::test::VectorMaker::nullEvery(13);
  std::vector<VectorPtr> vectors{
      makeFlatVector<int8_t>(
          count, [](auto row) { return row / 12 % 19; }, nulls),
      makeFlatVector<int16_t>(
          count, [](auto row) { return row / 12; }, nulls),
      makeFlatVector<int32_t>(
          count, [](auto row) { return row / 12; }, nulls),
      makeFlatVector<int64_t>(
          count, [](auto row) { return int64_t(row / 12) << 40; }, nulls),
      makeFlatVector<int128_t>(
          count,
          [](auto row) { return (int128_t(row / 12) << 100) + 7; },
          nulls),
      makeFlatVector<Timestamp>(
          count, [](auto row) { return Timestamp(row / 12, 0); }, nulls),
      makeFlatVector<std::string>(
          count,
          [](auto row) { return "long-string-" + std::to_string(row / 12); },
          nulls),
  };
  for (const auto& vector : vectors) {
    SCOPED_TRACE(vector->type()->toString());
    compareIds(vector, false, 1);
    compareIds(vector, true, 17);
  }
}

TEST_F(VectorHasherBatchReuseTest, dictionaryOversizedBaseOuterNulls) {
  auto base = makeFlatVector<int64_t>(
      4096, [](auto row) { return int64_t(row) << 30; });
  auto indices = makeIndices(1024, [](auto row) { return row / 8; });
  auto vector = BaseVector::wrapInDictionary(
      makeNulls(1024, velox::test::VectorMaker::nullEvery(11)),
      indices,
      1024,
      base);
  compareIds(vector, true, 3);
  compareIds(vector, false, 1);
}

TEST_F(VectorHasherBatchReuseTest, hashReuseMatchesLegacyIncludingMixAndNulls) {
  std::vector<VectorPtr> bases{
      makeFlatVector<int64_t>(4096, [](auto row) { return row; }),
      makeFlatVector<double>(
          4096,
          [](auto row) {
            return row % 13 ? double(row)
                            : std::numeric_limits<double>::quiet_NaN();
          }),
      makeFlatVector<std::string>(
          4096,
          [](auto row) { return "non-inline-binary-" + std::to_string(row); }),
      makeFlatVector<int128_t>(
          4096, [](auto row) { return (int128_t(row) << 100) + 9; }),
  };
  for (auto& base : bases) {
    auto vector = BaseVector::wrapInDictionary(
        makeNulls(1024, velox::test::VectorMaker::nullEvery(11)),
        makeIndices(1024, [](auto row) { return row / 8; }),
        1024,
        base);
    VectorHasher legacy(vector->type(), 0), reused(vector->type(), 0);
    reused.setBatchReuseEnabled(true);
    SelectivityVector rows(vector->size());
    rows.setValid(33, false);
    for (bool mix : {false, true}) {
      raw_vector<uint64_t> a(vector->size()), b(vector->size());
      std::fill(a.begin(), a.end(), 99);
      std::fill(b.begin(), b.end(), 99);
      legacy.decode(*vector, rows);
      reused.decode(*vector, rows);
      legacy.hash(rows, mix, a);
      reused.hash(rows, mix, b);
      for (auto i = 0; i < vector->size(); ++i) {
        EXPECT_EQ(a[i], b[i]);
      }
    }
    const auto stats = reused.takeBatchReuseStats();
    EXPECT_GT(stats.encodedHashReusedRows, stats.encodedHashComputations);
  }
}

TEST_F(VectorHasherBatchReuseTest, unmappableRangeAndRetryPreserveStatistics) {
  auto input = makeFlatVector<int64_t>(
      2048, [](auto row) { return int64_t(row / 12) << 40; });
  VectorHasher a(BIGINT(), 0), b(BIGINT(), 0);
  b.setBatchReuseEnabled(true);
  SelectivityVector rows(input->size());
  raw_vector<uint64_t> x(input->size()), y(input->size());
  a.decode(*input, rows);
  b.decode(*input, rows);
  EXPECT_FALSE(a.computeValueIds(rows, x));
  EXPECT_FALSE(b.computeValueIds(rows, y));
  uint64_t ar, ad, br, bd;
  a.cardinality(0, ar, ad);
  b.cardinality(0, br, bd);
  EXPECT_EQ(ar, br);
  EXPECT_EQ(ad, bd);
  EXPECT_EQ(
      a.enableValueIds(7, VectorHasher::kNoLimit),
      b.enableValueIds(7, VectorHasher::kNoLimit));
  std::fill(x.begin(), x.end(), 0);
  std::fill(y.begin(), y.end(), 0);
  EXPECT_TRUE(a.computeValueIds(rows, x));
  EXPECT_TRUE(b.computeValueIds(rows, y));
  for (auto i = 0; i < input->size(); ++i) {
    EXPECT_EQ(x[i], y[i]);
  }
}

TEST_F(VectorHasherBatchReuseTest, realDedupGroupProbe) {
  for (bool enabled : {false, true}) {
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    hashers.push_back(VectorHasher::create(BIGINT(), 0));
    hashers.back()->setBatchReuseEnabled(enabled);
    auto table = HashTable<true>::createForJoin(
        std::move(hashers), {}, false, false, false, 100000, pool_.get());
    auto keys = makeFlatVector<int64_t>(
        4096, [](auto row) { return int64_t(row / 12) << 32; });
    auto input = makeRowVector({keys});
    HashLookup lookup(table->hashers(), pool_.get());
    SelectivityVector rows(input->size());
    for (int batch = 0; batch < 2; ++batch) {
      table->prepareForGroupProbe(
          lookup, input, rows, BaseHashTable::kNoSpillInputStartPartitionBit);
      table->groupProbe(lookup, BaseHashTable::kNoSpillInputStartPartitionBit);
      EXPECT_EQ(table->numDistinct(), 342);
      for (auto row = 1; row < input->size(); ++row) {
        if (row % 12) {
          EXPECT_EQ(lookup.hits[row], lookup.hits[row - 1]);
        }
      }
    }
  }
}

} // namespace facebook::velox::exec::test
