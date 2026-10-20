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
class VectorHasherAdaptiveTest : public testing::Test,
                                 public velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  VectorHasher::HashJoinAnalysis analyze(
      VectorHasher& hasher,
      const VectorPtr& input,
      uint64_t& seen,
      uint32_t minRows = 32768,
      uint32_t minPct = 80) {
    SelectivityVector rows(input->size());
    hasher.decode(*input, rows);
    seen += rows.countSelected();
    return hasher.analyzeForHashJoin(rows, seen, minRows, minPct);
  }

  std::pair<uint64_t, uint64_t> cardinality(VectorHasher& hasher) {
    uint64_t range, distinct;
    hasher.cardinality(0, range, distinct);
    return {range, distinct};
  }

  VectorPtr strings(vector_size_t count, int distinct, const TypePtr& type) {
    auto result =
        BaseVector::create<FlatVector<StringView>>(type, count, pool_.get());
    for (auto i = 0; i < count; ++i) {
      auto value = "long-key-" + std::to_string(i % distinct);
      value[4] = '\0';
      result->set(i, StringView(value));
      if (i % 13 == 0)
        result->setNull(i, true);
    }
    return result;
  }
};

TEST_F(VectorHasherAdaptiveTest, lowCardinalityMatchesLegacy) {
  constexpr vector_size_t kRows = 8192;
  std::vector<VectorPtr> inputs{
      makeFlatVector<bool>(kRows, [](auto row) { return row % 2; }),
      makeFlatVector<int8_t>(kRows, [](auto row) { return row % 17; }),
      makeFlatVector<int16_t>(kRows, [](auto row) { return row % 17; }),
      makeFlatVector<int32_t>(kRows, [](auto row) { return row % 17; }),
      makeFlatVector<int64_t>(kRows, [](auto row) { return row % 17; }),
      makeFlatVector<int128_t>(
          kRows, [](auto row) { return (int128_t{row % 17} << 100) + 9; }),
      makeFlatVector<Timestamp>(
          kRows, [](auto row) { return Timestamp(row % 17, 0); }),
      strings(kRows, 17, VARCHAR()),
      strings(kRows, 17, VARBINARY())};
  for (const auto& type : std::vector<TypePtr>{VARCHAR(), VARBINARY()}) {
    auto plain = strings(kRows, 17, type);
    for (vector_size_t i = 0; i < plain->size(); ++i) {
      plain->setNull(i, false);
    }
    inputs.push_back(plain);
  }
  for (const auto& input : inputs) {
    SCOPED_TRACE(input->type()->toString());
    VectorHasher legacy(input->type(), 0), adaptive(input->type(), 0);
    uint64_t seen = 0;
    raw_vector<uint64_t> oldIds(kRows), newIds(kRows);
    SelectivityVector rows(kRows);
    for (int batch = 0; batch < 8; ++batch) {
      legacy.decode(*input, rows);
      legacy.computeValueIds(rows, oldIds);
      const auto result = analyze(adaptive, input, seen);
      EXPECT_FALSE(result.abandonedDistincts);
      EXPECT_FALSE(result.rangeOnly);
    }
    EXPECT_EQ(cardinality(legacy), cardinality(adaptive));
    if (input->typeKind() == TypeKind::BOOLEAN) {
      EXPECT_EQ(legacy.enableValueRange(1, 0), adaptive.enableValueRange(1, 0));
    } else {
      EXPECT_EQ(legacy.enableValueIds(1, 0), adaptive.enableValueIds(1, 0));
    }
    legacy.decode(*input, rows);
    adaptive.decode(*input, rows);
    ASSERT_TRUE(legacy.computeValueIds(rows, oldIds));
    ASSERT_TRUE(adaptive.computeValueIds(rows, newIds));
    for (auto row = 0; row < kRows; ++row)
      EXPECT_EQ(oldIds[row], newIds[row]);
  }
}

TEST_F(VectorHasherAdaptiveTest, repeatedRunsNeedOneDictionaryLookup) {
  VectorHasher adaptive(BIGINT(), 0);
  auto input = makeFlatVector<int64_t>(8192, [](auto row) { return row / 12; });
  uint64_t seen = 0;
  const auto result = analyze(adaptive, input, seen);
  EXPECT_EQ(result.dictionaryLookups, (8192 + 11) / 12);
  EXPECT_EQ(result.distinctValues, (8192 + 11) / 12);
  EXPECT_FALSE(result.abandonedDistincts);
}

TEST_F(VectorHasherAdaptiveTest, denseIntegerAbandonsDictionaryKeepsRange) {
  VectorHasher adaptive(BIGINT(), 0);
  uint64_t seen = 0;
  for (int batch = 0; batch < 8; ++batch) {
    auto input = makeFlatVector<int64_t>(
        8192, [batch](auto row) { return int64_t(batch * 8192 + row) * 3; });
    auto result = analyze(adaptive, input, seen);
    EXPECT_EQ(result.abandonedDistincts, batch == 3);
    EXPECT_EQ(result.rangeOnly, batch >= 3);
    ASSERT_TRUE(adaptive.mayUseValueIds());
  }
  auto [range, distinct] = cardinality(adaptive);
  EXPECT_EQ(range, 3 * (65536 - 1) + 2);
  EXPECT_EQ(distinct, VectorHasher::kRangeTooLarge);
  EXPECT_EQ(adaptive.enableValueRange(1, 0), range);
  VectorHasher restored(BIGINT(), 0);
  restored.deserializeState(adaptive.serializeState());
  auto probe = makeFlatVector<int64_t>({0, 3, 65535 * 3});
  SelectivityVector rows(3);
  raw_vector<uint64_t> ids(3);
  adaptive.decode(*probe, rows);
  ASSERT_TRUE(adaptive.computeValueIds(rows, ids));
  EXPECT_EQ(ids[0], 1);
  EXPECT_EQ(ids[1], 4);
  EXPECT_EQ(ids[2], 65535 * 3 + 1);
  restored.decode(*probe, rows);
  ASSERT_TRUE(restored.computeValueIds(rows, ids));
  EXPECT_EQ(ids[0], 1);
  EXPECT_EQ(ids[2], 65535 * 3 + 1);
}

TEST_F(VectorHasherAdaptiveTest, wideDomainsForceGenericHash) {
  std::vector<VectorPtr> inputs{
      makeFlatVector<int64_t>(
          32768,
          [](auto row) {
            return row % 2 ? std::numeric_limits<int64_t>::max() - row
                           : std::numeric_limits<int64_t>::min() + row;
          }),
      makeFlatVector<int128_t>(
          32768, [](auto row) { return (int128_t{row} << 100) + 9; })};
  auto bytes = strings(32768, 32768, VARBINARY());
  // Remove nulls so that the distinct ratio has a simple exact boundary.
  for (auto i = 0; i < bytes->size(); ++i)
    bytes->setNull(i, false);
  inputs.push_back(bytes);
  for (const auto& input : inputs) {
    SCOPED_TRACE(input->type()->toString());
    VectorHasher adaptive(input->type(), 0);
    uint64_t seen = 0;
    const auto result = analyze(adaptive, input, seen);
    EXPECT_TRUE(result.abandonedDistincts);
    EXPECT_FALSE(result.rangeOnly);
    EXPECT_FALSE(adaptive.mayUseValueIds());
    EXPECT_EQ(
        cardinality(adaptive),
        std::make_pair(
            VectorHasher::kRangeTooLarge, VectorHasher::kRangeTooLarge));
    EXPECT_FALSE(adaptive.getFilter(false));
  }
}

TEST_F(VectorHasherAdaptiveTest, dictionaryNullsConstantsAndSelections) {
  auto base = strings(16, 16, VARCHAR());
  for (auto i = 0; i < base->size(); ++i)
    base->setNull(i, false);
  auto indices = makeIndices(4096, [](auto row) { return row % 16; });
  auto nulls = allocateNulls(4096, pool_.get());
  bits::setNull(nulls->asMutable<uint64_t>(), 0, true);
  auto input = BaseVector::wrapInDictionary(nulls, indices, 4096, base);
  VectorHasher legacy(VARCHAR(), 0), adaptive(VARCHAR(), 0);
  SelectivityVector rows(input->size());
  raw_vector<uint64_t> ids(input->size());
  legacy.decode(*input, rows);
  legacy.computeValueIds(rows, ids);
  uint64_t seen = 0;
  auto result = analyze(adaptive, input, seen, 1024);
  EXPECT_FALSE(result.abandonedDistincts);
  EXPECT_EQ(result.distinctValues, 16);
  EXPECT_EQ(cardinality(legacy), cardinality(adaptive));

  // A null constant must never decode a value; an unselected outlier must not
  // affect the distinct count or integer range.
  auto nullConstant = makeNullConstant(TypeKind::BIGINT, 100);
  VectorHasher empty(BIGINT(), 0);
  seen = 0;
  EXPECT_FALSE(analyze(empty, nullConstant, seen, 64).abandonedDistincts);
  EXPECT_TRUE(empty.empty());
  auto numbers =
      makeFlatVector<int64_t>({5, 6, std::numeric_limits<int64_t>::max()});
  SelectivityVector selected(3);
  selected.setValid(2, false);
  selected.updateBounds();
  empty.decode(*numbers, selected);
  empty.analyzeForHashJoin(selected, 2, 64, 80);
  EXPECT_EQ(cardinality(empty), std::make_pair(uint64_t{3}, uint64_t{3}));
}

TEST_F(VectorHasherAdaptiveTest, timestampPrecisionAndEmptyInput) {
  VectorHasher timestamp(TIMESTAMP(), 0);
  uint64_t seen = 0;
  auto input =
      makeFlatVector<Timestamp>({Timestamp(10, 100), Timestamp(10, 200)});
  analyze(timestamp, input, seen, 1);
  EXPECT_FALSE(timestamp.mayUseValueIds());
  VectorHasher empty(BIGINT(), 0);
  SelectivityVector none(0);
  EXPECT_FALSE(empty.analyzeForHashJoin(none, 0, 64, 80).abandonedDistincts);
  EXPECT_TRUE(empty.empty());
}

TEST_F(VectorHasherAdaptiveTest, thresholdAndMerge) {
  VectorHasher below(BIGINT(), 0);
  uint64_t seen = 0;
  auto input = makeFlatVector<int64_t>(64, [](auto row) { return row % 51; });
  EXPECT_FALSE(analyze(below, input, seen, 64, 80).abandonedDistincts);
  VectorHasher at(BIGINT(), 0);
  seen = 0;
  input = makeFlatVector<int64_t>(64, [](auto row) { return row % 52; });
  EXPECT_TRUE(analyze(at, input, seen, 64, 80).abandonedDistincts);
  VectorHasher untouched(BIGINT(), 0);
  seen = 0;
  input = makeFlatVector<int64_t>(63, [](auto row) { return row; });
  EXPECT_FALSE(analyze(untouched, input, seen, 64).abandonedDistincts);

  // Merging a range-only builder propagates its bounds and loss of exact IDs,
  // in either order, including the empty-destination fast path.
  for (bool reverse : {false, true}) {
    VectorHasher a(BIGINT(), 0), b(BIGINT(), 0);
    uint64_t aSeen = 0, bSeen = 0;
    analyze(
        a,
        makeFlatVector<int64_t>(128, [](auto row) { return row; }),
        aSeen,
        64);
    analyze(b, makeFlatVector<int64_t>({-10, -9}), bSeen, 64);
    if (reverse) {
      b.merge(a, 100000);
      EXPECT_EQ(cardinality(b).first, 139);
      EXPECT_EQ(cardinality(b).second, VectorHasher::kRangeTooLarge);
    } else {
      a.merge(b, 100000);
      EXPECT_EQ(cardinality(a).first, 139);
      EXPECT_EQ(cardinality(a).second, VectorHasher::kRangeTooLarge);
    }
  }
}

TEST_F(
    VectorHasherAdaptiveTest,
    mergeIntoEmptyAndRangeOverflowAfterAbandonment) {
  VectorHasher source(BIGINT(), 0), destination(BIGINT(), 0);
  uint64_t seen = 0;
  analyze(
      source,
      makeFlatVector<int64_t>(128, [](auto row) { return row; }),
      seen,
      64);
  destination.merge(source, 100000);
  EXPECT_EQ(cardinality(destination), cardinality(source));
  auto extremes = makeFlatVector<int64_t>(
      {std::numeric_limits<int64_t>::min(),
       std::numeric_limits<int64_t>::max()});
  analyze(source, extremes, seen, 64);
  EXPECT_FALSE(source.mayUseValueIds());
}

TEST_F(VectorHasherAdaptiveTest, finalJoinModesAndProbe) {
  for (bool sparse : {false, true}) {
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    hashers.push_back(VectorHasher::create(BIGINT(), 0));
    auto table = HashTable<true>::createForJoin(
        std::move(hashers), {}, true, false, false, 100000, pool_.get());
    auto keys = makeFlatVector<int64_t>(128, [sparse](auto row) {
      return sparse ? (row % 2 ? std::numeric_limits<int64_t>::max() - row
                               : std::numeric_limits<int64_t>::min() + row)
                    : int64_t(row) * 3;
    });
    uint64_t seen = 0;
    analyze(*table->hashers()[0], keys, seen, 64);
    SelectivityVector rows(keys->size());
    DecodedVector decoded(*keys, rows);
    std::vector<char*> stored(keys->size());
    for (auto i = 0; i < keys->size(); ++i) {
      stored[i] = table->rows()->newRow();
      table->rows()->store(decoded, i, stored[i], 0);
    }
    table->prepareJoinTable(
        {},
        BaseHashTable::kNoSpillInputStartPartitionBit,
        100000,
        false,
        nullptr);
    EXPECT_EQ(
        table->hashMode(),
        sparse ? BaseHashTable::HashMode::kHash
               : BaseHashTable::HashMode::kArray);
    auto probe = makeRowVector({keys});
    SelectivityVector selected(keys->size());
    HashLookup lookup(table->hashers(), pool_.get());
    lookup.hits.resize(keys->size());
    table->prepareForJoinProbe(lookup, probe, selected, false);
    table->joinProbe(lookup);
    for (auto i = 0; i < keys->size(); ++i)
      EXPECT_EQ(lookup.hits[i], stored[i]);
  }
}
} // namespace facebook::velox::exec::test
