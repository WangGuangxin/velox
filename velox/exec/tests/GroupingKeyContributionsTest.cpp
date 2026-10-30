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

#include "velox/exec/GroupingKeyContributions.h"

#include <gtest/gtest.h>

#include "velox/exec/HashTable.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::exec::test {

class GroupingKeyContributionsTest : public testing::Test,
                                     public velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  std::unique_ptr<BaseHashTable> makeTable(
      const std::vector<TypePtr>& keyTypes) {
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    for (auto i = 0; i < keyTypes.size(); ++i) {
      hashers.push_back(VectorHasher::create(keyTypes[i], i));
    }
    return HashTable<false>::createForAggregation(
        std::move(hashers), {}, pool_.get());
  }

  // Probes 'batches' in order, returning the hash or value ID each probe
  // computed for every row. 'contributions', when set, is installed on the
  // table, which is otherwise identical.
  std::vector<std::vector<uint64_t>> probeAll(
      const std::vector<TypePtr>& keyTypes,
      const std::vector<RowVectorPtr>& batches,
      GroupingKeyContributions* contributions) {
    auto table = makeTable(keyTypes);
    table->setKeyContributions(contributions);
    HashLookup lookup(table->hashers(), pool_.get());
    std::vector<std::vector<uint64_t>> result;
    for (const auto& batch : batches) {
      SelectivityVector rows(batch->size());
      table->prepareForGroupProbe(
          lookup, batch, rows, BaseHashTable::kNoSpillInputStartPartitionBit);
      // Captured before the probe, which is free to reuse 'hashes'.
      result.emplace_back(
          lookup.hashes.begin(), lookup.hashes.begin() + batch->size());
      table->groupProbe(lookup, BaseHashTable::kNoSpillInputStartPartitionBit);
    }
    return result;
  }

  // Asserts that installing the cache changes neither the computed hashes nor
  // the number of groups the probes produce.
  void expectReuseIsTransparent(
      const std::vector<TypePtr>& keyTypes,
      const std::vector<RowVectorPtr>& batches,
      int64_t maxBytes = GroupingKeyContributions::kDefaultMaxBytes) {
    GroupingKeyContributions contributions(
        keyTypes.size(), pool_.get(), maxBytes);
    const auto expected = probeAll(keyTypes, batches, nullptr);
    const auto actual = probeAll(keyTypes, batches, &contributions);
    ASSERT_EQ(actual.size(), expected.size());
    for (auto batch = 0; batch < actual.size(); ++batch) {
      EXPECT_EQ(actual[batch], expected[batch]) << "batch " << batch;
    }
    lastReusedRows_ = contributions.numReusedRows();
    lastUncachedRows_ = contributions.numUncachedRows();
  }

  // Mimics Expand: one output per grouping set, all sharing 'columns' except
  // for the key each set replaces with a null constant.
  std::vector<RowVectorPtr> fanOut(
      const std::vector<VectorPtr>& columns,
      int32_t numSets) {
    std::vector<RowVectorPtr> batches;
    const auto size = columns[0]->size();
    for (auto set = 0; set < numSets; ++set) {
      auto children = columns;
      if (set > 0) {
        const auto nulled = (set - 1) % columns.size();
        children[nulled] = BaseVector::createNullConstant(
            columns[nulled]->type(), size, pool_.get());
      }
      batches.push_back(makeRowVector(children));
    }
    return batches;
  }

  int64_t lastReusedRows_{0};
  int64_t lastUncachedRows_{0};
};

TEST_F(GroupingKeyContributionsTest, valueIdStringKeys) {
  // Few enough distinct strings to stay in value-ID mode, long enough that
  // each one costs an F14 lookup rather than arithmetic on its bytes.
  auto columns = std::vector<VectorPtr>{
      makeFlatVector<std::string>(
          256,
          [](auto row) { return "grouping-key-" + std::to_string(row % 7); },
          velox::test::VectorMaker::nullEvery(5)),
      makeFlatVector<std::string>(
          256,
          [](auto row) { return "grouping-key-" + std::to_string(row % 11); })};
  expectReuseIsTransparent({VARCHAR(), VARCHAR()}, fanOut(columns, 6));
  EXPECT_GT(lastReusedRows_, 0);
}

TEST_F(GroupingKeyContributionsTest, fixedWidthKeysAreNotCached) {
  // Hashing a fixed-width key costs less than reading a cached contribution
  // back, so these are left alone.
  auto columns = std::vector<VectorPtr>{
      makeFlatVector<int64_t>(
          256,
          [](auto row) { return row % 7; },
          velox::test::VectorMaker::nullEvery(5)),
      makeFlatVector<int32_t>(256, [](auto row) { return row % 11; })};
  expectReuseIsTransparent({BIGINT(), INTEGER()}, fanOut(columns, 6));
  EXPECT_EQ(lastReusedRows_, 0);
}

TEST_F(GroupingKeyContributionsTest, hashedKeys) {
  // Wide and string keys leave value-ID mode, so the probes hash instead.
  auto columns = std::vector<VectorPtr>{
      makeFlatVector<int64_t>(
          512, [](auto row) { return int64_t(row) * 1'000'003; }),
      makeFlatVector<std::string>(
          512,
          [](auto row) { return "key-" + std::to_string(row); },
          velox::test::VectorMaker::nullEvery(9))};
  expectReuseIsTransparent({BIGINT(), VARCHAR()}, fanOut(columns, 5));
  EXPECT_GT(lastReusedRows_, 0);
}

TEST_F(GroupingKeyContributionsTest, dictionaryEncodedKeys) {
  auto base = makeFlatVector<std::string>(
      16, [](auto row) { return "base-value-" + std::to_string(row); });
  auto columns = std::vector<VectorPtr>{
      BaseVector::wrapInDictionary(
          makeNulls(256, velox::test::VectorMaker::nullEvery(9)),
          makeIndices(256, [](auto row) { return row % 16; }),
          256,
          base),
      makeFlatVector<int64_t>(256, [](auto row) { return row % 5; })};
  // A dictionary already reduces hashing to one call per distinct base
  // value, so there is nothing left for the cache to save.
  expectReuseIsTransparent({VARCHAR(), BIGINT()}, fanOut(columns, 4));
  EXPECT_EQ(lastReusedRows_, 0);
}

TEST_F(GroupingKeyContributionsTest, hashModeChangeMidStream) {
  // Every batch introduces keys the value-ID dictionary has not seen, so the
  // table changes hash mode partway through and reassigns the multipliers the
  // cached contributions were computed with.
  // String keys, so that contributions really are cached and the mode change
  // has something to invalidate.
  std::vector<RowVectorPtr> batches;
  for (int64_t batch = 0; batch < 32; ++batch) {
    auto columns = std::vector<VectorPtr>{
        makeFlatVector<std::string>(
            256,
            [&](auto row) {
              return "key-" + std::to_string(batch * 1'000'000 + row);
            }),
        makeFlatVector<std::string>(
            256, [&](auto row) { return "tail-" + std::to_string(row % 11); })};
    for (auto& fanned : fanOut(columns, 3)) {
      batches.push_back(fanned);
    }
  }
  expectReuseIsTransparent({VARCHAR(), VARCHAR()}, batches);
  EXPECT_GT(lastReusedRows_, 0);
}

TEST_F(GroupingKeyContributionsTest, newSourceVectorIsNotReused) {
  // Same contents, different vector: the cache must not serve the old one.
  std::vector<RowVectorPtr> batches;
  for (auto batch = 0; batch < 4; ++batch) {
    auto columns = std::vector<VectorPtr>{makeFlatVector<std::string>(
        64, [](auto row) { return "key-" + std::to_string(row % 7); })};
    for (auto& fanned : fanOut(columns, 2)) {
      batches.push_back(fanned);
    }
  }
  expectReuseIsTransparent({VARCHAR()}, batches);
}

TEST_F(GroupingKeyContributionsTest, budgetTooSmallSkipsCaching) {
  auto columns = std::vector<VectorPtr>{makeFlatVector<std::string>(
      256, [](auto row) { return "grouping-key-" + std::to_string(row % 7); })};
  expectReuseIsTransparent({VARCHAR()}, fanOut(columns, 4), /*maxBytes=*/0);
  EXPECT_EQ(lastReusedRows_, 0);
  EXPECT_GT(lastUncachedRows_, 0);
}

TEST_F(GroupingKeyContributionsTest, clearDropsEverything) {
  GroupingKeyContributions contributions(
      1, pool_.get(), GroupingKeyContributions::kDefaultMaxBytes);
  auto values = makeFlatVector<int64_t>(16, [](auto row) { return row; });
  SelectivityVector rows(values->size());

  auto* fill = contributions.prepareFill(0, rows);
  ASSERT_NE(fill, nullptr);
  EXPECT_EQ(fill->size(), values->size());
  EXPECT_TRUE(std::all_of(
      fill->begin(), fill->end(), [](uint64_t value) { return value == 0; }));
  contributions.publish(0, values, /*usesValueIds=*/true, rows);
  EXPECT_NE(contributions.find(0, *values, true, rows), nullptr);

  contributions.clear();
  EXPECT_EQ(contributions.find(0, *values, true, rows), nullptr);
}

TEST_F(GroupingKeyContributionsTest, modeAndRowsMustMatch) {
  GroupingKeyContributions contributions(
      1, pool_.get(), GroupingKeyContributions::kDefaultMaxBytes);
  auto values = makeFlatVector<int64_t>(16, [](auto row) { return row; });
  SelectivityVector half(values->size());
  for (auto row = 8; row < values->size(); ++row) {
    half.setValid(row, false);
  }
  half.updateBounds();

  ASSERT_NE(contributions.prepareFill(0, half), nullptr);
  contributions.publish(0, values, /*usesValueIds=*/true, half);
  EXPECT_NE(contributions.find(0, *values, true, half), nullptr);
  // A different mode never matches.
  EXPECT_EQ(contributions.find(0, *values, false, half), nullptr);
  // Nor do rows the cached buffer does not cover.
  SelectivityVector all(values->size());
  EXPECT_EQ(contributions.find(0, *values, true, all), nullptr);
}

} // namespace facebook::velox::exec::test
