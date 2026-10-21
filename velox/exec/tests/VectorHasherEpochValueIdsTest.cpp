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
#include <gtest/gtest.h>
#include <algorithm>
#include <vector>
#include "velox/exec/VectorHasher.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::exec::test {
class VectorHasherEpochValueIdsTest : public testing::Test,
                                      public velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }
  void configure(VectorHasher& hasher, bool reuse, uint64_t bytes = 16 << 20) {
    hasher.configureGroupingSetReuse(false, reuse, bytes, 500000);
    EXPECT_EQ(hasher.enableValueIds(1, VectorHasher::kNoLimit), 500001);
  }
  std::pair<bool, std::vector<uint64_t>> run(
      VectorHasher& hasher,
      const VectorPtr& values,
      const SelectivityVector& rows,
      uint64_t epoch,
      uint64_t initial = 0) {
    hasher.setGroupingSetHashInput(epoch, values, pool());
    hasher.decode(*values, rows);
    raw_vector<uint64_t> ids(pool());
    ids.resize(values->size());
    std::fill(ids.begin(), ids.end(), initial);
    const bool ok = hasher.computeValueIds(rows, ids);
    return {ok, std::vector<uint64_t>(ids.begin(), ids.end())};
  }
  VectorPtr strings(const TypePtr& type, int size = 129) {
    return makeFlatVector<std::string>(
        size,
        [](auto row) {
          return std::string("long\0grouping-key-", 18) +
              std::to_string(row % 31);
        },
        [](auto row) { return row % 11 == 0; },
        type);
  }
};

TEST_F(VectorHasherEpochValueIdsTest, ExactIdsMultiplierAndNullProjections) {
  for (const auto& type : std::vector<TypePtr>{VARCHAR(), VARBINARY()}) {
    auto values = strings(type);
    SelectivityVector rows(values->size());
    VectorHasher cached(type, 0), legacy(type, 0);
    configure(cached, true);
    configure(legacy, false);
    auto first = run(cached, values, rows, 1);
    EXPECT_TRUE(first.first);
    EXPECT_EQ(first, run(legacy, values, rows, 1));
    auto stats = cached.takeGroupingSetValueIdStats();
    EXPECT_GT(stats.valueIdEpochLookups, 0);
    EXPECT_EQ(stats.valueIdEpochReusedRows, 0);
    EXPECT_EQ(cached.enableValueIds(7, VectorHasher::kNoLimit), 7 * 500001);
    legacy.enableValueIds(7, VectorHasher::kNoLimit);
    EXPECT_EQ(
        run(cached, values, rows, 1, 43), run(legacy, values, rows, 1, 43));
    stats = cached.takeGroupingSetValueIdStats();
    EXPECT_EQ(stats.valueIdEpochLookups, 0);
    EXPECT_EQ(stats.valueIdEpochReusedRows, values->size());
    auto nulls = BaseVector::createNullConstant(type, values->size(), pool());
    EXPECT_EQ(run(cached, nulls, rows, 1, 43), run(legacy, nulls, rows, 1, 43));
    EXPECT_EQ(
        run(cached, values, rows, 1, 43), run(legacy, values, rows, 1, 43));
    stats = cached.takeGroupingSetValueIdStats();
    EXPECT_EQ(stats.valueIdEpochLookups, 0);
    EXPECT_EQ(stats.valueIdEpochReusedRows, values->size());
  }
}

TEST_F(VectorHasherEpochValueIdsTest, SparseSelectionsDictionariesAndNewEpoch) {
  auto type = VARCHAR();
  auto values = strings(type);
  auto indices =
      makeIndices(values->size(), [](auto row) { return 128 - row; });
  auto dictionary =
      BaseVector::wrapInDictionary(nullptr, indices, values->size(), values);
  SelectivityVector sparse(values->size(), false), all(values->size());
  for (int i = 1; i < values->size(); i += 3)
    sparse.setValid(i, true);
  sparse.updateBounds();
  VectorHasher cached(type, 0), legacy(type, 0);
  configure(cached, true);
  configure(legacy, false);
  EXPECT_EQ(
      run(cached, dictionary, sparse, 1, 23),
      run(legacy, dictionary, sparse, 1, 23));
  EXPECT_EQ(run(cached, dictionary, all, 1), run(legacy, dictionary, all, 1));
  cached.takeGroupingSetValueIdStats();
  EXPECT_EQ(run(cached, dictionary, all, 1), run(legacy, dictionary, all, 1));
  EXPECT_EQ(cached.takeGroupingSetValueIdStats().valueIdEpochLookups, 0);
  auto flat = values->asFlatVector<StringView>();
  flat->set(0, StringView("a-different-long-value"));
  flat->setNull(0, false);
  EXPECT_EQ(run(cached, dictionary, all, 2), run(legacy, dictionary, all, 2));
  EXPECT_GT(cached.takeGroupingSetValueIdStats().valueIdEpochLookups, 0);
  EXPECT_EQ(run(cached, values, all, 2), run(legacy, values, all, 2));
}

TEST_F(
    VectorHasherEpochValueIdsTest,
    ResetDeserializeAndDecodeInvalidateCachedIds) {
  auto values = strings(VARCHAR());
  SelectivityVector rows(values->size());
  VectorHasher cached(VARCHAR(), 0), legacy(VARCHAR(), 0);
  configure(cached, true);
  configure(legacy, false);
  EXPECT_EQ(run(cached, values, rows, 1), run(legacy, values, rows, 1));
  cached.resetStats();
  legacy.resetStats();
  cached.finishGroupingSetHashInput();
  auto reverse = BaseVector::wrapInDictionary(
      nullptr,
      makeIndices(values->size(), [](auto row) { return 128 - row; }),
      values->size(),
      values);
  // Seed a different ID order without replacing the retained epoch source.
  raw_vector<uint64_t> scratch(pool());
  scratch.resize(values->size());
  cached.decode(*reverse, rows);
  EXPECT_TRUE(cached.computeValueIds(rows, scratch));
  legacy.decode(*reverse, rows);
  EXPECT_TRUE(legacy.computeValueIds(rows, scratch));
  EXPECT_EQ(run(cached, values, rows, 1), run(legacy, values, rows, 1));
  EXPECT_GT(cached.takeGroupingSetValueIdStats().valueIdEpochInvalidations, 0);
  const auto state = legacy.serializeState();
  cached.deserializeState(state);
  EXPECT_EQ(run(cached, values, rows, 1), run(legacy, values, rows, 1));
  auto other = makeFlatVector<std::string>(values->size(), [](auto row) {
    return "a-new-input-value-" + std::to_string(row % 13);
  });
  cached.decode(*other, rows);
  legacy.decode(*other, rows);
  raw_vector<uint64_t> a(pool()), b(pool());
  a.resize(values->size());
  b.resize(values->size());
  EXPECT_EQ(cached.computeValueIds(rows, a), legacy.computeValueIds(rows, b));
  EXPECT_TRUE(std::equal(a.begin(), a.end(), b.begin()));
}

TEST_F(
    VectorHasherEpochValueIdsTest,
    OperatorBudgetAndV3StateRestoreKeepNumericLimits) {
  constexpr int count = 120000;
  auto values = makeFlatVector<std::string>(count, [](auto row) {
    return std::string(64, 'x') + std::to_string(row);
  });
  SelectivityVector rows(count);
  VectorHasher cached(VARCHAR(), 0), legacy(VARCHAR(), 0);
  configure(cached, true);
  configure(legacy, false);
  auto expected = run(legacy, values, rows, 1);
  ASSERT_TRUE(expected.first);
  EXPECT_EQ(run(cached, values, rows, 1), expected);
  const auto state = cached.serializeState();
  EXPECT_EQ(state.size(), cached.serializedStateSize());
  VectorHasher restored(VARCHAR(), 0);
  restored.deserializeState(state);
  EXPECT_EQ(restored.serializeState(), state);
  EXPECT_EQ(run(restored, values, rows, 1), expected);
  VectorHasher numeric(BIGINT(), 0), binary(VARBINARY(), 0);
  numeric.configureGroupingSetReuse(false, true, 16 << 20, 500000);
  binary.configureGroupingSetReuse(false, true, 16 << 20, 500000);
  EXPECT_EQ(numeric.enableValueIds(1, VectorHasher::kNoLimit), 100001);
  EXPECT_EQ(binary.enableValueIds(1, VectorHasher::kNoLimit), 500001);
}

TEST_F(VectorHasherEpochValueIdsTest, OverflowNeverReusesClearedDictionary) {
  auto values = makeFlatVector<std::string>(4096, [](auto row) {
    return std::to_string(row) + std::string(1024, 's');
  });
  SelectivityVector rows(values->size());
  VectorHasher cached(VARCHAR(), 0), legacy(VARCHAR(), 0);
  configure(cached, true, 1 << 20);
  configure(legacy, false, 1 << 20);
  EXPECT_FALSE(run(cached, values, rows, 1).first);
  EXPECT_FALSE(run(legacy, values, rows, 1).first);
  EXPECT_FALSE(cached.mayUseValueIds());
  EXPECT_EQ(cached.serializeState(), legacy.serializeState());
  EXPECT_GT(cached.takeGroupingSetValueIdStats().valueIdEpochInvalidations, 0);
}
} // namespace facebook::velox::exec::test
