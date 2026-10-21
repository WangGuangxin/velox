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
#include "velox/exec/RowContainer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

namespace facebook::velox::exec::test {
class RowContainerBatchStoreTest : public testing::Test,
                                   public velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void compare(const VectorPtr& input) {
    RowContainer scalar({input->type()}, pool());
    RowContainer batched({input->type()}, pool());
    std::vector<char*> scalarRows;
    std::vector<char*> batchRows;
    SelectivityVector selected(input->size());
    DecodedVector decoded(*input, selected);
    for (auto row = 0; row < input->size(); ++row) {
      scalarRows.push_back(scalar.newRow());
      scalar.store(decoded, row, scalarRows.back(), 0);
      batchRows.push_back(batched.newRow());
    }
    batched.store(
        decoded, folly::Range<char**>(batchRows.data(), batchRows.size()), 0);
    auto expected = BaseVector::create(input->type(), input->size(), pool());
    auto actual = BaseVector::create(input->type(), input->size(), pool());
    scalar.extractColumn(scalarRows.data(), scalarRows.size(), 0, expected);
    batched.extractColumn(batchRows.data(), batchRows.size(), 0, actual);
    for (auto row = 0; row < input->size(); ++row) {
      EXPECT_TRUE(actual->equalValueAt(expected.get(), row, row));
      EXPECT_TRUE(actual->equalValueAt(input.get(), row, row));
    }
    const auto a = scalar.columnStats(0).value();
    const auto b = batched.columnStats(0).value();
    EXPECT_EQ(a.minBytes(), b.minBytes());
    EXPECT_EQ(a.maxBytes(), b.maxBytes());
    EXPECT_EQ(a.sumBytes(), b.sumBytes());
    EXPECT_EQ(a.nullCount(), b.nullCount());
    EXPECT_EQ(a.nonNullCount(), b.nonNullCount());
    batched.clear();
    for (auto row = 0; row < input->size(); ++row) {
      EXPECT_TRUE(actual->equalValueAt(expected.get(), row, row));
    }
  }
};

TEST_F(RowContainerBatchStoreTest, valuesNullsDictionariesAndComplexTypes) {
  constexpr int count = 129;
  std::vector<VectorPtr> inputs{
      makeFlatVector<int32_t>(count, [](auto row) { return row - 64; }),
      makeFlatVector<int64_t>(
          count, [](auto row) { return int64_t(row) << 32; }),
      makeFlatVector<double>(count, [](auto row) { return row / 3.0; }),
      makeFlatVector<int128_t>(
          count, [](auto row) { return int128_t(row) << 70; }),
      makeFlatVector<std::string>(
          count,
          [](auto row) {
            return row % 2 ? "inline" : std::string(row * 37 + 1, 'x');
          }),
      makeArrayVector<int64_t>(
          std::vector<std::vector<int64_t>>(count, {1, 2, 3})),
      makeRowVector(
          {makeFlatVector<int64_t>(count, [](auto row) { return row; })}),
  };
  for (auto input : inputs) {
    compare(input);
    input->setNull(0, true);
    input->setNull(23, true);
    compare(input);
    auto indices = makeIndices(count, [](auto row) { return 128 - row; });
    compare(BaseVector::wrapInDictionary(nullptr, indices, count, input));
  }
}

TEST_F(RowContainerBatchStoreTest, emptyAndAllNullColumns) {
  compare(makeFlatVector<int64_t>(std::vector<int64_t>{}));
  compare(makeNullableFlatVector<int64_t>(
      std::vector<std::optional<int64_t>>(129, std::nullopt)));
}

TEST_F(RowContainerBatchStoreTest, batchedStatisticsMatchIndividualCells) {
  RowColumn::Stats scalar;
  RowColumn::Stats batch;
  for (auto width : {0, 1, 4, 8, 16}) {
    batch.addFixedWidthCells(width, 137, 23);
    for (int i = 0; i < 137; ++i) {
      scalar.addCellSize(width);
    }
    for (int i = 0; i < 23; ++i) {
      scalar.addNullCell();
    }
    EXPECT_EQ(batch.minBytes(), scalar.minBytes());
    EXPECT_EQ(batch.maxBytes(), scalar.maxBytes());
    EXPECT_EQ(batch.sumBytes(), scalar.sumBytes());
    EXPECT_EQ(batch.nonNullCount(), scalar.nonNullCount());
    EXPECT_EQ(batch.nullCount(), scalar.nullCount());
  }
  batch.addFixedWidthCells(99, 0);
  EXPECT_EQ(batch.maxBytes(), scalar.maxBytes());
  batch.invalidateMinMaxColumnStats();
  batch.addFixedWidthCells(16, 7);
  EXPECT_FALSE(batch.minMaxColumnStatsValid());
  RowColumn::Stats large;
  large.addFixedWidthCells(16, 1U << 30);
  EXPECT_EQ(large.sumBytes(), 16ULL << 30);
}
} // namespace facebook::velox::exec::test
