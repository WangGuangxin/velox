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
#include <algorithm>
#include <cstring>

namespace facebook::velox::exec::test {
using velox::test::assertEqualVectors;

class RowContainerBatchStringTest : public testing::Test,
                                    public velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  std::unique_ptr<RowContainer> container(const TypePtr& type, bool nullable) {
    return std::make_unique<RowContainer>(
        std::vector<TypePtr>{type},
        nullable,
        std::vector<Accumulator>{},
        std::vector<TypePtr>{},
        false,
        false,
        false,
        false,
        false,
        false,
        pool_.get());
  }

  VectorPtr output(const TypePtr& type, vector_size_t size) {
    auto result =
        BaseVector::create<FlatVector<StringView>>(type, size, pool_.get());
    for (vector_size_t i = 0; i < size; ++i) {
      result->set(i, StringView("untouched-output-value"));
      if (i % 3 == 0) {
        result->setNull(i, true);
      }
    }
    return result;
  }
};

TEST_F(RowContainerBatchStringTest, offsetsSelectionsNullsAndOwnership) {
  gflags::FlagSaver savedFlags;
  std::vector<VectorPtr> legacyResults, batchResults;
  for (const auto& type : std::vector<TypePtr>{VARCHAR(), VARBINARY()}) {
    for (bool nullable : {false, true}) {
      for (const vector_size_t count : {0, 1, 31, 32, 33, 127, 128, 129, 513}) {
        SCOPED_TRACE(fmt::format(
            "{} nullable={} count={}", type->toString(), nullable, count));
        auto data = container(type, nullable);
        auto input = BaseVector::create<FlatVector<StringView>>(
            type, count, pool_.get());
        const int lengths[] = {0, 3, 12, 13, 32, 127, 1024};
        for (vector_size_t i = 0; i < count; ++i) {
          std::string bytes(lengths[i % 7], static_cast<char>('a' + i % 20));
          if (bytes.size() > 1) {
            bytes[1] = '\0';
          }
          input->set(i, StringView(bytes));
          if (nullable && i % 7 == 0) {
            input->setNull(i, true);
          }
        }
        SelectivityVector all(count);
        DecodedVector decoded(*input, all);
        std::vector<char*> rows(count);
        for (vector_size_t i = 0; i < count; ++i) {
          rows[i] = data->newRow();
          data->store(decoded, i, rows[i], 0);
        }
        // Null row pointers model an unmatched outer-join build row, including
        // columns that themselves have no null flag.
        for (vector_size_t i = 5; i < count; i += 11) {
          rows[i] = nullptr;
        }
        for (bool indexed : {false, true}) {
          std::vector<vector_size_t> selection(count);
          for (vector_size_t i = 0; i < count; ++i) {
            selection[i] = i % 9 == 0 ? -1 : (count - 1 - i) / 2;
          }
          auto legacy = output(type, count + 9);
          auto batch = output(type, count + 9);
          auto alias = batch->as<FlatVector<StringView>>()->values();
          const auto* aliasValues = alias->as<StringView>();
          auto extract = [&](VectorPtr& result, bool enabled) {
            FLAGS_velox_row_container_batch_string_extraction_enabled = enabled;
            if (indexed) {
              RowContainer::extractColumn(
                  rows.data(),
                  folly::Range<const vector_size_t*>(
                      selection.data(), selection.size()),
                  data->columnAt(0),
                  data->columnHasNulls(0),
                  4,
                  result);
            } else {
              RowContainer::extractColumn(
                  rows.data(),
                  count,
                  data->columnAt(0),
                  data->columnHasNulls(0),
                  4,
                  result);
            }
          };
          extract(legacy, false);
          extract(batch, true);
          assertEqualVectors(legacy, batch);
          // mutableRawValues must preserve any shared copy of the old output.
          if (count > 0) {
            EXPECT_EQ(aliasValues[4], StringView("untouched-output-value"));
          }
          legacyResults.push_back(legacy);
          batchResults.push_back(batch);
          // Reuse with a bitmap containing nulls from the previous extraction.
          extract(legacy, false);
          extract(batch, true);
          assertEqualVectors(legacy, batch);
        }
        data.reset();
        input.reset();
      }
    }
  }
  // Every source container and source vector has been destroyed. All string
  // payloads in the optimized output must still be independently owned.
  for (size_t i = 0; i < legacyResults.size(); ++i) {
    assertEqualVectors(legacyResults[i], batchResults[i]);
  }
}

TEST_F(RowContainerBatchStringTest, fragmentedAndLargeBinaryValues) {
  gflags::FlagSaver savedFlags;
  constexpr vector_size_t kCount = 257;
  auto data = container(VARBINARY(), false);
  auto input = BaseVector::create<FlatVector<StringView>>(
      VARBINARY(), kCount, pool_.get());
  for (vector_size_t i = 0; i < kCount; ++i) {
    input->set(i, StringView("inline"));
  }
  SelectivityVector all(kCount);
  DecodedVector decoded(*input, all);
  std::vector<char*> rows(kCount);
  for (vector_size_t i = 0; i < kCount; ++i) {
    rows[i] = data->newRow();
    data->store(decoded, i, rows[i], 0);
  }
  std::string large(200003, 'x');
  large[0] = '\0';
  large[100000] = '\0';
  large.back() = 'z';
  ByteOutputStream stream(&data->stringAllocator(), false, false);
  auto position = data->stringAllocator().newWrite(stream, 32);
  stream.appendStringView(StringView(large));
  data->stringAllocator().finishWrite(stream, 0);
  StringView fragmented(position.position, large.size());
  ASSERT_LT(
      HashStringAllocator::headerOf(fragmented.data())->size(),
      fragmented.size());
  std::memcpy(
      rows[0] + data->columnAt(0).offset(), &fragmented, sizeof(fragmented));
  for (vector_size_t i = 0; i < kCount; i += 17) {
    rows[i] = rows[0];
  }
  auto legacy = output(VARBINARY(), kCount);
  auto batch = output(VARBINARY(), kCount);
  FLAGS_velox_row_container_batch_string_extraction_enabled = false;
  data->extractColumn(rows.data(), kCount, 0, legacy);
  FLAGS_velox_row_container_batch_string_extraction_enabled = true;
  data->extractColumn(rows.data(), kCount, 0, batch);
  assertEqualVectors(legacy, batch);
  data.reset();
  input.reset();
  assertEqualVectors(legacy, batch);
  EXPECT_EQ(batch->as<FlatVector<StringView>>()->valueAt(0), StringView(large));
}

TEST_F(RowContainerBatchStringTest, allNullRowsAndInlineOnly) {
  gflags::FlagSaver savedFlags;
  constexpr vector_size_t kCount = 256;
  auto data = container(VARCHAR(), false);
  std::vector<char*> rows(kCount, nullptr);
  auto result = output(VARCHAR(), kCount);
  FLAGS_velox_row_container_batch_string_extraction_enabled = true;
  data->extractColumn(rows.data(), kCount, 0, result);
  for (vector_size_t i = 0; i < kCount; ++i) {
    EXPECT_TRUE(result->isNullAt(i));
  }
  auto input =
      BaseVector::create<FlatVector<StringView>>(VARCHAR(), 1, pool_.get());
  input->set(0, StringView("inline"));
  SelectivityVector all(1);
  DecodedVector decoded(*input, all);
  auto* row = data->newRow();
  data->store(decoded, 0, row, 0);
  std::fill(rows.begin(), rows.end(), row);
  data->extractColumn(rows.data(), kCount, 0, result);
  for (vector_size_t i = 0; i < kCount; ++i) {
    EXPECT_FALSE(result->isNullAt(i));
    EXPECT_EQ(
        result->as<FlatVector<StringView>>()->valueAt(i), StringView("inline"));
  }
}

TEST_F(RowContainerBatchStringTest, freshNullBitmapAndSharedOutputBuffers) {
  gflags::FlagSaver savedFlags;
  constexpr vector_size_t kCount = 256;
  auto data = container(VARCHAR(), false);
  auto input =
      BaseVector::create<FlatVector<StringView>>(VARCHAR(), 1, pool_.get());
  input->set(0, StringView("a long source string for materialization"));
  SelectivityVector all(1);
  DecodedVector decoded(*input, all);
  auto* row = data->newRow();
  data->store(decoded, 0, row, 0);
  std::vector<char*> rows(kCount, row);
  rows[17] = nullptr;
  VectorPtr result = BaseVector::create<FlatVector<StringView>>(
      VARCHAR(), kCount, pool_.get());
  EXPECT_FALSE(result->nulls());
  FLAGS_velox_row_container_batch_string_extraction_enabled = true;
  data->extractColumn(rows.data(), kCount, 0, result);
  EXPECT_TRUE(result->isNullAt(17));
  EXPECT_FALSE(result->isNullAt(16));

  auto* flat = result->as<FlatVector<StringView>>();
  VectorPtr alias = std::make_shared<FlatVector<StringView>>(
      pool_.get(),
      VARCHAR(),
      result->nulls(),
      kCount,
      flat->values(),
      std::vector<BufferPtr>(flat->stringBuffers()));
  input->set(0, StringView("different long payload for the reused output"));
  data->store(decoded, 0, row, 0);
  rows[17] = row;
  rows[18] = nullptr;
  data->extractColumn(rows.data(), kCount, 0, result);
  data.reset();
  input.reset();
  EXPECT_TRUE(alias->isNullAt(17));
  EXPECT_FALSE(alias->isNullAt(18));
  EXPECT_EQ(
      alias->as<FlatVector<StringView>>()->valueAt(18),
      StringView("a long source string for materialization"));
  EXPECT_FALSE(result->isNullAt(17));
  EXPECT_TRUE(result->isNullAt(18));
  EXPECT_EQ(
      flat->valueAt(17),
      StringView("different long payload for the reused output"));
}
} // namespace facebook::velox::exec::test
