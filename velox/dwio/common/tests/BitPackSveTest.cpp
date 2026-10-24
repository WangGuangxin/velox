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

#include "velox/dwio/common/BitPackDecoder.h"

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <numeric>
#include <random>

namespace facebook::velox::dwio::common {
namespace {

template <typename T>
void checkUnpack(uint8_t width) {
  // Put the exact encoded buffer at a guard-page boundary. Both vector
  // gathers and the scalar tail must respect bufferEnd.
  const auto page = sysconf(_SC_PAGESIZE);
  auto* memory = static_cast<char*>(
      mmap(nullptr, 2 * page, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
  ASSERT_NE(memory, MAP_FAILED);
  ASSERT_EQ(mprotect(memory, page, PROT_READ | PROT_WRITE), 0);
  constexpr int count = 257;
  std::mt19937_64 rng(43 + width);
  std::vector<uint64_t> values(count);
  for (auto& value : values) {
    value = rng() & bits::lowMask(width);
  }
  for (int offset = 0; offset < 8; ++offset) {
    auto bytes = bits::divRoundUp(count * width + offset, 8);
    auto* begin = memory + page - bytes;
    std::memset(begin, 0, bytes);
    for (int i = 0; i < count; ++i) {
      for (int bit = 0; bit < width; ++bit) {
        if ((values[i] >> bit) & 1) {
          bits::setBit(begin, offset + i * width + bit);
        }
      }
    }
    for (int step : {1, 2, 7}) {
      std::vector<int32_t> rows;
      for (int i = 0; i < count; i += step) {
        rows.push_back(i);
      }
      std::vector<T> result(rows.size());
      unpack(
          reinterpret_cast<const uint64_t*>(begin),
          offset,
          RowSet(rows),
          0,
          width,
          memory + page,
          result.data());
      for (int i = 0; i < rows.size(); ++i) {
        ASSERT_EQ(result[i], static_cast<T>(values[rows[i]]))
            << "width=" << int(width) << " offset=" << offset
            << " step=" << step << " row=" << rows[i];
      }
      // Preserve rowBias semantics when the buffer starts at a later row.
      for (auto& row : rows) {
        row += 19;
      }
      unpack(
          reinterpret_cast<const uint64_t*>(begin),
          offset + 19 * width,
          RowSet(rows),
          38,
          width,
          memory + page,
          result.data());
      for (int i = 0; i < rows.size(); ++i) {
        ASSERT_EQ(result[i], static_cast<T>(values[rows[i] - 19]));
      }
    }
  }
  EXPECT_EQ(munmap(memory, 2 * page), 0);
}

TEST(BitPackSveTest, DenseSparseBiasOffsetsAndGuardedTail) {
  for (int width = 0; width <= 24; ++width) {
    checkUnpack<int32_t>(width);
    checkUnpack<int64_t>(width);
  }
  for (int width : {25, 31, 32, 40, 56}) {
    checkUnpack<int64_t>(width);
  }
}

} // namespace
} // namespace facebook::velox::dwio::common
