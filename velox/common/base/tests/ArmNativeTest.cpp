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

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/SimdUtil.h"

#include <gtest/gtest.h>
#include <array>
#include <cstring>
#include <limits>

#if defined(__unix__)
#include <sys/mman.h>
#include <unistd.h>
#endif

using namespace facebook::velox;

namespace {

// Independent software CRC32C oracle, including truncation of the seed to
// 32 bits. Keeping the old hash composition checks compatibility with data
// produced by the unoptimized library, rather than just hash distribution.
uint64_t referenceHash(uint64_t seed, const char* data, size_t size) {
  auto word = [&](size_t offset, size_t count) {
    uint64_t value = 0;
    std::memcpy(&value, data + offset, count);
    return value;
  };
  auto crc = [](uint64_t initial, uint64_t value) -> uint64_t {
    uint32_t result = initial;
    for (int byte = 0; byte < 8; ++byte) {
      result ^= value & 255;
      value >>= 8;
      for (int bit = 0; bit < 8; ++bit) {
        result = (result >> 1) ^ ((result & 1) ? 0x82f63b78U : 0);
      }
    }
    return result;
  };
  if (size < 8) {
    const auto value = word(0, size);
    return crc(seed, value) | (crc(seed, value >> 32) << 32);
  }
  uint64_t accumulators[] = {seed, seed << 32, seed >> 16};
  size_t offset = 0;
  while (size - offset >= 24) {
    for (int i = 0; i < 3; ++i) {
      accumulators[i] = crc(accumulators[i], word(offset + i * 8, 8));
    }
    offset += 24;
  }
  for (int i = 0; offset < size; ++i) {
    const auto count = std::min<size_t>(8, size - offset);
    accumulators[i] = crc(accumulators[i], word(offset, count));
    offset += count;
  }
  constexpr uint64_t kMul = 0x9ddfea08eb382d69ULL;
  return accumulators[0] ^ (accumulators[1] * kMul) ^ (accumulators[2] * kMul);
}

TEST(ArmNativeTest, hashCompatibilityAndUnalignedInputs) {
  std::array<char, 1100> data;
  for (size_t i = 0; i < data.size(); ++i) {
    data[i] = static_cast<char>(i * 73 + 19);
  }
  for (uint64_t seed : std::array<uint64_t, 3>{0, 1, 0xfedcba9876543210ULL}) {
    for (int offset = 0; offset < 16; ++offset) {
      for (size_t size = 0; size <= 1024; ++size) {
        ASSERT_EQ(
            bits::hashBytes(seed, data.data() + offset, size),
            referenceHash(seed, data.data() + offset, size))
            << "seed=" << seed << " offset=" << offset << " size=" << size;
      }
    }
  }
}

template <typename T>
void testGather() {
  constexpr int kLanes = xsimd::batch<T>::size;
  std::array<T, 64> data;
  for (size_t i = 0; i < sizeof(data); ++i) {
    reinterpret_cast<uint8_t*>(data.data())[i] = i * 73 + 19;
  }
  if constexpr (std::is_same_v<T, float>) {
    const std::array<uint32_t, 8> patterns{
        0x7fc12345,
        0x7fa12345,
        0xffc00007,
        0x80000000,
        0x7f800000,
        0xff800000,
        1,
        0};
    for (size_t i = 0; i < data.size(); ++i) {
      std::memcpy(&data[i], &patterns[i % patterns.size()], sizeof(T));
    }
  } else if constexpr (std::is_same_v<T, double>) {
    const std::array<uint64_t, 8> patterns{
        0x7ff8000000001234ULL,
        0x7ff0000000001234ULL,
        0xfff8000000000007ULL,
        0x8000000000000000ULL,
        0x7ff0000000000000ULL,
        0xfff0000000000000ULL,
        1,
        0};
    for (size_t i = 0; i < data.size(); ++i) {
      std::memcpy(&data[i], &patterns[i % patterns.size()], sizeof(T));
    }
  }
  const auto* base = data.data() + 32;
  std::array<int32_t, kLanes> indices;
  std::array<T, kLanes> result;
  for (int i = 0; i < kLanes; ++i) {
    indices[i] = i % 2 ? i : -i - 1;
  }
  const auto indexVector = simd::loadGatherIndices<T, int32_t>(indices.data());
  for (bool vectorIndices : {false, true}) {
    const auto values = vectorIndices ? simd::gather(base, indexVector)
                                      : simd::gather(base, indices.data());
    values.store_unaligned(result.data());
    for (int i = 0; i < kLanes; ++i) {
      EXPECT_EQ(std::memcmp(&result[i], &base[indices[i]], sizeof(T)), 0);
    }
  }
  for (int active = 0; active <= kLanes; ++active) {
    for (int i = 0; i < kLanes; ++i) {
      indices[i] = i < active ? -i - 1 : std::numeric_limits<int32_t>::min();
    }
    const auto source = xsimd::batch<T>::load_unaligned(base);
    using Signed = std::conditional_t<sizeof(T) == 4, int32_t, int64_t>;
    const auto mask =
        xsimd::batch_bool<T>(simd::leadingMask<Signed>(active).data);
    const auto vector = simd::loadGatherIndices<T, int32_t>(indices.data());
    for (bool vectorIndices : {false, true}) {
      const auto values = vectorIndices
          ? simd::maskGather(source, mask, base, vector)
          : simd::maskGather(source, mask, base, indices.data());
      values.store_unaligned(result.data());
      for (int i = 0; i < kLanes; ++i) {
        const T* expected = i < active ? &base[indices[i]] : &base[i];
        EXPECT_EQ(std::memcmp(&result[i], expected, sizeof(T)), 0);
      }
    }
  }
}

TEST(ArmNativeTest, gatherBitsSignedIndicesAndMasks) {
  testGather<int32_t>();
  testGather<uint32_t>();
  testGather<float>();
  testGather<int64_t>();
  testGather<uint64_t>();
  testGather<double>();
}

TEST(ArmNativeTest, gather64BitIndices) {
  constexpr int kLanes = xsimd::batch<int64_t>::size;
  std::array<int64_t, 64> data;
  for (int i = 0; i < data.size(); ++i) {
    data[i] = i * 71;
  }
  const auto* base = data.data() + 32;
  std::array<int64_t, kLanes> indices;
  std::array<int64_t, kLanes> result;
  for (int active = 0; active <= kLanes; ++active) {
    for (int i = 0; i < kLanes; ++i) {
      indices[i] = i < active ? -i - 1 : std::numeric_limits<int64_t>::max();
    }
    const auto mask = simd::leadingMask<int64_t>(active);
    const auto source = xsimd::batch<int64_t>::broadcast(-1);
    simd::maskGather(source, mask, base, indices.data())
        .store_unaligned(result.data());
    for (int i = 0; i < kLanes; ++i) {
      EXPECT_EQ(result[i], i < active ? base[indices[i]] : -1);
    }
    const auto vector = xsimd::batch<int64_t>::load_unaligned(indices.data());
    simd::maskGather(source, mask, base, vector).store_unaligned(result.data());
    for (int i = 0; i < kLanes; ++i) {
      EXPECT_EQ(result[i], i < active ? base[indices[i]] : -1);
    }
    if (active == kLanes) {
      simd::gather(base, vector).store_unaligned(result.data());
      for (int i = 0; i < kLanes; ++i) {
        EXPECT_EQ(result[i], base[indices[i]]);
      }
      simd::gather(base, indices.data()).store_unaligned(result.data());
      for (int i = 0; i < kLanes; ++i) {
        EXPECT_EQ(result[i], base[indices[i]]);
      }
    }
  }
}

TEST(ArmNativeTest, nonNaturalGatherScaleFallsBack) {
  alignas(64) std::array<int64_t, 64> data;
  for (int i = 0; i < data.size(); ++i) {
    data[i] = i * 71;
  }
  constexpr int kLanes = xsimd::batch<int64_t>::size;
  std::array<int32_t, kLanes> byteOffsets;
  std::array<int64_t, kLanes> output;
  const auto* base = data.data() + 32;
  for (int i = 0; i < kLanes; ++i) {
    byteOffsets[i] = (-i - 1) * sizeof(int64_t);
  }
  simd::gather<int64_t, int32_t, 1>(base, byteOffsets.data())
      .store_unaligned(output.data());
  for (int i = 0; i < kLanes; ++i) {
    EXPECT_EQ(output[i], base[-i - 1]);
  }
}

#if defined(__unix__)
TEST(ArmNativeTest, guardPageTailsAndInactiveIndices) {
  const auto page = sysconf(_SC_PAGESIZE);
  auto* mapping = static_cast<char*>(mmap(
      nullptr,
      2 * page,
      PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_ANONYMOUS,
      -1,
      0));
  ASSERT_NE(mapping, MAP_FAILED);
  if (mprotect(mapping + page, page, PROT_NONE) != 0) {
    munmap(mapping, 2 * page);
    FAIL() << "mprotect failed";
  }
  for (int i = 0; i < page; ++i) {
    mapping[i] = static_cast<char>(i * 19);
  }
  for (int size = 0; size <= 128; ++size) {
    const auto* data = mapping + page - size;
    EXPECT_EQ(bits::hashBytes(17, data, size), referenceHash(17, data, size));
  }
  const auto source = xsimd::batch<int64_t>::broadcast(123);
  const auto source32 = xsimd::batch<int32_t>::broadcast(123);
  const auto values32 = simd::maskGather(
      source32,
      simd::leadingMask<int32_t>(0),
      reinterpret_cast<const int32_t*>(mapping + page),
      reinterpret_cast<const int32_t*>(mapping + page));
  for (int i = 0; i < values32.size; ++i) {
    EXPECT_EQ(values32.get(i), 123);
  }
  auto values = simd::maskGather(
      source,
      simd::leadingMask<int64_t>(0),
      reinterpret_cast<const int64_t*>(mapping + page),
      reinterpret_cast<const int32_t*>(mapping + page));
  for (int i = 0; i < values.size; ++i) {
    EXPECT_EQ(values.get(i), 123);
  }
  // A widening gather needs only one int32 index per int64 result lane.
  constexpr int kLanes = xsimd::batch<int64_t>::size;
  auto* indices = reinterpret_cast<int32_t*>(mapping + page) - kLanes;
  std::fill(indices, indices + kLanes, 0);
  const int64_t data = 42;
  values = simd::gather(&data, indices);
  for (int i = 0; i < kLanes; ++i) {
    EXPECT_EQ(values.get(i), 42);
  }
  EXPECT_EQ(munmap(mapping, 2 * page), 0);
}
#endif

} // namespace
