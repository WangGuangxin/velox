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

#include "velox/common/base/SimdUtil.h"

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <array>
#include <cstring>
#include <random>

namespace facebook::velox::simd {
namespace {

template <typename T>
void checkMasksAndFilter() {
  using B = xsimd::batch<T>;
  constexpr auto N = B::size;
  std::array<T, N> input{};
  for (int i = 0; i < N; ++i) {
    input[i] = static_cast<T>(i * 13 - 47);
    if constexpr (std::is_floating_point_v<T>) {
      if (i == 0) {
        input[i] = std::numeric_limits<T>::quiet_NaN();
      } else if (i == 1) {
        input[i] = -T{0};
      } else if (i == 2) {
        input[i] = std::numeric_limits<T>::infinity();
      }
    }
  }
  std::mt19937 rng(42);
  auto check = [&](uint32_t mask) {
    std::array<bool, N> flags{};
    for (int i = 0; i < N; ++i) {
      flags[i] = (mask >> i) & 1;
    }
    auto predicate = xsimd::batch_bool<T>::load_unaligned(flags.data());
    EXPECT_EQ(static_cast<uint32_t>(toBitMask(predicate)), mask);
    if constexpr (sizeof(T) >= 2) {
      auto packed = filter(B::load_unaligned(input.data()), mask);
      std::array<T, N> actual;
      packed.store_unaligned(actual.data());
      int out = 0;
      for (int i = 0; i < N; ++i) {
        if (flags[i]) {
          EXPECT_EQ(std::memcmp(&actual[out++], &input[i], sizeof(T)), 0);
        }
      }
    }
  };
  const uint32_t full = static_cast<uint32_t>(bits::lowMask(N));
  check(0);
  check(full);
  for (unsigned i = 0; i < N; ++i) {
    check(uint32_t{1} << i);
    check(full ^ (uint32_t{1} << i));
  }
  if constexpr (N <= 16) {
    for (uint32_t m = 0; m <= full; ++m) {
      check(m);
    }
  } else {
    for (int i = 0; i < 4096; ++i) {
      check(rng() & full);
    }
  }
}

TEST(SimdUtilSveTest, MasksAndStableCompaction) {
  checkMasksAndFilter<int8_t>();
  checkMasksAndFilter<int16_t>();
  checkMasksAndFilter<int32_t>();
  checkMasksAndFilter<int64_t>();
  checkMasksAndFilter<float>();
  checkMasksAndFilter<double>();
}

template <typename T, typename I, int Scale>
void checkGather() {
  using B = xsimd::batch<T>;
  constexpr auto N = B::size;
  alignas(64) std::array<uint64_t, 512> storage;
  std::mt19937_64 rng(19);
  for (auto& value : storage) {
    value = rng();
  }
  const auto* base = reinterpret_cast<const T*>(storage.data() + 256);
  std::array<I, N> indices;
  std::array<T, N> expected;
  for (int i = 0; i < N; ++i) {
    indices[i] = (i % 2 ? -1 : 1) * (i + 1) * sizeof(T);
    std::memcpy(
        &expected[i],
        reinterpret_cast<const char*>(base) + indices[i] * Scale,
        sizeof(T));
  }
  auto loaded = gather<T, I, Scale>(base, indices.data());
  std::array<T, N> actual;
  loaded.store_unaligned(actual.data());
  EXPECT_EQ(std::memcmp(actual.data(), expected.data(), sizeof(actual)), 0);
  auto vindex = loadGatherIndices<T>(indices.data());
  loaded = gather<T, I, Scale>(base, vindex);
  loaded.store_unaligned(actual.data());
  EXPECT_EQ(std::memcmp(actual.data(), expected.data(), sizeof(actual)), 0);
  for (int count = 0; count <= N; ++count) {
    const auto mask = leadingMask<T>(count);
    const auto src = B(T{-17});
    auto result = maskGather<T, I, Scale>(src, mask, base, indices.data());
    result.store_unaligned(actual.data());
    for (int i = 0; i < N; ++i) {
      T value = i < count ? expected[i] : T{-17};
      EXPECT_EQ(std::memcmp(&actual[i], &value, sizeof(T)), 0);
    }
    result = maskGather<T, I, Scale>(src, mask, base, vindex);
    result.store_unaligned(actual.data());
    for (int i = 0; i < N; ++i) {
      T value = i < count ? expected[i] : T{-17};
      EXPECT_EQ(std::memcmp(&actual[i], &value, sizeof(T)), 0);
    }
  }
}

template <typename T, typename I>
void checkGatherScales() {
  checkGather<T, I, 1>();
  checkGather<T, I, 2>();
  checkGather<T, I, 4>();
  checkGather<T, I, 8>();
}

TEST(SimdUtilSveTest, GatherSignedIndicesAndByteScales) {
  checkGatherScales<int32_t, int32_t>();
  checkGatherScales<float, int32_t>();
  checkGatherScales<int64_t, int32_t>();
  checkGatherScales<double, int32_t>();
  checkGatherScales<int64_t, int64_t>();
  checkGatherScales<double, int64_t>();
}

template <typename T, typename I>
void checkMaskedGuardPage() {
  const auto page = sysconf(_SC_PAGESIZE);
  void* memory =
      mmap(nullptr, 3 * page, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  ASSERT_NE(memory, MAP_FAILED);
  auto* base = reinterpret_cast<T*>(static_cast<char*>(memory) + page);
  ASSERT_EQ(mprotect(base, page, PROT_READ | PROT_WRITE), 0);
  base[0] = T{37};
  constexpr auto N = xsimd::batch<T>::size;
  std::array<I, N> indices;
  indices.fill(-static_cast<I>(page / sizeof(T)));
  indices[0] = 0;
  auto result = maskGather<T, I>(
      xsimd::batch<T>(T{-17}), leadingMask<T>(1), base, indices.data());
  EXPECT_EQ(result.get(0), T{37});
  for (int i = 1; i < N; ++i) {
    EXPECT_EQ(result.get(i), T{-17});
  }
  // A half-batch index array ends exactly at a protected page boundary.
  auto* tail = reinterpret_cast<I*>(reinterpret_cast<char*>(base) + page) - N;
  std::fill(tail, tail + N, 0);
  result = gather<T, I>(base, loadGatherIndices<T>(tail));
  for (int i = 0; i < N; ++i) {
    EXPECT_EQ(result.get(i), T{37});
  }
  EXPECT_EQ(munmap(memory, 3 * page), 0);
}

TEST(SimdUtilSveTest, InactiveLanesAndHalfIndicesDoNotOverread) {
  checkMaskedGuardPage<int32_t, int32_t>();
  checkMaskedGuardPage<float, int32_t>();
  checkMaskedGuardPage<int64_t, int32_t>();
  checkMaskedGuardPage<double, int32_t>();
  checkMaskedGuardPage<int64_t, int64_t>();
  checkMaskedGuardPage<double, int64_t>();
}

TEST(SimdUtilSveTest, PackLow16Bits) {
  constexpr auto N = xsimd::batch<int32_t>::size;
  std::array<int32_t, N> x, y;
  for (int i = 0; i < N; ++i) {
    x[i] = i % 2 ? -1 - i : 0x12348000 + i;
    y[i] = i % 2 ? -65536 - i : 0x4321ffff - i;
  }
  auto packed = detail::pack32(
      xsimd::batch<int32_t>::load_unaligned(x.data()),
      xsimd::batch<int32_t>::load_unaligned(y.data()),
      xsimd::default_arch{});
  std::array<int16_t, 2 * N> actual;
  packed.store_unaligned(actual.data());
  for (int i = 0; i < N; ++i) {
    EXPECT_EQ(actual[i], static_cast<int16_t>(x[i]));
    EXPECT_EQ(actual[i + N], static_cast<int16_t>(y[i]));
  }
}

TEST(SimdUtilSveTest, WidenBothHalves) {
  constexpr auto N = xsimd::batch<int32_t>::size;
  std::array<int32_t, N> input;
  for (int i = 0; i < N; ++i) {
    input[i] = i % 2 ? std::numeric_limits<int32_t>::min() + i
                     : std::numeric_limits<int32_t>::max() - i;
  }
  const auto values = xsimd::batch<int32_t>::load_unaligned(input.data());
  const auto low = getHalf<int64_t, 0>(values);
  const auto high = getHalf<int64_t, 1>(values);
  const auto unsignedLow = getHalf<uint64_t, 0>(values);
  const auto unsignedHigh = getHalf<uint64_t, 1>(values);
  for (int i = 0; i < N / 2; ++i) {
    EXPECT_EQ(low.get(i), static_cast<int64_t>(input[i]));
    EXPECT_EQ(high.get(i), static_cast<int64_t>(input[i + N / 2]));
    EXPECT_EQ(unsignedLow.get(i), static_cast<uint32_t>(input[i]));
    EXPECT_EQ(unsignedHigh.get(i), static_cast<uint32_t>(input[i + N / 2]));
  }
}

#if XSIMD_WITH_SVE
template <typename T>
void checkMaskedLoadGuardPage() {
  const auto page = sysconf(_SC_PAGESIZE);
  auto* memory = static_cast<char*>(
      mmap(nullptr, 2 * page, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
  ASSERT_NE(memory, MAP_FAILED);
  ASSERT_EQ(mprotect(memory, page, PROT_READ | PROT_WRITE), 0);
  constexpr auto N = xsimd::batch<T>::size;
  for (int count = 0; count <= N; ++count) {
    auto* end = reinterpret_cast<T*>(memory + page);
    auto* input = end - count;
    std::fill(input, end, T{37});
    const auto result = maskLoad(input, leadingMask<T>(count));
    for (int i = 0; i < N; ++i) {
      EXPECT_EQ(result.get(i), i < count ? T{37} : T{0});
    }
  }
  EXPECT_EQ(munmap(memory, 2 * page), 0);
}

TEST(SimdUtilSveTest, MaskedLoadDoesNotTouchInactiveLanes) {
  checkMaskedLoadGuardPage<int32_t>();
  checkMaskedLoadGuardPage<int64_t>();
  checkMaskedLoadGuardPage<float>();
  checkMaskedLoadGuardPage<double>();
}
#endif

} // namespace
} // namespace facebook::velox::simd
