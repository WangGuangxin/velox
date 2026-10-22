/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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

#include "velox/common/base/ArmSveNeonGather.h"

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>
#include <xsimd/xsimd.hpp>
#include <array>
#include <atomic>
#include <cstring>
#include <limits>
#include <random>
#include <stdexcept>
#include <thread>
#include <vector>

namespace facebook::velox::simd {
#if VELOX_SVE_NEON_GATHER_AVAILABLE
namespace {

// The production xsimd ABI remains NEON, even though these helpers use SVE.
static_assert(xsimd::batch<int32_t>::size == 4);
static_assert(xsimd::batch<int64_t>::size == 2);
static_assert(sizeof(xsimd::batch<float>) == 16);
static_assert(sizeof(xsimd::batch<double>) == 16);

template <typename T>
constexpr int kLanes = 16 / sizeof(T);

template <typename T>
using Values = std::array<T, kLanes<T>>;

template <typename T, int kScale>
Values<T> gather(const T* base, const int32_t* indices) {
  Values<T> out;
  if constexpr (sizeof(T) == 4) {
    auto result = arm_detail::gather32<T, kScale>(base, vld1q_s32(indices));
    std::memcpy(out.data(), &result, 16);
  } else {
    auto result = arm_detail::gather64<T, kScale>(base, vld1_s32(indices));
    std::memcpy(out.data(), &result, 16);
  }
  return out;
}

template <typename T, int kScale>
Values<T> maskGather(
    const T* base,
    const int32_t* indices,
    const Values<T>& source,
    int mask) {
  Values<T> out;
  if constexpr (sizeof(T) == 4) {
    arm_detail::Neon32<T> original;
    std::memcpy(&original, source.data(), 16);
    std::array<uint32_t, 4> enabled;
    for (int i = 0; i < 4; ++i) {
      enabled[i] = mask & (1 << i) ? ~uint32_t{0} : 0;
    }
    auto result = arm_detail::maskGather32<T, kScale>(
        original, vld1q_u32(enabled.data()), base, vld1q_s32(indices));
    std::memcpy(out.data(), &result, 16);
  } else {
    arm_detail::Neon64<T> original;
    std::memcpy(&original, source.data(), 16);
    std::array<uint64_t, 2> enabled;
    for (int i = 0; i < 2; ++i) {
      enabled[i] = mask & (1 << i) ? ~uint64_t{0} : 0;
    }
    auto result = arm_detail::maskGather64<T, kScale>(
        original, vld1q_u64(enabled.data()), base, vld1_s32(indices));
    std::memcpy(out.data(), &result, 16);
  }
  return out;
}

template <typename T, int kScale>
void checkValues() {
  alignas(64) std::array<uint8_t, 2048> data;
  for (size_t i = 0; i < data.size(); ++i) {
    data[i] = static_cast<uint8_t>((i * 79) ^ (i >> 3));
  }
  Values<T> source;
  // Include signed zero, NaN payload bits and high-bit integers without any
  // floating point arithmetic or equality comparisons.
  const std::array<uint64_t, 2> original{
      0x7ff0000000000001ULL, 0x800000007f800001ULL};
  std::memcpy(source.data(), original.data(), 16);
  for (int alignment = 0; alignment < 16; ++alignment) {
    const auto* base =
        reinterpret_cast<const T*>(data.data() + 1024 + alignment);
    std::array<int32_t, kLanes<T>> indices;
    for (int round = 0; round < 32; ++round) {
      for (int i = 0; i < kLanes<T>; ++i) {
        indices[i] = (round * 17 + i * 31) % 65 - 32;
      }
      for (int mask = 0; mask < (1 << kLanes<T>); ++mask) {
        auto expected = source;
        for (int i = 0; i < kLanes<T>; ++i) {
          if (mask & (1 << i)) {
            std::memcpy(
                &expected[i],
                reinterpret_cast<const char*>(base) +
                    static_cast<int64_t>(indices[i]) * kScale,
                sizeof(T));
          }
        }
        const auto actual =
            maskGather<T, kScale>(base, indices.data(), source, mask);
        ASSERT_EQ(std::memcmp(actual.data(), expected.data(), 16), 0)
            << "width=" << sizeof(T) << " scale=" << kScale << " mask=" << mask;
      }
      const auto actual = gather<T, kScale>(base, indices.data());
      const auto expected = maskGather<T, kScale>(
          base, indices.data(), source, (1 << kLanes<T>)-1);
      ASSERT_EQ(std::memcmp(actual.data(), expected.data(), 16), 0);
    }
  }
}

template <typename T>
void checkAllScales() {
  checkValues<T, 1>();
  checkValues<T, 2>();
  checkValues<T, 4>();
  checkValues<T, 8>();
}

TEST(ArmSveNeonGather, AllWidthsScalesMasksAndBitPatternsMatchScalar) {
  checkAllScales<int32_t>();
  checkAllScales<uint32_t>();
  checkAllScales<float>();
  checkAllScales<int64_t>();
  checkAllScales<uint64_t>();
  checkAllScales<double>();
}

template <typename T, int kScale>
void checkGuardPage() {
  const size_t page = sysconf(_SC_PAGESIZE);
  auto* memory = static_cast<uint8_t*>(
      mmap(nullptr, 3 * page, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
  ASSERT_NE(memory, MAP_FAILED);
  struct Cleanup {
    uint8_t* memory;
    size_t bytes;
    ~Cleanup() {
      munmap(memory, bytes);
    }
  } cleanup{memory, 3 * page};
  ASSERT_EQ(mprotect(memory + page, page, PROT_READ | PROT_WRITE), 0);
  std::memset(memory + page, 0xa7, page);
  Values<T> source;
  std::memset(source.data(), 0xc9, 16);
  for (int mask = 0; mask < (1 << kLanes<T>); ++mask) {
    const auto* base =
        reinterpret_cast<const T*>(memory + 2 * page - (mask ? sizeof(T) : 0));
    std::array<int32_t, kLanes<T>> indices;
    auto expected = source;
    for (int i = 0; i < kLanes<T>; ++i) {
      indices[i] = mask & (1 << i)
          ? 0
          : (i & 1 ? std::numeric_limits<int32_t>::max()
                   : std::numeric_limits<int32_t>::min());
      if (mask & (1 << i)) {
        std::memcpy(&expected[i], base, sizeof(T));
      }
    }
    const auto actual =
        maskGather<T, kScale>(base, indices.data(), source, mask);
    ASSERT_EQ(std::memcmp(actual.data(), expected.data(), 16), 0);
  }
}

template <typename T>
void checkAllGuardScales() {
  checkGuardPage<T, 1>();
  checkGuardPage<T, 2>();
  checkGuardPage<T, 4>();
  checkGuardPage<T, 8>();
}

TEST(ArmSveNeonGather, InactiveLanesNeverTouchGuardPages) {
  checkAllGuardScales<int32_t>();
  checkAllGuardScales<uint32_t>();
  checkAllGuardScales<float>();
  checkAllGuardScales<int64_t>();
  checkAllGuardScales<uint64_t>();
  checkAllGuardScales<double>();
}

TEST(ArmSveNeonGather, Scaled32BitIndicesPreserve64BitAddressArithmetic) {
  constexpr size_t span = size_t{12} << 30;
  auto* memory = static_cast<uint8_t*>(mmap(
      nullptr,
      span,
      PROT_NONE,
      MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE,
      -1,
      0));
  ASSERT_NE(memory, MAP_FAILED);
  struct Cleanup {
    uint8_t* memory;
    ~Cleanup() {
      munmap(memory, span);
    }
  } cleanup{memory};
  const size_t page = sysconf(_SC_PAGESIZE);
  auto* base = memory + (size_t{6} << 30);
  for (int64_t offset : {-(int64_t{1} << 32), int64_t{1} << 32}) {
    ASSERT_EQ(mprotect(base + offset, page, PROT_READ | PROT_WRITE), 0);
    const int32_t value = offset < 0 ? -1971 : 2026;
    std::memcpy(base + offset, &value, sizeof(value));
  }
  const std::array<int32_t, 4> indices{
      1 << 29, -(1 << 29), 1 << 29, -(1 << 29)};
  const auto actual = gather<int32_t, 8>(
      reinterpret_cast<const int32_t*>(base), indices.data());
  const Values<int32_t> expected{2026, -1971, 2026, -1971};
  EXPECT_EQ(actual, expected);
}

TEST(ArmSveNeonGather, ConcurrentCallsHaveNoSharedScratch) {
  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&, t] {
      std::mt19937 rng(1237 + t);
      std::array<int32_t, 256> data;
      for (auto& value : data) {
        value = rng();
      }
      for (int round = 0; round < 2048; ++round) {
        std::array<int32_t, 4> indices;
        for (auto& index : indices) {
          index = rng() % data.size();
        }
        auto actual = gather<int32_t, 4>(data.data(), indices.data());
        for (int i = 0; i < 4; ++i) {
          if (actual[i] != data[indices[i]]) {
            ++failures;
          }
        }
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  ASSERT_EQ(failures, 0);
}

} // namespace
#else
TEST(ArmSveNeonGather, BuildWithoutSVEBridgeKeepsExistingImplementation) {
  GTEST_SKIP()
      << "Standalone ARM SVE helpers require SVE and NEON-SVE bridge support";
}
#endif
} // namespace facebook::velox::simd
