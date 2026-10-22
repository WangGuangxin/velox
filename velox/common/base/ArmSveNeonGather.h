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

#pragma once

#include <cstdint>
#include <cstring>
#include <type_traits>

#if defined(__ARM_FEATURE_SVE) && __has_include(<arm_neon_sve_bridge.h>)
#include <arm_neon_sve_bridge.h>
#define VELOX_SVE_NEON_GATHER_AVAILABLE 1

namespace facebook::velox::simd::arm_detail {

// Keep the existing NEON register ABI: four 32-bit lanes or two 64-bit
// lanes, regardless of the calling thread's actual SVE vector length.
template <typename T>
using Neon32 = std::conditional_t<
    std::is_same_v<T, float>,
    float32x4_t,
    std::conditional_t<std::is_same_v<T, uint32_t>, uint32x4_t, int32x4_t>>;

template <typename T>
using Neon64 = std::conditional_t<
    std::is_same_v<T, double>,
    float64x2_t,
    std::conditional_t<std::is_same_v<T, uint64_t>, uint64x2_t, int64x2_t>>;

template <typename T, int kScale>
inline Neon32<T>
gather32Active(const T* base, int32x4_t indices, svbool_t active) {
  static_assert(
      std::is_same_v<T, int32_t> || std::is_same_v<T, uint32_t> ||
      std::is_same_v<T, float>);
  static_assert(kScale == 1 || kScale == 2 || kScale == 4 || kScale == 8);
  const auto index = svset_neonq_s32(svdup_n_s32(0), indices);
  if constexpr (kScale == 4) {
    if constexpr (std::is_same_v<T, float>) {
      return svget_neonq_f32(svld1_gather_s32index_f32(active, base, index));
    } else if constexpr (std::is_same_v<T, uint32_t>) {
      return svget_neonq_u32(svld1_gather_s32index_u32(active, base, index));
    } else {
      return svget_neonq_s32(svld1_gather_s32index_s32(active, base, index));
    }
  } else if constexpr (kScale == 1) {
    if constexpr (std::is_same_v<T, float>) {
      return svget_neonq_f32(svld1_gather_s32offset_f32(active, base, index));
    } else if constexpr (std::is_same_v<T, uint32_t>) {
      return svget_neonq_u32(svld1_gather_s32offset_u32(active, base, index));
    } else {
      return svget_neonq_s32(svld1_gather_s32offset_s32(active, base, index));
    }
  } else {
    // A 32-bit byte offset can overflow even for a valid int32_t index.
    // Keep the existing scalar address semantics for scales two and eight.
    alignas(16) int32_t offsets[4];
    alignas(16) uint32_t enabled[4];
    alignas(16) T values[4]{};
    vst1q_s32(offsets, indices);
    svst1_u32(
        svptrue_pat_b32(SV_VL4), enabled, svdup_u32_z(active, ~uint32_t{0}));
    for (int lane = 0; lane < 4; ++lane) {
      if (enabled[lane]) {
        std::memcpy(
            &values[lane],
            reinterpret_cast<const char*>(base) +
                static_cast<int64_t>(offsets[lane]) * kScale,
            sizeof(T));
      }
    }
    Neon32<T> result;
    std::memcpy(&result, values, 16);
    return result;
  }
}

template <typename T, int kScale = sizeof(T)>
inline Neon32<T> gather32(const T* base, int32x4_t indices) {
  return gather32Active<T, kScale>(base, indices, svptrue_pat_b32(SV_VL4));
}

template <typename T, int kScale = sizeof(T)>
inline Neon32<T> maskGather32(
    Neon32<T> source,
    uint32x4_t mask,
    const T* base,
    int32x4_t indices) {
  // The mask uses the canonical all-zero/all-one NEON boolean lane format.
  const auto lanes = svptrue_pat_b32(SV_VL4);
  const auto active =
      svcmpne_n_u32(lanes, svset_neonq_u32(svdup_n_u32(0), mask), 0);
  // Masked-off lanes are never dereferenced, even when their indices point
  // into inaccessible memory. Preserve source bits, including NaN payloads.
  const auto gathered = gather32Active<T, kScale>(base, indices, active);
  if constexpr (std::is_same_v<T, float>) {
    return vbslq_f32(mask, gathered, source);
  } else if constexpr (std::is_same_v<T, uint32_t>) {
    return vbslq_u32(mask, gathered, source);
  } else {
    return vbslq_s32(mask, gathered, source);
  }
}

template <typename T, int kScale>
inline Neon64<T>
gather64Active(const T* base, int32x2_t indices, svbool_t active) {
  static_assert(
      std::is_same_v<T, int64_t> || std::is_same_v<T, uint64_t> ||
      std::is_same_v<T, double>);
  static_assert(kScale == 1 || kScale == 2 || kScale == 4 || kScale == 8);
  const auto index = svset_neonq_s64(svdup_n_s64(0), vmovl_s32(indices));
  if constexpr (kScale == 8) {
    if constexpr (std::is_same_v<T, double>) {
      return svget_neonq_f64(svld1_gather_s64index_f64(active, base, index));
    } else if constexpr (std::is_same_v<T, uint64_t>) {
      return svget_neonq_u64(svld1_gather_s64index_u64(active, base, index));
    } else {
      return svget_neonq_s64(svld1_gather_s64index_s64(active, base, index));
    }
  } else {
    const auto bytes = svmul_n_s64_x(active, index, kScale);
    if constexpr (std::is_same_v<T, double>) {
      return svget_neonq_f64(svld1_gather_s64offset_f64(active, base, bytes));
    } else if constexpr (std::is_same_v<T, uint64_t>) {
      return svget_neonq_u64(svld1_gather_s64offset_u64(active, base, bytes));
    } else {
      return svget_neonq_s64(svld1_gather_s64offset_s64(active, base, bytes));
    }
  }
}

template <typename T, int kScale = sizeof(T)>
inline Neon64<T> gather64(const T* base, int32x2_t indices) {
  return gather64Active<T, kScale>(base, indices, svptrue_pat_b64(SV_VL2));
}

template <typename T, int kScale = sizeof(T)>
inline Neon64<T> maskGather64(
    Neon64<T> source,
    uint64x2_t mask,
    const T* base,
    int32x2_t indices) {
  // The mask uses the canonical all-zero/all-one NEON boolean lane format.
  const auto lanes = svptrue_pat_b64(SV_VL2);
  const auto active =
      svcmpne_n_u64(lanes, svset_neonq_u64(svdup_n_u64(0), mask), 0);
  const auto gathered = gather64Active<T, kScale>(base, indices, active);
  if constexpr (std::is_same_v<T, double>) {
    return vbslq_f64(mask, gathered, source);
  } else if constexpr (std::is_same_v<T, uint64_t>) {
    return vbslq_u64(mask, gathered, source);
  } else {
    return vbslq_s64(mask, gathered, source);
  }
}

} // namespace facebook::velox::simd::arm_detail
#else
#define VELOX_SVE_NEON_GATHER_AVAILABLE 0
#endif
