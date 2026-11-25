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

// Adapted from Apache Arrow.

#include "velox/dwio/parquet/common/LevelComparison.h"

#include <limits>

#include "folly/lang/Bits.h"

#if defined(__aarch64__)
#include <arm_neon.h>
#endif

namespace facebook::velox::parquet {
namespace {
template <typename Predicate>
inline uint64_t
LevelsToBitmap(const int16_t* levels, int64_t numLevels, Predicate predicate) {
  // Both clang and GCC can vectorize this automatically with SSE4/AVX2.
  uint64_t mask = 0;
  for (int x = 0; x < numLevels; x++) {
    mask |= static_cast<uint64_t>(predicate(levels[x]) ? 1 : 0) << x;
  }
  return folly::Endian::little(mask);
}

} // namespace

uint64_t
GreaterThanBitmap(const int16_t* levels, int64_t numLevels, int16_t rhs) {
#if defined(__aarch64__)
  // Full 64-level batches (the common case): compare 8 lanes at a time,
  // narrow the masks to bytes and fold each 8 bytes into one mask byte.
  if (numLevels == 64) {
    static constexpr uint8_t kWeights[16] = {
        1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};
    const uint8x16_t weights = vld1q_u8(kWeights);
    const int16x8_t threshold = vdupq_n_s16(rhs);
    uint8x16_t bytes[4];
    for (int i = 0; i < 4; ++i) {
      const uint16x8_t firstHalf =
          vcgtq_s16(vld1q_s16(levels + 16 * i), threshold);
      const uint16x8_t secondHalf =
          vcgtq_s16(vld1q_s16(levels + 16 * i + 8), threshold);
      bytes[i] = vandq_u8(
          vcombine_u8(vmovn_u16(firstHalf), vmovn_u16(secondHalf)), weights);
    }
    uint8x16_t sum =
        vpaddq_u8(vpaddq_u8(bytes[0], bytes[1]), vpaddq_u8(bytes[2], bytes[3]));
    sum = vpaddq_u8(sum, sum);
    return vgetq_lane_u64(vreinterpretq_u64_u8(sum), 0);
  }
#endif
  return LevelsToBitmap(
      levels, numLevels, [rhs](int16_t value) { return value > rhs; });
}

MinMax FindMinMax(const int16_t* levels, int64_t numLevels) {
  MinMax out{
      std::numeric_limits<int16_t>::max(), std::numeric_limits<int16_t>::min()};
  for (int x = 0; x < numLevels; x++) {
    out.min = std::min(levels[x], out.min);
    out.max = std::max(levels[x], out.max);
  }
  return out;
}

} // namespace facebook::velox::parquet
