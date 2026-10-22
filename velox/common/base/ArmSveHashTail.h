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

#pragma once

#include <cstdint>
#include <cstring>

#if defined(__aarch64__) && defined(__ARM_FEATURE_SVE) && \
    defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
#include <arm_sve.h>
#define VELOX_ARM_SVE_HASH_TAIL_AVAILABLE 1
#else
#define VELOX_ARM_SVE_HASH_TAIL_AVAILABLE 0
#endif

namespace facebook::velox::bits::arm_sve_hash_tail {

// Caller bounds size to0..7. Read exactly those bytes and zero the rest;
// extraction always selects the first64-bit lane, independent of SVE VL.
// Scalar arguments/return preserve the existing NEON and public hash ABI.
inline uint64_t load0To7(const uint8_t* input, int32_t size) {
#if VELOX_ARM_SVE_HASH_TAIL_AVAILABLE
  const auto active = svwhilelt_b8(uint64_t{0}, static_cast<uint64_t>(size));
  const auto bytes = svld1_u8(active, input);
  return svlastb_u64(svptrue_pat_b64(SV_VL1), svreinterpret_u64_u8(bytes));
#else
  uint64_t word = 0;
  if (size != 0) {
    std::memcpy(&word, input, size);
  }
  return word;
#endif
}

} // namespace facebook::velox::bits::arm_sve_hash_tail
