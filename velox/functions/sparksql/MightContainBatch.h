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
#include <memory>
#include <string>
#include <vector>

#include <folly/hash/Hash.h>

#include "velox/expression/VectorFunction.h"

#if defined(__aarch64__) && defined(__ARM_FEATURE_SVE) && \
    defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
#include <arm_sve.h>
#define VELOX_ARM_SVE_BLOOM_PROBE_AVAILABLE 1
#else
#define VELOX_ARM_SVE_BLOOM_PROBE_AVAILABLE 0
#endif

namespace facebook::velox::functions::sparksql {

/// Batched Bloom filter probes for might_contain. Each result equals
/// BloomFilterView::mayContain(folly::hasher<int64_t>()(value)): the hash
/// selects word (hash >> 24) & (bloomSize - 1) and requires the four bits
/// selected by the low four 6-bit fields of the hash.
namespace bloom_probe {

inline uint64_t mask(uint64_t hash) {
  return (1UL << (hash & 63)) | (1UL << ((hash >> 6) & 63)) |
      (1UL << ((hash >> 12) & 63)) | (1UL << ((hash >> 18) & 63));
}

inline bool test(const uint64_t* bloom, uint32_t bloomSize, int64_t value) {
  const uint64_t hash = folly::hasher<int64_t>()(value);
  const uint64_t m = mask(hash);
  return (bloom[(hash >> 24) & (bloomSize - 1)] & m) == m;
}

/// Portable batch: hash a block of values first and prefetch its words so
/// that the dependent loads overlap.
void probeGeneric(
    const uint64_t* bloom,
    uint32_t bloomSize,
    const int64_t* values,
    int32_t size,
    uint8_t* out);

/// ARM SVE batch: vector twang hash, vector masks and a predicated gather of
/// the selected words. Falls back to probeGeneric without SVE.
void probeSve(
    const uint64_t* bloom,
    uint32_t bloomSize,
    const int64_t* values,
    int32_t size,
    uint8_t* out);

} // namespace bloom_probe

/// Batched vector function forms of might_contain, registered as
/// might_contain_batch and might_contain_batch_sve. Results equal the simple
/// might_contain function: any NULL input yields NULL and a non-constant
/// filter yields false.
std::vector<std::shared_ptr<exec::FunctionSignature>> mightContainSignatures();

/// Creates the batched function; 'sve' selects probeSve over probeGeneric.
std::shared_ptr<exec::VectorFunction> makeMightContainBatch(
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    bool sve);

} // namespace facebook::velox::functions::sparksql
