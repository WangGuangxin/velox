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
#include <utility>

namespace facebook::velox::exec::detail {
enum class PartialMemoryGrowth { kNotApplicable, kReservationRejected, kGrown };

// Reserve before changing the operator limit. False leaves the old policy in
// force; exceptions (including an aborted pool) must propagate to the caller.
template <typename Reserve>
PartialMemoryGrowth tryGrowPartialAggregationMemory(
    int64_t currentLimit,
    int64_t adaptiveLimit,
    int64_t inputRows,
    int64_t distinctRows,
    int64_t allocatedBytes,
    Reserve&& reserve) {
  if (adaptiveLimit <= currentLimit || adaptiveLimit > (int64_t{1} << 30) ||
      inputRows <= 0 || distinctRows < 0 || allocatedBytes < 0 ||
      allocatedBytes >= adaptiveLimit ||
      static_cast<double>(distinctRows) / inputRows > 0.40) {
    return PartialMemoryGrowth::kNotApplicable;
  }
  return std::forward<Reserve>(reserve)(adaptiveLimit - allocatedBytes)
      ? PartialMemoryGrowth::kGrown
      : PartialMemoryGrowth::kReservationRejected;
}
} // namespace facebook::velox::exec::detail
