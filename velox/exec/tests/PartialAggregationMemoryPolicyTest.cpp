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
#include "velox/exec/PartialAggregationMemoryPolicy.h"
#include <gtest/gtest.h>
#include <stdexcept>

namespace facebook::velox::exec::detail {
TEST(PartialAggregationMemoryPolicyTest, eligibilityBoundariesDoNotReserve) {
  int calls = 0;
  auto reserve = [&](int64_t) {
    ++calls;
    return true;
  };
  auto check = [&](int64_t current,
                   int64_t cap,
                   int64_t input,
                   int64_t distinct,
                   int64_t allocated) {
    EXPECT_EQ(
        tryGrowPartialAggregationMemory(
            current, cap, input, distinct, allocated, reserve),
        PartialMemoryGrowth::kNotApplicable);
  };
  check(16, 0, 100, 20, 17);
  check(16, 16, 100, 20, 17);
  check(16, (1LL << 30) + 1, 100, 20, 17);
  check(16, 64, 0, 0, 17);
  check(16, 64, 100, -1, 17);
  check(16, 64, 100, 41, 17);
  check(16, 64, 100, 20, -1);
  check(16, 64, 100, 20, 64);
  EXPECT_EQ(calls, 0);
}
TEST(PartialAggregationMemoryPolicyTest, reservesOnlyMissingBytesBeforeGrowth) {
  int64_t bytes = 0;
  EXPECT_EQ(
      tryGrowPartialAggregationMemory(
          16,
          64,
          100,
          40,
          17,
          [&](int64_t requested) {
            bytes = requested;
            return true;
          }),
      PartialMemoryGrowth::kGrown);
  EXPECT_EQ(bytes, 47);
}
TEST(
    PartialAggregationMemoryPolicyTest,
    reservationFailureAndAbortRemainVisible) {
  EXPECT_EQ(
      tryGrowPartialAggregationMemory(
          16, 64, 100, 20, 17, [](int64_t) { return false; }),
      PartialMemoryGrowth::kReservationRejected);
  EXPECT_THROW(
      tryGrowPartialAggregationMemory(
          16,
          64,
          100,
          20,
          17,
          [](int64_t) -> bool { throw std::runtime_error("aborted pool"); }),
      std::runtime_error);
}
} // namespace facebook::velox::exec::detail
