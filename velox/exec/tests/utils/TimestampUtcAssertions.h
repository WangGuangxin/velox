/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
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

#include "velox/core/PlanNode.h"
#include "velox/exec/Task.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec::test {

/// Returns 'type' with every TIMESTAMP, including those nested in complex
/// types, replaced by TIMESTAMP UTC.
TypePtr toTimestampUtc(const TypePtr& type);

/// Returns a flat copy of 'vector' whose type has every TIMESTAMP replaced by
/// TIMESTAMP UTC. The values are unchanged.
RowVectorPtr toTimestampUtc(
    const RowVectorPtr& vector,
    memory::MemoryPool* pool);

/// Returns the input batches with the timestamp type of the current run.
using TimestampTypedInput = std::function<std::vector<RowVectorPtr>(
    const std::vector<RowVectorPtr>& input)>;

/// Makes the plan to check. The plan reads every input batch through
/// 'typedInput'.
using TimestampUtcPlanFactory =
    std::function<core::PlanNodePtr(const TimestampTypedInput& typedInput)>;

/// Options of assertTimestampUtcMatchesTimestamp.
struct TimestampUtcAssertionOptions {
  /// Compares the rows in order. Otherwise, compares them as multisets.
  bool ordered{false};

  /// Maximum number of drivers per pipeline.
  int32_t maxDrivers{1};

  /// Query configs for both runs.
  std::unordered_map<std::string, std::string> configs;

  /// Directory for spill files. Empty if the plan does not spill.
  std::string spillDirectory;
};

/// Checks that operators handle TIMESTAMP UTC the same way as TIMESTAMP.
///
/// Runs the plan from 'makePlan' twice: over input with TIMESTAMP columns, and
/// over the same values typed TIMESTAMP UTC. The results of the second run must
/// have the same values as the first, and TIMESTAMP UTC wherever the first has
/// TIMESTAMP. The results of the first run must be non-empty and contain
/// TIMESTAMP. Both runs use a session time zone other than UTC, so an operator
/// that adjusted TIMESTAMP UTC values to it would fail the check.
///
/// Returns the task of the second run, for example to check its spill stats.
std::shared_ptr<Task> assertTimestampUtcMatchesTimestamp(
    const TimestampUtcPlanFactory& makePlan,
    const TimestampUtcAssertionOptions& options,
    memory::MemoryPool* pool);

} // namespace facebook::velox::exec::test
