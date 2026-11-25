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

#include "velox/exec/tests/utils/TimestampUtcAssertions.h"

#include <gtest/gtest.h>

#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/vector/tests/utils/VectorAssertions.h"

namespace facebook::velox::exec::test {

TypePtr toTimestampUtc(const TypePtr& type) {
  if (type->equivalent(*TIMESTAMP())) {
    return TIMESTAMP_UTC();
  }
  switch (type->kind()) {
    case TypeKind::ARRAY:
      return ARRAY(toTimestampUtc(type->childAt(0)));
    case TypeKind::MAP:
      return MAP(
          toTimestampUtc(type->childAt(0)), toTimestampUtc(type->childAt(1)));
    case TypeKind::ROW: {
      const auto& rowType = type->asRow();
      std::vector<TypePtr> children;
      children.reserve(rowType.size());
      for (const auto& child : rowType.children()) {
        children.push_back(toTimestampUtc(child));
      }
      return ROW(
          std::vector<std::string>(rowType.names()), std::move(children));
    }
    default:
      return type;
  }
}

RowVectorPtr toTimestampUtc(
    const RowVectorPtr& vector,
    memory::MemoryPool* pool) {
  // A flat copy is singly referenced and keeps no inner vector, such as the
  // base of a dictionary, with the old type.
  auto copy = BaseVector::copy(*vector, pool);
  copy->setType(toTimestampUtc(vector->type()));
  return std::static_pointer_cast<RowVector>(copy);
}

namespace {

// The America/Los_Angeles DST changes skip and repeat local times, so
// adjusting a TIMESTAMP UTC value to this zone changes it.
constexpr std::string_view kSessionTimezone{"America/Los_Angeles"};

RowVectorPtr run(
    const TimestampUtcPlanFactory& makePlan,
    const TimestampTypedInput& typedInput,
    const TimestampUtcAssertionOptions& options,
    memory::MemoryPool* pool,
    std::shared_ptr<Task>& task) {
  AssertQueryBuilder builder(makePlan(typedInput));
  builder.maxDrivers(options.maxDrivers)
      .config(
          core::QueryConfig::kSessionTimezone, std::string(kSessionTimezone))
      .config(core::QueryConfig::kAdjustTimestampToTimezone, "true")
      .configs(options.configs);
  if (!options.spillDirectory.empty()) {
    builder.spillDirectory(options.spillDirectory);
  }
  return builder.copyResults(pool, task);
}

void assertResults(
    const RowVectorPtr& timestampResult,
    const RowVectorPtr& timestampUtcResult,
    bool ordered,
    memory::MemoryPool* pool) {
  if (timestampResult->size() == 0) {
    ADD_FAILURE() << "Plan must return rows to compare";
    return;
  }

  const auto& timestampType = timestampResult->type();
  const auto expectedType = toTimestampUtc(timestampType);
  if (expectedType->equivalent(*timestampType)) {
    ADD_FAILURE() << "Plan output must contain TIMESTAMP: "
                  << timestampType->toString();
    return;
  }
  if (!expectedType->equivalent(*timestampUtcResult->type())) {
    ADD_FAILURE() << "TIMESTAMP UTC run must return "
                  << expectedType->toString() << ", but returned "
                  << timestampUtcResult->type()->toString();
    return;
  }

  const auto expected = toTimestampUtc(timestampResult, pool);
  if (ordered) {
    velox::test::VectorAssertions::equal(expected, timestampUtcResult);
  } else {
    EXPECT_TRUE(assertEqualResults({expected}, {timestampUtcResult}));
  }
}

} // namespace

std::shared_ptr<Task> assertTimestampUtcMatchesTimestamp(
    const TimestampUtcPlanFactory& makePlan,
    const TimestampUtcAssertionOptions& options,
    memory::MemoryPool* pool) {
  const TimestampTypedInput asTimestamp =
      [](const std::vector<RowVectorPtr>& input) { return input; };
  const TimestampTypedInput asTimestampUtc =
      [pool](const std::vector<RowVectorPtr>& input) {
        std::vector<RowVectorPtr> converted;
        converted.reserve(input.size());
        for (const auto& vector : input) {
          converted.push_back(toTimestampUtc(vector, pool));
        }
        return converted;
      };

  std::shared_ptr<Task> timestampTask;
  const auto timestampResult =
      run(makePlan, asTimestamp, options, pool, timestampTask);
  std::shared_ptr<Task> timestampUtcTask;
  const auto timestampUtcResult =
      run(makePlan, asTimestampUtc, options, pool, timestampUtcTask);

  assertResults(timestampResult, timestampUtcResult, options.ordered, pool);
  return timestampUtcTask;
}

} // namespace facebook::velox::exec::test
