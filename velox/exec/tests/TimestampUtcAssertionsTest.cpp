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

#include <gtest/gtest-spi.h>

#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TimestampUtcAssertions.h"

namespace facebook::velox::exec::test {
namespace {

class TimestampUtcAssertionsTest : public OperatorTestBase {
 protected:
  RowVectorPtr makeInput() {
    return makeRowVector(
        {"id", "ts"},
        {
            makeFlatVector<int32_t>({1, 2, 3, 4}),
            makeNullableFlatVector<Timestamp>(
                {Timestamp(1'710'037'800, 0),
                 std::nullopt,
                 Timestamp(-1, 999'999'000),
                 Timestamp(1'710'037'800, 0)}),
        });
  }
};

TEST_F(TimestampUtcAssertionsTest, toTimestampUtcType) {
  EXPECT_EQ(*toTimestampUtc(TIMESTAMP()), *TIMESTAMP_UTC());
  EXPECT_EQ(*toTimestampUtc(BIGINT()), *BIGINT());
  EXPECT_EQ(*toTimestampUtc(TIMESTAMP_UTC()), *TIMESTAMP_UTC());

  const auto type = ROW({
      {"a", TIMESTAMP()},
      {"b", ARRAY(TIMESTAMP())},
      {"c", MAP(VARCHAR(), ROW({{"d", TIMESTAMP()}, {"e", DOUBLE()}}))},
  });
  const auto expected = ROW({
      {"a", TIMESTAMP_UTC()},
      {"b", ARRAY(TIMESTAMP_UTC())},
      {"c", MAP(VARCHAR(), ROW({{"d", TIMESTAMP_UTC()}, {"e", DOUBLE()}}))},
  });
  EXPECT_EQ(*toTimestampUtc(type), *expected);
}

TEST_F(TimestampUtcAssertionsTest, toTimestampUtcVector) {
  const std::vector<std::optional<Timestamp>> timestamps{
      Timestamp(1'710'037'800, 0),
      std::nullopt,
      Timestamp(-1, 999'999'000),
  };
  const std::vector<std::vector<std::optional<Timestamp>>> arrays{
      timestamps,
      {},
      {timestamps[0]},
  };
  const auto input = makeRowVector({
      makeNullableFlatVector<Timestamp>(timestamps),
      wrapInDictionary(
          makeIndicesInReverse(3),
          makeNullableFlatVector<Timestamp>(timestamps)),
      makeNullableArrayVector<Timestamp>(arrays),
  });

  const auto result = toTimestampUtc(input, pool());

  const auto expected = makeRowVector({
      makeNullableFlatVector<Timestamp>(timestamps, TIMESTAMP_UTC()),
      makeNullableFlatVector<Timestamp>(
          {timestamps[2], timestamps[1], timestamps[0]}, TIMESTAMP_UTC()),
      makeNullableArrayVector<Timestamp>(arrays, ARRAY(TIMESTAMP_UTC())),
  });
  velox::test::assertEqualVectors(expected, result);
}

TEST_F(TimestampUtcAssertionsTest, matches) {
  const auto input = makeInput();
  {
    SCOPED_TRACE("Ordered");
    assertTimestampUtcMatchesTimestamp(
        [&](const auto& typedInput) {
          return PlanBuilder()
              .values(typedInput({input}))
              .orderBy({"ts", "id"}, false)
              .planNode();
        },
        {.ordered = true},
        pool());
  }
  {
    SCOPED_TRACE("Unordered");
    const auto task = assertTimestampUtcMatchesTimestamp(
        [&](const auto& typedInput) {
          return PlanBuilder()
              .values(typedInput({input}))
              .singleAggregation({"ts"}, {"count(1)"})
              .planNode();
        },
        {},
        pool());
    ASSERT_NE(task, nullptr);
  }
}

TEST_F(TimestampUtcAssertionsTest, mismatchedType) {
  const auto input = makeInput();
  // The plan reads its input directly, so both runs return TIMESTAMP.
  EXPECT_NONFATAL_FAILURE(
      assertTimestampUtcMatchesTimestamp(
          [&](const auto& /*typedInput*/) {
            return PlanBuilder()
                .values({input})
                .orderBy({"ts", "id"}, false)
                .planNode();
          },
          {},
          pool()),
      "TIMESTAMP UTC run must return ROW<id:INTEGER,ts:TIMESTAMP UTC>, "
      "but returned ROW<id:INTEGER,ts:TIMESTAMP>");
}

TEST_F(TimestampUtcAssertionsTest, noTimestampInOutput) {
  const auto input = makeInput();
  EXPECT_NONFATAL_FAILURE(
      assertTimestampUtcMatchesTimestamp(
          [&](const auto& typedInput) {
            return PlanBuilder()
                .values(typedInput({input}))
                .project({"id"})
                .planNode();
          },
          {},
          pool()),
      "Plan output must contain TIMESTAMP: ROW<id:INTEGER>");
}

TEST_F(TimestampUtcAssertionsTest, noRows) {
  const auto input = makeInput();
  EXPECT_NONFATAL_FAILURE(
      assertTimestampUtcMatchesTimestamp(
          [&](const auto& typedInput) {
            return PlanBuilder()
                .values(typedInput({input}))
                .filter("id > 10")
                .planNode();
          },
          {},
          pool()),
      "Plan must return rows to compare");
}

} // namespace
} // namespace facebook::velox::exec::test
