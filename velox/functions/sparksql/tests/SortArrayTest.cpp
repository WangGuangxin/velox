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

#include <algorithm>
#include <limits>
#include <optional>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/sparksql/registration/Register.h"
#include "velox/functions/sparksql/tests/ArraySortTestData.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "velox/vector/ComplexVector.h"

using namespace facebook::velox::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

class SortArrayTest : public SparkFunctionBaseTest {
 protected:
  void testSortArray(
      const VectorPtr& input,
      const VectorPtr& expectedAsc,
      const VectorPtr& expectedDesc) {
    // Verify that by default array is sorted in ascending order.
    testSortArray("sort_array(c0)", input, expectedAsc);

    // Verify sort order with asc flag set to true.
    testSortArray("sort_array(c0, true)", input, expectedAsc);

    // Verify sort order with asc flag set to false.
    testSortArray("sort_array(c0, false)", input, expectedDesc);
  }

  void testSortArray(
      const std::string& expr,
      const VectorPtr& input,
      const VectorPtr& expected) {
    SCOPED_TRACE(expr);
    auto result = evaluate(expr, makeRowVector({input}));
    assertEqualVectors(expected, result);
  }

  template <typename T>
  void testInt() {
    auto input = makeNullableArrayVector(intInput<T>());
    auto expected = intAscNullSmallest<T>();
    testSortArray(
        input,
        makeNullableArrayVector(expected),
        makeNullableArrayVector(reverseNested(expected)));
  }

  template <typename T>
  void testRowWithIntegerFirstField(const TypePtr& intType) {
    SCOPED_TRACE(intType->toString());
    const T kMin = std::numeric_limits<T>::min();
    const T kMax = std::numeric_limits<T>::max();
    const variant nullRow = variant(TypeKind::ROW);
    auto row = [](T key, const char* value) {
      return variant::row({variant(key), variant(value)});
    };
    // Many ties on the first field so that the second field decides, plus the
    // extreme values of the type.
    NestedVector<variant> input{
        {},
        {row(1, "b")},
        {row(2, "b"), row(1, "z"), row(2, "a"), row(1, "a"), row(0, "c")},
        {row(kMax, "x"), row(kMin, "x"), row(-1, "y"), row(kMax, "a")},
        {row(5, "q"), nullRow, row(-5, "q"), nullRow, row(5, "p")},
        {row(3, "s"), row(3, "s"), row(3, "r")},
    };
    NestedVector<variant> expected{
        {},
        {row(1, "b")},
        {row(0, "c"), row(1, "a"), row(1, "z"), row(2, "a"), row(2, "b")},
        {row(kMin, "x"), row(-1, "y"), row(kMax, "a"), row(kMax, "x")},
        {nullRow, nullRow, row(-5, "q"), row(5, "p"), row(5, "q")},
        {row(3, "r"), row(3, "s"), row(3, "s")},
    };
    auto rowType = ROW({intType, VARCHAR()});
    testSortArray(
        makeArrayOfRowVector(rowType, input),
        makeArrayOfRowVector(rowType, expected),
        makeArrayOfRowVector(rowType, reverseNested(expected)));
  }

  template <typename T>
  void testFloatingPoint() {
    auto input = makeNullableArrayVector(floatingPointInput<T>());
    auto expected = floatingPointAscNullSmallest<T>();
    testSortArray(
        input,
        makeNullableArrayVector(expected),
        makeNullableArrayVector(reverseNested(expected)));
  }
};

TEST_F(SortArrayTest, invalidInput) {
  auto arg0 = makeNullableArrayVector<int>({{0, 1}});
  std::vector<bool> v = {false};
  auto arg1 = makeFlatVector<bool>(v);
  ASSERT_THROW(
      evaluate<ArrayVector>("sort_array(c0, c1)", makeRowVector({arg0, arg1})),
      VeloxException);
}

TEST_F(SortArrayTest, int8) {
  testInt<int8_t>();
}

TEST_F(SortArrayTest, int16) {
  testInt<int16_t>();
}

TEST_F(SortArrayTest, int32) {
  testInt<int32_t>();
}

TEST_F(SortArrayTest, int64) {
  testInt<int64_t>();
}

TEST_F(SortArrayTest, float) {
  testFloatingPoint<float>();
}

TEST_F(SortArrayTest, double) {
  testFloatingPoint<double>();
}

TEST_F(SortArrayTest, string) {
  auto input = makeNullableArrayVector(stringInput());
  auto expected = stringAscNullSmallest();
  testSortArray(
      input,
      makeNullableArrayVector(expected),
      makeNullableArrayVector(reverseNested(expected)));
}

TEST_F(SortArrayTest, timestamp) {
  auto input = makeNullableArrayVector(timestampInput());
  auto expected = timestampAscNullSmallest();
  testSortArray(
      input,
      makeNullableArrayVector(expected),
      makeNullableArrayVector(reverseNested(expected)));
}

TEST_F(SortArrayTest, date) {
  auto input = makeNullableArrayVector(dateInput(), ARRAY(DATE()));
  auto expected = dateAscNullSmallest();
  testSortArray(
      input,
      makeNullableArrayVector(expected, ARRAY(DATE())),
      makeNullableArrayVector(reverseNested(expected), ARRAY(DATE())));
}

TEST_F(SortArrayTest, bool) {
  auto input = makeNullableArrayVector(boolInput());
  auto expected = boolAscNullSmallest();
  testSortArray(
      input,
      makeNullableArrayVector(expected),
      makeNullableArrayVector(reverseNested(expected)));
}

TEST_F(SortArrayTest, array) {
  auto input = makeNullableNestedArrayVector(arrayInput());
  auto expected = arrayAscNullSmallest();
  testSortArray(
      input,
      makeNullableNestedArrayVector(expected),
      makeNullableNestedArrayVector(reverseNested(expected)));
}

// Map is not orderable, so sorting is not supported.
TEST_F(SortArrayTest, failOnMapTypeSort) {
  auto input = makeArrayOfMapVector(mapInput());
  const std::string kErrorMessage =
      "Scalar function signature is not supported";

  VELOX_ASSERT_THROW(
      evaluate("sort_array(c0)", makeRowVector({input})), kErrorMessage);
  VELOX_ASSERT_THROW(
      evaluate("sort_array(c0, true)", makeRowVector({input})), kErrorMessage);
}

TEST_F(SortArrayTest, row) {
  auto rowType = ROW({INTEGER(), VARCHAR()});
  auto input = makeArrayOfRowVector(rowType, rowInput());
  auto expected = rowAscNullSmallest();
  testSortArray(
      input,
      makeArrayOfRowVector(rowType, expected),
      makeArrayOfRowVector(rowType, reverseNested(expected)));
}

// Rows whose first field is a non-null integer are ordered by that field
// directly and only compared in full on ties.
TEST_F(SortArrayTest, rowWithIntegerFirstField) {
  testRowWithIntegerFirstField<int8_t>(TINYINT());
  testRowWithIntegerFirstField<int16_t>(SMALLINT());
  testRowWithIntegerFirstField<int32_t>(INTEGER());
  testRowWithIntegerFirstField<int64_t>(BIGINT());
}

TEST_F(SortArrayTest, dictionaryEncodedRowWithIntegerFirstField) {
  // Elements are a dictionary over a row vector whose first field is itself
  // a dictionary.
  auto rowValues = makeRowVector({
      wrapInDictionary(
          makeIndicesInReverse(6), makeFlatVector<int64_t>({3, 1, 2, 1, 3, 0})),
      makeFlatVector<std::string>({"a", "b", "c", "d", "e", "f"}),
  });
  // First field values after the reverse dictionary: 0, 3, 1, 2, 1, 3.
  auto elements =
      wrapInDictionary(makeIndices({5, 4, 3, 2, 1, 0, 1, 3}), rowValues);
  auto input = makeArrayVector({0, 4}, elements);

  // Array 0: (3,f) (1,e) (2,d) (1,c); array 1: (3,b) (0,a) (3,b) (2,d).
  auto rowType = ROW({BIGINT(), VARCHAR()});
  auto expected = NestedVector<variant>{
      {variant::row({1L, "c"}),
       variant::row({1L, "e"}),
       variant::row({2L, "d"}),
       variant::row({3L, "f"})},
      {variant::row({0L, "a"}),
       variant::row({2L, "d"}),
       variant::row({3L, "b"}),
       variant::row({3L, "b"})},
  };
  testSortArray(
      input,
      makeArrayOfRowVector(rowType, expected),
      makeArrayOfRowVector(rowType, reverseNested(expected)));
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
