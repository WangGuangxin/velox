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
#include "functions/sparksql/specialforms/GetArrayStructFields.h"
#include <gtest/gtest.h>
#include <type/Type.h>
#include <vector/ComplexVector.h>
#include <optional>
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "velox/vector/tests/utils/VectorTestBase.h"
using namespace facebook::velox::test;
namespace facebook::velox::functions::sparksql::test {
namespace {
class GetArrayStructFieldsTest : public SparkFunctionBaseTest {
 protected:
  void testGetArrayStructFields(
      const VectorPtr& input,
      int ordinal,
      const VectorPtr& expected) {
    std::vector<core::TypedExprPtr> inputs = {
        std::make_shared<const core::FieldAccessTypedExpr>(input->type(), "c0"),
        std::make_shared<core::ConstantTypedExpr>(INTEGER(), variant(ordinal))};
    auto expr = std::make_shared<const core::CallTypedExpr>(
        expected->type(), std::move(inputs), "get_array_struct_fields");
    testEncodings(expr, {input}, expected);
  }
};

TEST_F(GetArrayStructFieldsTest, basic) {
  // array(row(int, varchar)).
  variant nullRow = variant(TypeKind::ROW);
  variant nullInt = variant(TypeKind::INTEGER);
  auto data = makeArrayOfRowVector(
      ROW({INTEGER(), VARCHAR()}),
      std::vector<std::vector<variant>>{
          // Empty.
          {},
          // All nulls.
          {nullRow, nullRow},
          // Null row.
          {variant::row({2, "red"}), variant::row({1, "blue"})},
          // Null values in row.
          {variant::row({3, "green"}), variant::row({nullInt, "red"})},
      });

  auto expected = makeNullableArrayVector<int32_t>(
      std::vector<std::optional<std::vector<std::optional<int32_t>>>>{
          std::vector<std::optional<int32_t>>{},
          std::vector<std::optional<int32_t>>{std::nullopt, std::nullopt},
          std::vector<std::optional<int32_t>>{2, 1},
          std::vector<std::optional<int32_t>>{3, std::nullopt}});
  testGetArrayStructFields(data, 0, expected);

  expected = makeNullableArrayVector<std::string>(
      std::vector<std::optional<std::vector<std::optional<std::string>>>>{
          std::vector<std::optional<std::string>>{},
          std::vector<std::optional<std::string>>{std::nullopt, std::nullopt},
          std::vector<std::optional<std::string>>{"red", "blue"},
          std::vector<std::optional<std::string>>{"green", "red"}});
  testGetArrayStructFields(data, 1, expected);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
