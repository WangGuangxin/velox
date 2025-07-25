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

#include "velox/functions/sparksql/DecimalArithmetic.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/Expressions.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

class DecimalArithmeticTest : public SparkFunctionBaseTest {
 public:
  DecimalArithmeticTest() {
    options_.parseDecimalAsDouble = false;
  }

 protected:
  void testArithmeticFunction(
      const std::string& functionName,
      const VectorPtr& expected,
      const std::vector<VectorPtr>& inputs) {
    VELOX_USER_CHECK_GE(
        inputs.size(),
        1,
        "At least one input vector is needed for arithmetic function test.");
    std::vector<core::TypedExprPtr> inputExprs;
    for (int i = 0; i < inputs.size(); ++i) {
      inputExprs.emplace_back(std::make_shared<core::FieldAccessTypedExpr>(
          inputs[i]->type(), fmt::format("c{}", i)));
    }
    auto expr = std::make_shared<const core::CallTypedExpr>(
        expected->type(), std::move(inputExprs), functionName);
    assertEqualVectors(expected, evaluate(expr, makeRowVector(inputs)));
  }

  VectorPtr makeNullableLongDecimalVector(
      const std::vector<std::string>& values,
      const TypePtr& type) {
    VELOX_USER_CHECK(
        type->isDecimal(),
        "Decimal type is needed to create long decimal vector.");
    std::vector<std::optional<int128_t>> numbers;
    numbers.reserve(values.size());
    for (const auto& value : values) {
      if (value == "null") {
        numbers.emplace_back(std::nullopt);
      } else {
        numbers.emplace_back(HugeInt::parse(value));
      }
    }
    return makeNullableFlatVector<int128_t>(numbers, type);
  }
};

TEST_F(DecimalArithmeticTest, add) {
  // Precision < 38.
  testArithmeticFunction(
      "add",
      makeNullableLongDecimalVector(
          {"502", "1502", "11232", "1999999999999999999999999999998"},
          DECIMAL(31, 3)),
      {makeNullableLongDecimalVector(
           {"201", "601", "1366", "999999999999999999999999999999"},
           DECIMAL(30, 3)),
       makeNullableLongDecimalVector(
           {"301", "901", "9866", "999999999999999999999999999999"},
           DECIMAL(30, 3))});

  // Min leading zero >= 3.
  testArithmeticFunction(
      "add",
      makeFlatVector(
          std::vector<int128_t>{2123210, 2999889, 4234568, 4213563},
          DECIMAL(38, 6)),
      {makeFlatVector(
           std::vector<int128_t>{11232100, 9998888, 12345678, 2135632},
           DECIMAL(38, 7)),
       makeFlatVector(std::vector<int64_t>{1, 2, 3, 4}, DECIMAL(10, 0))});

  // No carry to left.
  testArithmeticFunction(
      "add",
      makeNullableLongDecimalVector(
          {"99999999999999999999999999999990000010",
           "99999999999999999999999999999999010000",
           "99999999999999999999999999999999900123",
           "99999999999999999999999999999999990100"},
          DECIMAL(38, 6)),
      {makeNullableLongDecimalVector(
           {"9999999999999999999999999999999000000",
            "9999999999999999999999999999999900000",
            "9999999999999999999999999999999990000",
            "9999999999999999999999999999999999000"},
           DECIMAL(38, 5)),
       makeFlatVector(
           std::vector<int128_t>{100, 99999, 1234, 999}, DECIMAL(38, 7))});

  // Carry to left.
  testArithmeticFunction(
      "add",
      makeNullableLongDecimalVector(
          {"99999999999999999999999999999991500000",
           "99999999999999999999999999999991000000",
           "99999999999999999999999999999999500000",
           "99999999999999999999999999999999100000"},
          DECIMAL(38, 6)),
      {makeNullableLongDecimalVector(
           {"9999999999999999999999999999999070000",
            "9999999999999999999999999999999050000",
            "9999999999999999999999999999999870000",
            "9999999999999999999999999999999890000"},
           DECIMAL(38, 5)),
       makeFlatVector(
           std::vector<int128_t>{8000000, 5000000, 8000000, 1999999},
           DECIMAL(38, 7))});

  // Both -ve.
  testArithmeticFunction(
      "add",
      makeNullableLongDecimalVector(
          {"-502", "-1502", "-11232", "-1999999999999999999999999999998"},
          DECIMAL(31, 3)),
      {makeNullableLongDecimalVector(
           {"-201", "-601", "-1366", "-999999999999999999999999999999"},
           DECIMAL(30, 3)),
       makeNullableLongDecimalVector(
           {"-301", "-901", "-9866", "-999999999999999999999999999999"},
           DECIMAL(30, 3))});

  // Overflow when scaling up the whole part.
  testArithmeticFunction(
      "add",
      makeNullableLongDecimalVector(
          {"null", "null", "null", "null"}, DECIMAL(38, 6)),
      {makeNullableLongDecimalVector(
           {"-99999999999999999999999999999999990000",
            "99999999999999999999999999999999999000",
            "-99999999999999999999999999999999999900",
            "99999999999999999999999999999999999990"},
           DECIMAL(38, 3)),
       makeFlatVector(
           std::vector<int128_t>{-100, 9999999, -999900, 99999},
           DECIMAL(38, 7))});

  // Ve and -ve.
  testArithmeticFunction(
      "add",
      makeNullableLongDecimalVector(
          {"999990", "-999990", "-10", "10"}, DECIMAL(38, 6)),
      {makeNullableLongDecimalVector(
           {"99999999999999999999999999999989999990",
            "-99999999999999999999999999999989999990",
            "99999999999999999999999999999999999980",
            "-99999999999999999999999999999999999980"},
           DECIMAL(38, 6)),
       makeNullableLongDecimalVector(
           {"-9999999999999999999999999999998900000",
            "9999999999999999999999999999998900000",
            "-9999999999999999999999999999999999999",
            "9999999999999999999999999999999999999"},
           DECIMAL(38, 5))});
}

TEST_F(DecimalArithmeticTest, subtract) {
  testArithmeticFunction(
      "subtract",
      makeNullableLongDecimalVector(
          {"-100", "-300", "-8500", "1999999999999999999999999999998"},
          DECIMAL(31, 3)),
      {makeNullableLongDecimalVector(
           {"201", "601", "1366", "999999999999999999999999999999"},
           DECIMAL(30, 3)),
       makeNullableLongDecimalVector(
           {"301", "901", "9866", "-999999999999999999999999999999"},
           DECIMAL(30, 3))});

  // Min leading zero >= 3.
  testArithmeticFunction(
      "subtract",
      makeFlatVector(
          std::vector<int128_t>{123210, -1000111, -1765432, -3786437},
          DECIMAL(38, 6)),
      {makeFlatVector(
           std::vector<int128_t>{11232100, 9998888, 12345678, 2135632},
           DECIMAL(38, 7)),
       makeFlatVector(std::vector<int64_t>{1, 2, 3, 4}, DECIMAL(10, 0))});

  // No carry to left.
  testArithmeticFunction(
      "subtract",
      makeNullableLongDecimalVector(
          {"99999999999999999999999999999990000010",
           "99999999999999999999999999999999010000",
           "99999999999999999999999999999999900123",
           "99999999999999999999999999999999990100"},
          DECIMAL(38, 6)),
      {makeNullableLongDecimalVector(
           {"9999999999999999999999999999999000000",
            "9999999999999999999999999999999900000",
            "9999999999999999999999999999999990000",
            "9999999999999999999999999999999999000"},
           DECIMAL(38, 5)),
       makeFlatVector(
           std::vector<int128_t>{-100, -99999, -1234, -999}, DECIMAL(38, 7))});

  // Carry to left.
  testArithmeticFunction(
      "subtract",
      makeNullableLongDecimalVector(
          {"99999999999999999999999999999991500000",
           "99999999999999999999999999999991000000",
           "99999999999999999999999999999999500000",
           "99999999999999999999999999999999100000"},
          DECIMAL(38, 6)),
      {makeNullableLongDecimalVector(
           {"9999999999999999999999999999999070000",
            "9999999999999999999999999999999050000",
            "9999999999999999999999999999999870000",
            "9999999999999999999999999999999890000"},
           DECIMAL(38, 5)),
       makeFlatVector(
           std::vector<int128_t>{-8000000, -5000000, -8000000, -1999999},
           DECIMAL(38, 7))});

  // Both -ve.
  testArithmeticFunction(
      "subtract",
      makeNullableLongDecimalVector(
          {"100", "300", "8500", "0"}, DECIMAL(31, 3)),
      {makeNullableLongDecimalVector(
           {"-201", "-601", "-1366", "-999999999999999999999999999999"},
           DECIMAL(30, 3)),
       makeNullableLongDecimalVector(
           {"-301", "-901", "-9866", "-999999999999999999999999999999"},
           DECIMAL(30, 3))});

  // Overflow when scaling up the whole part.
  testArithmeticFunction(
      "subtract",
      makeNullableLongDecimalVector(
          {"null", "null", "null", "null"}, DECIMAL(38, 6)),
      {makeNullableLongDecimalVector(
           {"-99999999999999999999999999999999990000",
            "99999999999999999999999999999999999000",
            "-99999999999999999999999999999999999900",
            "99999999999999999999999999999999999990"},
           DECIMAL(38, 3)),
       makeFlatVector(
           std::vector<int128_t>{100, -9999999, 999900, -99999},
           DECIMAL(38, 7))});

  // Ve and -ve.
  testArithmeticFunction(
      "subtract",
      makeNullableLongDecimalVector(
          {"99999999999999999999999999999999999990",
           "-99999999999999999999999999999999999990",
           "99999999999999999999999999999999999990",
           "-99999999999999999999999999999999999990"},
          DECIMAL(38, 6)),
      {makeNullableLongDecimalVector(
           {"99999999999999999999999999999989999990",
            "-99999999999999999999999999999989999990",
            "99999999999999999999999999999999999980",
            "-99999999999999999999999999999999999980"},
           DECIMAL(38, 6)),
       makeFlatVector(
           std::vector<int128_t>{-1000000, 1000000, -1, 1}, DECIMAL(38, 5))});
}

TEST_F(DecimalArithmeticTest, multiply) {
  // The result can be obtained by Spark unit test
  //       test("multiply") {
  //     val l1 = Literal.create(
  //       Decimal(BigDecimal(1), 17, 3),
  //       DecimalType(17, 3))
  //     val l2 = Literal.create(
  //       Decimal(BigDecimal(1), 17, 3),
  //       DecimalType(17, 3))
  //     checkEvaluation(Divide(l1, l2), null)
  //   }
  auto shortFlat = makeFlatVector<int64_t>({1000, 2000}, DECIMAL(17, 3));
  // Multiply short and short, returning long.
  testArithmeticFunction(
      "multiply",
      makeFlatVector<int128_t>({1000000, 4000000}, DECIMAL(35, 6)),
      {shortFlat, shortFlat});
  // Multiply short and long, returning long.
  auto longFlat = makeFlatVector<int128_t>({1000, 2000}, DECIMAL(20, 3));
  auto expectedLongFlat =
      makeFlatVector<int128_t>({1000000, 4000000}, DECIMAL(38, 6));
  testArithmeticFunction("multiply", expectedLongFlat, {shortFlat, longFlat});
  // Multiply long and short, returning long.
  testArithmeticFunction("multiply", expectedLongFlat, {longFlat, shortFlat});

  // Multiply long and long, returning long.
  testArithmeticFunction(
      "multiply",
      makeFlatVector<int128_t>({1000000, 4000000}, DECIMAL(38, 6)),
      {longFlat, longFlat});

  auto leftFlat0 = makeFlatVector<int128_t>({0, 1, 0}, DECIMAL(20, 3));
  auto rightFlat0 = makeFlatVector<int128_t>({1, 0, 0}, DECIMAL(20, 2));
  testArithmeticFunction(
      "multiply",
      makeFlatVector<int128_t>({0, 0, 0}, DECIMAL(38, 5)),
      {leftFlat0, rightFlat0});

  // Multiply short and short, returning short.
  shortFlat = makeFlatVector<int64_t>({1000, 2000}, DECIMAL(6, 3));
  testArithmeticFunction(
      "multiply",
      makeFlatVector<int64_t>({1000000, 4000000}, DECIMAL(13, 6)),
      {shortFlat, shortFlat});

  auto expectedConstantFlat =
      makeFlatVector<int64_t>({100000, 200000}, DECIMAL(10, 5));
  // Constant and Flat arguments.
  testArithmeticFunction(
      "multiply",
      expectedConstantFlat,
      {makeConstant<int64_t>(100, 2, DECIMAL(3, 2)), shortFlat});

  // Flat and Constant arguments.
  testArithmeticFunction(
      "multiply",
      expectedConstantFlat,
      {shortFlat, makeConstant<int64_t>(100, 2, DECIMAL(3, 2))});

  // out_precision == 38, small input values, trimming of scale.
  testArithmeticFunction(
      "multiply",
      makeConstant<int128_t>(61, 1, DECIMAL(38, 7)),
      {makeConstant<int128_t>(201, 1, DECIMAL(20, 5)),
       makeConstant<int128_t>(301, 1, DECIMAL(20, 5))});

  // out_precision == 38, large values, trimming of scale.
  testArithmeticFunction(
      "multiply",
      makeConstant<int128_t>(
          HugeInt::parse("201" + std::string(31, '0')), 1, DECIMAL(38, 6)),
      {makeConstant<int128_t>(201, 1, DECIMAL(20, 5)),
       makeConstant<int128_t>(
           HugeInt::parse(std::string(35, '9')), 1, DECIMAL(35, 5))});

  // out_precision == 38, very large values, trimming of scale (requires convert
  // to 256).
  testArithmeticFunction(
      "multiply",
      makeConstant<int128_t>(
          HugeInt::parse("9999999999999999999999999999999999890"),
          1,
          DECIMAL(38, 6)),
      {makeConstant<int128_t>(
           HugeInt::parse(std::string(35, '9')), 1, DECIMAL(38, 20)),
       makeConstant<int128_t>(
           HugeInt::parse(std::string(36, '9')), 1, DECIMAL(38, 20))});

  // out_precision == 38, very large values, trimming of scale (requires convert
  // to 256). should cause overflow.
  testArithmeticFunction(
      "multiply",
      makeConstant<int128_t>(std::nullopt, 1, DECIMAL(38, 6)),
      {makeConstant<int128_t>(
           HugeInt::parse(std::string(35, '9')), 1, DECIMAL(38, 4)),
       makeConstant<int128_t>(
           HugeInt::parse(std::string(36, '9')), 1, DECIMAL(38, 4))});

  // Big scale * big scale.
  testArithmeticFunction(
      "multiply",
      makeConstant<int128_t>(0, 1, DECIMAL(38, 37)),
      {makeConstant<int128_t>(201, 1, DECIMAL(38, 38)),
       makeConstant<int128_t>(301, 1, DECIMAL(38, 38))});

  // Long decimal limits.
  testArithmeticFunction(
      "multiply",
      makeConstant<int128_t>(std::nullopt, 1, DECIMAL(38, 0)),
      {makeConstant<int128_t>(
           HugeInt::build(0x08FFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF),
           1,
           DECIMAL(38, 0)),
       makeConstant<int64_t>(10, 1, DECIMAL(2, 0))});

  // Rescaling the final result overflows.
  testArithmeticFunction(
      "multiply",
      makeConstant<int128_t>(std::nullopt, 1, DECIMAL(38, 1)),
      {makeConstant<int128_t>(
           HugeInt::build(0x08FFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF),
           1,
           DECIMAL(38, 0)),
       makeConstant<int64_t>(100, 1, DECIMAL(2, 1))});
}

TEST_F(DecimalArithmeticTest, divide) {
  auto shortFlat = makeFlatVector<int64_t>({1000, 2000}, DECIMAL(17, 3));
  // Divide short and short, returning long.
  testArithmeticFunction(
      "divide",
      makeNullableLongDecimalVector(
          {"500000000000000000000", "2000000000000000000000"}, DECIMAL(38, 21)),
      {makeFlatVector<int64_t>({500, 4000}, DECIMAL(17, 3)), shortFlat});

  // Divide short and long, returning long.
  auto longFlat = makeFlatVector<int128_t>({500, 4000}, DECIMAL(20, 2));
  testArithmeticFunction(
      "divide",
      makeFlatVector<int128_t>(
          {500000000000000000, 2000000000000000000}, DECIMAL(38, 17)),
      {longFlat, shortFlat});

  // Divide long and short, returning long.
  testArithmeticFunction(
      "divide",
      makeNullableLongDecimalVector(
          {"20" + std::string(20, '0'), "5" + std::string(20, '0')},
          DECIMAL(38, 22)),
      {shortFlat, longFlat});

  // Divide long and long, returning long.
  testArithmeticFunction(
      "divide",
      makeNullableLongDecimalVector(
          {"5" + std::string(18, '0'), "3" + std::string(18, '0')},
          DECIMAL(38, 18)),
      {makeFlatVector<int128_t>({2500, 12000}, DECIMAL(20, 2)), longFlat});

  // Divide short and short, returning short.
  testArithmeticFunction(
      "divide",
      makeFlatVector<int64_t>({500000000, 300000000}, DECIMAL(13, 11)),
      {makeFlatVector<int64_t>({2500, 12000}, DECIMAL(5, 5)),
       makeFlatVector<int64_t>({500, 4000}, DECIMAL(5, 2))});
  // This result can be obtained by Spark unit test
  //   test("divide decimal big") {
  //     val s = Seq(35, 6, 20, 3)
  //     var builder = new StringBuffer()
  //     (0 until 29).foreach(_ => builder = builder.append("9"))
  //     builder.append(".")
  //     (0 until 6).foreach(_ => builder = builder.append("9"))
  //     val str1 = builder.toString

  //     val l1 = Literal.create(
  //       Decimal(BigDecimal(str1), s.head, s(1)),
  //       DecimalType(s.head, s(1)))
  //     val l2 = Literal.create(
  //       Decimal(BigDecimal(0.201), s(2), s(3)),
  //       DecimalType(s(2), s(3)))
  //     checkEvaluation(Divide(l1, l2), null)
  //   }
  testArithmeticFunction(
      "divide",
      makeNullableLongDecimalVector(
          {"497512437810945273631840796019900493"}, DECIMAL(38, 6)),
      {makeNullableLongDecimalVector({std::string(35, '9')}, DECIMAL(35, 6)),
       makeConstant<int128_t>(201, 1, DECIMAL(20, 3))});

  testArithmeticFunction(
      "divide",
      makeNullableLongDecimalVector(
          {"1000" + std::string(17, '0'), "500" + std::string(17, '0')},
          DECIMAL(24, 20)),
      {makeConstant<int64_t>(100, 2, DECIMAL(3, 2)), shortFlat});

  // Flat and Constant arguments.
  testArithmeticFunction(
      "divide",
      makeNullableLongDecimalVector(
          {"500" + std::string(4, '0'), "1000" + std::string(4, '0')},
          DECIMAL(23, 7)),
      {shortFlat, makeConstant<int64_t>(200, 2, DECIMAL(3, 2))});

  // Divide and round-up.
  // The result can be obtained by Spark unit test
  //     test("divide test") {
  //     spark.sql("create table decimals_test(a decimal(2,1)) using parquet;")
  //     spark.sql("insert into decimals_test values(6)")
  //     val df = spark.sql("select a / -6.0 from decimals_test")
  //     df.printSchema()
  //     df.show(truncate = false)
  //     spark.sql("drop table decimals_test;")
  //   }
  testArithmeticFunction(
      "divide",
      makeFlatVector<int64_t>(
          {566667, -83333, -1083333, -1500000, -33333, 816667}, DECIMAL(8, 6)),
      {makeFlatVector<int64_t>({-34, 5, 65, 90, 2, -49}, DECIMAL(2, 1)),
       makeConstant<int64_t>(-60, 6, DECIMAL(2, 1))});
  // Divide by zero.
  testArithmeticFunction(
      "divide",
      makeConstant<int128_t>(std::nullopt, 2, DECIMAL(21, 6)),
      {shortFlat, makeConstant<int64_t>(0, 2, DECIMAL(2, 1))});

  // Long decimal limits.
  testArithmeticFunction(
      "divide",
      makeConstant<int128_t>(std::nullopt, 1, DECIMAL(38, 6)),
      {makeConstant<int128_t>(DecimalUtil::kLongDecimalMax, 1, DECIMAL(38, 0)),
       makeConstant<int64_t>(1, 1, DECIMAL(3, 2))});
}

TEST_F(DecimalArithmeticTest, denyPrecisionLoss) {
  const std::string denyPrecisionLoss = "_deny_precision_loss";
  testArithmeticFunction(
      "add" + denyPrecisionLoss,
      makeFlatVector(
          std::vector<int128_t>{21232100, 29998888, 42345678, 42135632},
          DECIMAL(38, 7)),
      {makeFlatVector(
           std::vector<int128_t>{11232100, 9998888, 12345678, 2135632},
           DECIMAL(38, 7)),
       makeFlatVector(std::vector<int64_t>{1, 2, 3, 4}, DECIMAL(10, 0))});

  // Overflow when scaling up the whole part.
  testArithmeticFunction(
      "add" + denyPrecisionLoss,
      makeNullableLongDecimalVector(
          {"null", "null", "null", "null"}, DECIMAL(38, 7)),
      {makeNullableLongDecimalVector(
           {"-99999999999999999999999999999999990000",
            "99999999999999999999999999999999999000",
            "-99999999999999999999999999999999999900",
            "99999999999999999999999999999999999990"},
           DECIMAL(38, 3)),
       makeFlatVector(
           std::vector<int128_t>{-100, 9999999, -999900, 99999},
           DECIMAL(38, 7))});

  testArithmeticFunction(
      "subtract" + denyPrecisionLoss,
      makeFlatVector(
          std::vector<int128_t>{1232100, -10001112, -17654322, -37864368},
          DECIMAL(38, 7)),
      {makeFlatVector(
           std::vector<int128_t>{11232100, 9998888, 12345678, 2135632},
           DECIMAL(38, 7)),
       makeFlatVector(std::vector<int64_t>{1, 2, 3, 4}, DECIMAL(10, 0))});

  testArithmeticFunction(
      "multiply" + denyPrecisionLoss,
      makeConstant<int128_t>(60501, 1, DECIMAL(38, 10)),
      {makeConstant<int128_t>(201, 1, DECIMAL(20, 5)),
       makeConstant<int128_t>(301, 1, DECIMAL(20, 5))});

  // diff > 0
  testArithmeticFunction(
      "divide" + denyPrecisionLoss,
      makeConstant<int128_t>(
          HugeInt::parse("5" + std::string(18, '0')), 1, DECIMAL(38, 18)),
      {makeConstant<int128_t>(500, 1, DECIMAL(20, 2)),
       makeConstant<int64_t>(1000, 1, DECIMAL(17, 3))});
  // diff < 0
  testArithmeticFunction(
      "divide" + denyPrecisionLoss,
      makeConstant<int128_t>(
          HugeInt::parse("5" + std::string(10, '0')), 1, DECIMAL(31, 10)),
      {makeConstant<int128_t>(500, 1, DECIMAL(20, 2)),
       makeConstant<int64_t>(1000, 1, DECIMAL(7, 3))});
}

TEST_F(DecimalArithmeticTest, unaryMinus) {
  testArithmeticFunction(
      "unaryminus",
      makeFlatVector<int64_t>(
          {1111,
           -1112,
           9999,
           0,
           -DecimalUtil::kShortDecimalMin,
           -DecimalUtil::kShortDecimalMax},
          DECIMAL(18, 9)),
      {makeFlatVector<int64_t>(
          {-1111,
           1112,
           -9999,
           0,
           DecimalUtil::kShortDecimalMin,
           DecimalUtil::kShortDecimalMax},
          DECIMAL(18, 9))});

  testArithmeticFunction(
      "unaryminus",
      makeFlatVector<int128_t>(
          {11111111,
           -11112112,
           99999999,
           -DecimalUtil::kLongDecimalMax,
           -DecimalUtil::kLongDecimalMin},
          DECIMAL(38, 19)),
      {makeFlatVector<int128_t>(
          {-11111111,
           11112112,
           -99999999,
           DecimalUtil::kLongDecimalMax,
           DecimalUtil::kLongDecimalMin},
          DECIMAL(38, 19))});
}

TEST_F(DecimalArithmeticTest, ceil) {
  // short DECIMAL -> short DECIMAL.
  testArithmeticFunction(
      "ceil",
      {makeFlatVector<int64_t>({0, 1, 0, 1, 0, 1, 0, 1, 0}, DECIMAL(2, 0))},
      {makeFlatVector<int64_t>(
          {0, 1, -1, 49, -49, 50, -50, 99, -99}, DECIMAL(3, 2))});
  testArithmeticFunction(
      "ceil",
      {makeFlatVector<int64_t>(
          {123, -123, 124, -123, 124, -123, 124, -123, 124, -123, 124, -123},
          DECIMAL(4, 0))},
      {makeFlatVector<int64_t>(
          {12300,
           -12300,
           12301,
           -12301,
           12345,
           -12345,
           12349,
           -12349,
           12350,
           -12350,
           12399,
           -12399},
          DECIMAL(5, 2))});
  testArithmeticFunction(
      "ceil",
      {makeFlatVector<int64_t>(
          {DecimalUtil::kShortDecimalMax, DecimalUtil::kShortDecimalMin},
          DECIMAL(18, 0))},
      {makeFlatVector<int64_t>(
          {DecimalUtil::kShortDecimalMax, DecimalUtil::kShortDecimalMin},
          DECIMAL(18, 0))});

  // long DECIMAL -> long DECIMAL.
  testArithmeticFunction(
      "ceil",
      {makeFlatVector<int128_t>({0, 1, 0, 1, 0, 1, 0, 1, 0}, DECIMAL(19, 0))},
      {makeFlatVector<int128_t>(
          {0, 1, -1, 49, -49, 50, -50, 99, -99}, DECIMAL(20, 2))});
  testArithmeticFunction(
      "ceil",
      {makeFlatVector<int128_t>(
          {DecimalUtil::kPowersOfTen[33], -DecimalUtil::kPowersOfTen[33] + 1},
          DECIMAL(34, 0))},
      {makeFlatVector<int128_t>(
          {DecimalUtil::kLongDecimalMax, DecimalUtil::kLongDecimalMin},
          DECIMAL(38, 5))});
  testArithmeticFunction(
      "ceil",
      {makeFlatVector<int128_t>(
          {DecimalUtil::kLongDecimalMax, DecimalUtil::kLongDecimalMin},
          DECIMAL(38, 0))},
      {makeFlatVector<int128_t>(
          {DecimalUtil::kLongDecimalMax, DecimalUtil::kLongDecimalMin},
          DECIMAL(38, 0))});

  // long DECIMAL -> short DECIMAL.
  testArithmeticFunction(
      "ceil",
      {makeFlatVector<int64_t>({1, 1, 0, 0, 0}, DECIMAL(1, 0))},
      {makeFlatVector<int128_t>(
          {1234567890123456789,
           5000000000000000000,
           -9000000000000000000,
           -1000000000000000000,
           0},
          DECIMAL(19, 19))});
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
