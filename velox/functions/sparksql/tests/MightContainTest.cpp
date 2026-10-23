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

#include "velox/functions/sparksql/MightContain.h"

#include <random>

#include "velox/common/base/BloomFilter.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/Expressions.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class MightContainTest : public SparkFunctionBaseTest {
 protected:
  // Evaluates might_contain on 'rows' with a constant (or NULL, or column)
  // filter argument.
  VectorPtr evaluateMightContain(
      const std::optional<std::string>& serialized,
      const RowVectorPtr& data,
      const SelectivityVector& rows,
      bool filterColumn = false) {
    // Not using `evaluate()` because the bloom filter binary cannot be parsed
    // by DuckDB parser.
    std::vector<core::TypedExprPtr> args;
    if (filterColumn) {
      args.push_back(
          std::make_shared<core::FieldAccessTypedExpr>(VARBINARY(), "c1"));
    } else if (serialized.has_value()) {
      args.push_back(
          std::make_shared<core::ConstantTypedExpr>(
              VARBINARY(), variant::binary(serialized.value())));
    } else {
      args.push_back(
          std::make_shared<core::ConstantTypedExpr>(
              VARBINARY(), variant::null(TypeKind::VARBINARY)));
    }
    args.push_back(
        std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "c0"));
    auto expr = exec::ExprSet(
        {std::make_shared<core::CallTypedExpr>(
            BOOLEAN(), args, "might_contain")},
        &execCtx_);
    exec::EvalCtx evalCtx(&execCtx_, &expr, data.get());
    std::vector<VectorPtr> results(1);
    expr.eval(rows, evalCtx, results);
    return results[0];
  }

  void testMightContain(
      const std::optional<std::string>& serialized,
      const VectorPtr& value,
      const VectorPtr& expected) {
    auto result = evaluateMightContain(
        serialized, makeRowVector({value}), SelectivityVector(value->size()));
    velox::test::assertEqualVectors(expected, result);
  }

  // Checks 'rows' of the result against BloomFilterView::mayContain; rows with
  // a NULL value must be NULL.
  void assertMatchesBloomFilter(
      const std::string& serialized,
      const RowVectorPtr& data,
      const SelectivityVector& rows) {
    const BloomFilterView view(serialized.data());
    auto result = evaluateMightContain(serialized, data, rows);
    auto values = data->childAt(0)->asFlatVector<int64_t>();
    rows.applyToSelected([&](vector_size_t row) {
      if (values->isNullAt(row)) {
        ASSERT_TRUE(result->isNullAt(row)) << "row " << row;
      } else {
        ASSERT_FALSE(result->isNullAt(row)) << "row " << row;
        ASSERT_EQ(
            result->asFlatVector<bool>()->valueAt(row),
            view.mayContain(folly::hasher<int64_t>()(values->valueAt(row))))
            << "row " << row;
      }
    });
  }

  static std::string serializedFilter(
      int32_t capacity,
      const std::vector<int64_t>& keys) {
    BloomFilter filter;
    filter.reset(capacity);
    for (auto key : keys) {
      filter.insert(folly::hasher<int64_t>()(key));
    }
    std::string data;
    data.resize(filter.serializedSize());
    filter.serialize(data.data());
    return data;
  }

  std::string getSerializedBloomFilter(int32_t kSize) {
    BloomFilter bloomFilter;
    bloomFilter.reset(kSize);
    for (auto i = 0; i < kSize; ++i) {
      bloomFilter.insert(folly::hasher<int64_t>()(i));
    }
    std::string data;
    data.resize(bloomFilter.serializedSize());
    bloomFilter.serialize(data.data());
    return data;
  }
};

TEST_F(MightContainTest, basic) {
  constexpr int32_t kSize = 10;
  auto serialized = getSerializedBloomFilter(kSize);
  auto value =
      makeFlatVector<int64_t>(kSize, [](vector_size_t row) { return row; });
  auto expectedContain = makeConstant(true, kSize);
  testMightContain(serialized, value, expectedContain);

  auto valueNotContain = makeFlatVector<int64_t>(
      kSize, [](vector_size_t row) { return row + 123451; });
  auto expectedNotContain = makeConstant(false, kSize);
  testMightContain(serialized, valueNotContain, expectedNotContain);

  auto values = makeNullableFlatVector<int64_t>(
      {1, 2, 3, 4, 5, std::nullopt, 123451, 23456, 4, 5});
  auto expected = makeNullableFlatVector<bool>(
      {true, true, true, true, true, std::nullopt, false, false, true, true});
  testMightContain(serialized, values, expected);
}

TEST_F(MightContainTest, nullBloomFilter) {
  auto value = makeFlatVector<int64_t>({2, 4});
  auto expected = makeNullConstant(TypeKind::BOOLEAN, value->size());
  testMightContain(std::nullopt, value, expected);
}

TEST_F(MightContainTest, probeKernelsMatchBloomFilter) {
  std::mt19937_64 random(7);
  for (int32_t capacity : {1, 7, 1'000, 100'000, 4'000'000}) {
    std::vector<int64_t> keys(std::min(capacity, 50'000));
    for (auto& key : keys) {
      key = static_cast<int64_t>(random());
    }
    const auto data = serializedFilter(capacity, keys);
    const BloomFilterView view(data.data());
    for (int32_t size : {0, 1, 2, 3, 31, 32, 33, 1'023, 4'097}) {
      std::vector<int64_t> values(size);
      for (int32_t i = 0; i < size; ++i) {
        values[i] = i % 3 == 0 && !keys.empty()
            ? keys[random() % keys.size()]
            : static_cast<int64_t>(random()) >> (i % 7);
      }
      if (size > 4) {
        values[1] = std::numeric_limits<int64_t>::min();
        values[2] = std::numeric_limits<int64_t>::max();
        values[3] = -1;
        values[4] = 0;
      }
      std::vector<uint8_t> generic(size + 1, 9), sve(size + 1, 9);
      bloom_probe::probeGeneric(
          view.bitsData(), view.size(), values.data(), size, generic.data());
      bloom_probe::probeSve(
          view.bitsData(), view.size(), values.data(), size, sve.data());
      for (int32_t i = 0; i < size; ++i) {
        const bool expected =
            view.mayContain(folly::hasher<int64_t>()(values[i]));
        ASSERT_EQ(generic[i], expected) << capacity << " " << size << " " << i;
        ASSERT_EQ(sve[i], expected) << capacity << " " << size << " " << i;
        ASSERT_EQ(
            bloom_probe::test(view.bitsData(), view.size(), values[i]),
            expected);
      }
      // Neither kernel writes past the requested size.
      ASSERT_EQ(generic[size], 9);
      ASSERT_EQ(sve[size], 9);
    }
  }
}

TEST_F(MightContainTest, flatNullableAndSelectedRows) {
  std::vector<int64_t> keys;
  for (int64_t i = -500; i < 2'000; i += 3) {
    keys.push_back(i);
  }
  const auto serialized = serializedFilter(4'096, keys);
  const vector_size_t size = 5'000;
  auto flat = makeFlatVector<int64_t>(
      size, [](vector_size_t row) { return row - 1'000; });
  auto nullable = makeFlatVector<int64_t>(
      size,
      [](vector_size_t row) { return row * 7 - 3'000; },
      [](vector_size_t row) { return row % 11 == 0; });
  for (const auto& values : {flat, nullable}) {
    auto data = makeRowVector({values});
    SelectivityVector all(size);
    assertMatchesBloomFilter(serialized, data, all);
    SelectivityVector partial(size);
    for (vector_size_t row = 0; row < size; row += 3) {
      partial.setValid(row, false);
    }
    partial.updateBounds();
    assertMatchesBloomFilter(serialized, data, partial);
  }
}

TEST_F(MightContainTest, dictionaryAndConstantInputs) {
  const auto serialized = serializedFilter(64, {1, 2, 3, 40, 50});
  auto base = makeNullableFlatVector<int64_t>({1, 2, std::nullopt, 40, 7, 50});
  auto indices =
      makeIndices(3'000, [](vector_size_t row) { return (row * 5) % 6; });
  auto dictionary = wrapInDictionary(indices, 3'000, base);
  auto expected = makeFlatVector<bool>(
      3'000,
      [&](vector_size_t row) {
        const auto value = base->valueAt((row * 5) % 6);
        return value == 1 || value == 2 || value == 40 || value == 50;
      },
      [&](vector_size_t row) { return base->isNullAt((row * 5) % 6); });
  testMightContain(serialized, dictionary, expected);
  testMightContain(
      serialized, makeConstant<int64_t>(40, 100), makeConstant(true, 100));
}

TEST_F(MightContainTest, nonConstantFilter) {
  // A filter column is not constant: non-NULL rows are false.
  auto values = makeNullableFlatVector<int64_t>({1, std::nullopt, 3});
  const auto serialized = serializedFilter(16, {1, 3});
  auto filters = makeFlatVector<StringView>(
      3,
      [&](vector_size_t) { return StringView(serialized); },
      nullptr,
      VARBINARY());
  auto result = evaluateMightContain(
      std::nullopt,
      makeRowVector({values, filters}),
      SelectivityVector(3),
      true);
  velox::test::assertEqualVectors(
      makeNullableFlatVector<bool>({false, std::nullopt, false}), result);
}

TEST_F(MightContainTest, tooSmallFilterIsUserError) {
  auto data = makeRowVector({makeFlatVector<int64_t>({1})});
  VELOX_ASSERT_USER_THROW(
      evaluateMightContain(std::string("ab"), data, SelectivityVector(1)),
      "Serialized BloomFilter is too small");
}

TEST_F(MightContainTest, registeredAsVectorFunction) {
  auto signatures = exec::getVectorFunctionSignatures("might_contain");
  ASSERT_TRUE(signatures.has_value());
  EXPECT_EQ(signatures->size(), 1);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
