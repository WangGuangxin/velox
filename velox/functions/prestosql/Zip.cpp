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
#include <limits>
#include "velox/expression/DecodedArgs.h"
#include "velox/expression/VectorFunction.h"

namespace facebook::velox::functions {

namespace {

// Returns the arguments as array vectors if all are flat and use the offsets
// and sizes buffers of the first one. Otherwise, returns an empty vector. Field
// projections of one array of structs (e.g. Spark's get_array_struct_fields)
// share that array's buffers this way, so every row has equal arrays at equal
// offsets.
std::vector<const ArrayVector*> flatArraysWithSharedLayout(
    const std::vector<VectorPtr>& args) {
  std::vector<const ArrayVector*> arrays;
  arrays.reserve(args.size());
  for (const auto& arg : args) {
    if (arg->encoding() != VectorEncoding::Simple::ARRAY) {
      return {};
    }
    arrays.push_back(arg->asUnchecked<ArrayVector>());
    if (arrays.back()->offsets() != arrays[0]->offsets() ||
        arrays.back()->sizes() != arrays[0]->sizes()) {
      return {};
    }
  }
  return arrays;
}

// Makes the result from the element vectors of 'baseVectors' as is, without
// padding or copying. Requires that in every row all arrays start at the same
// offset and have the same size.
VectorPtr zipElements(
    const std::vector<const ArrayVector*>& baseVectors,
    BufferPtr offsets,
    BufferPtr sizes,
    vector_size_t size,
    const TypePtr& outputType,
    memory::MemoryPool* pool) {
  std::vector<VectorPtr> elements;
  elements.reserve(baseVectors.size());
  // Since the offsets and sizes are all the same, using the minimum size is
  // big enough to contain all elements, while also guaranteeing all child
  // Vectors in the RowVector are at least this big.
  vector_size_t minElementsSize = std::numeric_limits<vector_size_t>::max();
  for (const auto* baseVector : baseVectors) {
    elements.push_back(baseVector->elements());
    minElementsSize = std::min(minElementsSize, elements.back()->size());
  }
  auto rowVector = std::make_shared<RowVector>(
      pool,
      outputType->childAt(0),
      BufferPtr(nullptr),
      minElementsSize,
      std::move(elements));
  return std::make_shared<ArrayVector>(
      pool,
      outputType,
      BufferPtr(nullptr),
      size,
      std::move(offsets),
      std::move(sizes),
      std::move(rowVector));
}

class ZipFunction : public exec::VectorFunction {
  static const auto kMinArity = 2;
  static const auto kMaxArity = 7;

 public:
  /// This class implements the zip function.
  ///
  /// DEFINITION:
  /// zip(ARRAY[T], ARRAY[U]) -> ARRAY(ROW[T,U])
  /// where we create a ROW[Ti, Ui] for every ith element in ARRAY[T], ARRAY[U].
  /// The smaller array is padded with nulls.
  ///
  /// IMPLEMENTATION:
  ///  1. The general idea is to create a new dictionary vector for each input
  ///  array vector and enumerate their indices to create a 1:1 mapping.
  ///  2. To do this, for each row we determine which is the largest Array
  ///  and subsequently pad the smaller arrays with nulls.
  ///  3. Then we take the resultant padded vectors together and create one ROW
  ///  Vector.
  ///  4. This forms the base to create the final output Array vector, whose
  ///  Arrays are the size of the largest input Array.
  ///
  ///  Note:
  ///   - We make no copy's of any constituent elements and are agnostic to
  ///   types.
  ///   - For compatibility with Presto a maximum arity of 7 is enforced.

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    const vector_size_t numInputArrays = args.size();

    auto* pool = context.pool();

    if (const auto arrays = flatArraysWithSharedLayout(args); !arrays.empty()) {
      context.moveOrCopyResult(
          zipElements(
              arrays,
              arrays[0]->offsets(),
              arrays[0]->sizes(),
              rows.end(),
              outputType,
              pool),
          rows,
          result);
      return;
    }

    exec::DecodedArgs decodedArgs(rows, args, context);
    std::vector<const ArrayVector*> baseVectors(numInputArrays);
    std::vector<const vector_size_t*> rawSizes(numInputArrays);
    std::vector<const vector_size_t*> rawOffsets(numInputArrays);
    std::vector<const vector_size_t*> indices(numInputArrays);

    for (int i = 0; i < numInputArrays; i++) {
      baseVectors[i] = decodedArgs.at(i)->base()->as<ArrayVector>();
      rawSizes[i] = baseVectors[i]->rawSizes();
      rawOffsets[i] = baseVectors[i]->rawOffsets();
      indices[i] = decodedArgs.at(i)->indices();
    }

    // Size of elements in result vector.
    vector_size_t resultElementsSize = 0;
    // This is true if for all rows, all the arrays within a row are the same
    // size.
    bool allSameSize = true;
    // This is true if for all rows, all the arrays within a row have the same
    // starting offset in their elements Vector.
    bool allSameOffsets = true;

    // Determine what the size of the resultant elements will be so we can
    // reserve enough space.
    auto getMaxArraySize = [&](vector_size_t row) -> vector_size_t {
      vector_size_t maxSize = 0;
      vector_size_t offset = -1;
      for (int i = 0; i < numInputArrays; i++) {
        vector_size_t size = rawSizes[i][indices[i][row]];
        allSameSize &= i == 0 || maxSize == size;
        allSameOffsets &= i == 0 || offset == rawOffsets[i][indices[i][row]];
        maxSize = std::max(maxSize, size);
        offset = rawOffsets[i][indices[i][row]];
      }
      return maxSize;
    };

    BufferPtr resultArraySizesBuffer = allocateSizes(rows.end(), pool);
    auto rawResultArraySizes =
        resultArraySizesBuffer->asMutable<vector_size_t>();
    rows.applyToSelected([&](auto row) {
      auto maxSize = getMaxArraySize(row);
      resultElementsSize += maxSize;
      rawResultArraySizes[row] = maxSize;
    });

    if (allSameSize && allSameOffsets) {
      // For all rows, all arrays within a row are the same size and start at
      // the same offset. In this case we don't have to add nulls, we can just
      // pass in the element Vectors as is to be the fields of the output Rows.
      // If all input Vectors are flat, they also share the offsets of the
      // first one.
      bool allFlat = true;
      for (const auto& arg : args) {
        allFlat &= arg->encoding() == VectorEncoding::Simple::ARRAY;
      }

      BufferPtr offsets;
      if (allFlat) {
        offsets = baseVectors[0]->offsets();
      } else {
        offsets = allocateOffsets(rows.end(), pool);
        auto* rawResultOffsets = offsets->asMutable<vector_size_t>();
        rows.applyToSelected([&](auto row) {
          rawResultOffsets[row] = rawOffsets[0][indices[0][row]];
        });
      }
      context.moveOrCopyResult(
          zipElements(
              baseVectors,
              std::move(offsets),
              resultArraySizesBuffer,
              rows.end(),
              outputType,
              pool),
          rows,
          result);
      return;
    }

    // Create individual result vectors for each input Array vector.

    std::vector<BufferPtr> nestedResultIndices(numInputArrays);
    std::vector<BufferPtr> nestedResultNulls(numInputArrays);
    std::vector<vector_size_t*> rawNestedResultIndices(numInputArrays);
    std::vector<uint64_t*> rawNestedResultNulls(numInputArrays);

    for (int i = 0; i < numInputArrays; i++) {
      nestedResultIndices[i] = allocateIndices(resultElementsSize, pool);
      nestedResultNulls[i] = AlignedBuffer::allocate<bool>(
          resultElementsSize, pool, bits::kNotNull);
      rawNestedResultIndices[i] =
          nestedResultIndices[i]->asMutable<vector_size_t>();
      rawNestedResultNulls[i] = nestedResultNulls[i]->asMutable<uint64_t>();
    }

    const auto resultArraySize = rows.end();
    BufferPtr resultArrayOffsets = allocateOffsets(resultArraySize, pool);
    auto rawResultArrayOffsets = resultArrayOffsets->asMutable<vector_size_t>();

    // Create right offsets/indexes for the individual and final result arrays.
    int elementRow = 0;
    rows.applyToSelected([&](auto row) {
      // Get the max size for that row.
      auto maxArraySize = rawResultArraySizes[row];
      rawResultArrayOffsets[row] = elementRow;

      for (int i = 0; i < numInputArrays; i++) {
        auto offset = rawOffsets[i][indices[i][row]];
        auto size = rawSizes[i][indices[i][row]];
        std::iota(
            rawNestedResultIndices[i] + elementRow,
            rawNestedResultIndices[i] + elementRow + size,
            offset);
        bits::fillBits(
            rawNestedResultNulls[i],
            elementRow + size,
            elementRow + maxArraySize,
            bits::kNull);
      }
      elementRow += maxArraySize;
    });

    // Create result dictionary vectors.
    std::vector<VectorPtr> resultDictionaryVectors(numInputArrays);

    for (int i = 0; i < numInputArrays; i++) {
      resultDictionaryVectors[i] = BaseVector::wrapInDictionary(
          nestedResultNulls[i],
          nestedResultIndices[i],
          resultElementsSize,
          baseVectors[i]->elements());
    }

    auto rowType = outputType->childAt(0);
    auto rowVector = std::make_shared<RowVector>(
        pool,
        rowType,
        BufferPtr(nullptr),
        resultElementsSize,
        resultDictionaryVectors);

    // Now convert these to an Array
    auto arrayVector = std::make_shared<ArrayVector>(
        pool,
        outputType,
        BufferPtr(nullptr),
        rows.end(),
        resultArrayOffsets,
        resultArraySizesBuffer,
        std::move(rowVector));

    context.moveOrCopyResult(arrayVector, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    static const auto kAritySize = (kMaxArity - kMinArity) + 1;
    std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;
    signatures.reserve(kAritySize);

    // Build all signatures from kMinArity to kMaxArity.
    for (int i = kMinArity; i <= kMaxArity; i++) {
      auto builder = exec::FunctionSignatureBuilder();
      std::vector<std::string> allTypeVars;
      allTypeVars.reserve(i);

      for (int j = 0; j < i; j++) {
        allTypeVars.emplace_back(fmt::format("E{:02d}", j));
        builder.typeVariable(allTypeVars.back());
        builder.argumentType(fmt::format("array({})", allTypeVars.back()));
      }

      const auto returnType = folly::join(",", allTypeVars);
      builder.returnType(fmt::format("array(row({}))", returnType));
      signatures.emplace_back(builder.build());
    }

    return signatures;
  }
};

} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_zip,
    ZipFunction::signatures(),
    std::make_unique<ZipFunction>());
} // namespace facebook::velox::functions
