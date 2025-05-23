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

#include "velox/dwio/common/SelectiveColumnReader.h"

namespace facebook::nimble {
class DeduplicatedReadHelper;
}

namespace facebook::velox::dwio::common {

// Abstract superclass for list and map readers. Encapsulates common
// logic for dealing with mapping between enclosing and nested rows.
class SelectiveRepeatedColumnReader : public SelectiveColumnReader {
 public:
  const std::vector<SelectiveColumnReader*>& children() const override {
    return children_;
  }

 protected:
  // Buffer size for reading lengths when skipping.
  static constexpr int32_t kBufferSize = 1024;

  SelectiveRepeatedColumnReader(
      const TypePtr& requestedType,
      FormatParams& params,
      velox::common::ScanSpec& scanSpec,
      std::shared_ptr<const dwio::common::TypeWithId> type)
      : SelectiveColumnReader(requestedType, std::move(type), params, scanSpec),
        nestedRowsHolder_(memoryPool_) {}

  /// Reads 'numLengths' next lengths into 'result'. If 'nulls' is
  /// non-null, each kNull bit signifies a null with a length of 0 to
  /// be inserted at the corresponding position in the result. 'nulls'
  /// is expected to be null flags for 'numRows' next rows at the
  /// level of this reader.
  virtual void
  readLengths(int32_t* lengths, int32_t numLengths, const uint64_t* nulls) = 0;

  /// Create row set for child columns based on the row set of parent column.
  virtual void makeNestedRowSet(const RowSet& rows, int32_t maxRow);

  /// Compute the offsets and lengths based on the current filtered rows passed
  /// in.
  void makeOffsetsAndSizes(const RowSet& rows, ArrayVectorBase&);

  /// Creates a struct if '*result' is empty and 'type' is a row.
  void prepareStructResult(const TypePtr& type, VectorPtr* result) {
    if (!*result && type->kind() == TypeKind::ROW) {
      *result = BaseVector::create(type, 0, memoryPool_);
    }
  }

  // Apply filter on parent level.  Child filtering should be handled separately
  // in subclasses.
  RowSet applyFilter(const RowSet& rows);

  vector_size_t prunedLengthAt(vector_size_t i) const {
    return std::min(scanSpec_->maxArrayElementsCount(), allLengths_[i]);
  }

  static vector_size_t
  advanceNestedRows(const RowSet& rows, vector_size_t i, vector_size_t last) {
    while (i + 16 < rows.size() && rows[i + 16] < last) {
      i += 16;
    }
    while (i < rows.size() && rows[i] < last) {
      ++i;
    }
    return i;
  }

  void ensureAllLengthsBuffer(vector_size_t size);

  BufferPtr allLengthsHolder_;
  vector_size_t* allLengths_;
  RowSet nestedRows_;
  raw_vector<vector_size_t> nestedRowsHolder_;
  bool nestedRowsAllSelected_;

  // The position in the child readers that corresponds to the position in the
  // length stream. The child readers can be behind if the last parents were
  // null, so that the child stream was only read up to the last position
  // corresponding to the last non-null parent.
  int64_t childTargetReadOffset_ = 0;
  std::vector<SelectiveColumnReader*> children_;

  friend class facebook::nimble::DeduplicatedReadHelper;
};

class SelectiveListColumnReader : public SelectiveRepeatedColumnReader {
 public:
  SelectiveListColumnReader(
      const TypePtr& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      FormatParams& params,
      velox::common::ScanSpec& scanSpec);

  void resetFilterCaches() override {
    child_->resetFilterCaches();
  }

  uint64_t skip(uint64_t numValues) override;

  virtual void read(
      int64_t offset,
      const RowSet& rows,
      const uint64_t* incomingNulls) override;

  void getValues(const RowSet& rows, VectorPtr* result) override;

 protected:
  std::unique_ptr<SelectiveColumnReader> child_;
};

class SelectiveMapColumnReader : public SelectiveRepeatedColumnReader {
 public:
  SelectiveMapColumnReader(
      const TypePtr& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      FormatParams& params,
      velox::common::ScanSpec& scanSpec);

  void resetFilterCaches() override {
    keyReader_->resetFilterCaches();
    elementReader_->resetFilterCaches();
  }

  uint64_t skip(uint64_t numValues) override;

  void read(int64_t offset, const RowSet& rows, const uint64_t* incomingNulls)
      override;

  void getValues(const RowSet& rows, VectorPtr* result) override;

 protected:
  std::unique_ptr<SelectiveColumnReader> keyReader_;
  std::unique_ptr<SelectiveColumnReader> elementReader_;
};

} // namespace facebook::velox::dwio::common
