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
#include "velox/exec/WindowPartition.h"
#include "velox/exec/window/KRangeFrameBound.h"
#include "velox/exec/window/PeerGroupComputation.h"

#include <algorithm>

namespace facebook::velox::exec {

WindowPartition::WindowPartition(
    RowContainer* data,
    const folly::Range<char**>& rows,
    const std::vector<column_index_t>& inputMapping,
    const std::vector<std::pair<column_index_t, core::SortOrder>>& sortKeyInfo)
    : partial_(false),
      data_(data),
      partition_(rows),
      complete_(true),
      inputMapping_(inputMapping),
      sortKeyInfo_(sortKeyInfo) {
  VELOX_CHECK(!partition_.empty());

  for (auto index : inputMapping_) {
    columns_.emplace_back(data_->columnAt(index));
  }
}

WindowPartition::WindowPartition(
    const std::vector<column_index_t>& inputMapping,
    const std::vector<std::pair<column_index_t, core::SortOrder>>& sortKeyInfo,
    bool partial)
    : partial_(partial),
      data_(nullptr),
      partition_(),
      complete_(!partial),
      inputMapping_(inputMapping),
      sortKeyInfo_(sortKeyInfo) {}

void WindowPartition::removeProcessedRows(vector_size_t /*numRows*/) {
  VELOX_FAIL("Only a partial WindowPartition can remove processed rows");
}

vector_size_t WindowPartition::numRowsForProcessing(
    vector_size_t partitionOffset) const {
  return partition_.size() - partitionOffset;
}

void WindowPartition::extractColumn(
    int32_t columnIndex,
    folly::Range<const vector_size_t*> rowNumbers,
    vector_size_t resultOffset,
    const VectorPtr& result) const {
  data_->extractColumn(
      partition_.data(),
      rowNumbers,
      inputMapping_[columnIndex],
      result,
      resultOffset);
}

void WindowPartition::extractColumn(
    int32_t columnIndex,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    vector_size_t resultOffset,
    const VectorPtr& result) const {
  data_->extractColumn(
      partition_.data() + partitionOffset,
      numRows,
      inputMapping_[columnIndex],
      result,
      resultOffset);
}

vector_size_t WindowPartition::extractNulls(
    int32_t columnIndex,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    const BufferPtr& nullsBuffer) const {
  return data_->extractNulls(
      partition_.data() + partitionOffset,
      numRows,
      inputMapping_[columnIndex],
      nullsBuffer);
}

namespace {

std::pair<vector_size_t, vector_size_t> findMinMaxFrameBounds(
    const SelectivityVector& validRows,
    const BufferPtr& frameStarts,
    const BufferPtr& frameEnds) {
  auto rawFrameStarts = frameStarts->as<vector_size_t>();
  auto rawFrameEnds = frameEnds->as<vector_size_t>();

  auto firstValidRow = validRows.begin();
  vector_size_t minFrame = rawFrameStarts[firstValidRow];
  vector_size_t maxFrame = rawFrameEnds[firstValidRow];
  validRows.applyToSelected([&](auto i) {
    minFrame = std::min(minFrame, rawFrameStarts[i]);
    maxFrame = std::max(maxFrame, rawFrameEnds[i]);
  });
  return {minFrame, maxFrame};
}

} // namespace

std::optional<std::pair<vector_size_t, vector_size_t>>
WindowPartition::extractNulls(
    column_index_t col,
    const SelectivityVector& validRows,
    const BufferPtr& frameStarts,
    const BufferPtr& frameEnds,
    BufferPtr* nulls) const {
  VELOX_CHECK(validRows.hasSelections(), "Buffer has no active rows");
  auto [minFrame, maxFrame] =
      findMinMaxFrameBounds(validRows, frameStarts, frameEnds);

  // Add 1 since maxFrame is the index of the frame end row.
  auto framesSize = maxFrame - minFrame + 1;
  AlignedBuffer::reallocate<bool>(nulls, framesSize);

  if (extractNulls(col, minFrame, framesSize, *nulls) == 0) {
    return std::nullopt;
  }
  return std::make_pair(minFrame, framesSize);
}

bool WindowPartition::compareRowsWithSortKeys(const char* lhs, const char* rhs)
    const {
  if (lhs == rhs) {
    return false;
  }
  for (auto& key : sortKeyInfo_) {
    if (auto result = data_->compare(
            lhs,
            rhs,
            key.first,
            {key.second.isNullsFirst(), key.second.isAscending(), false})) {
      return result < 0;
    }
  }
  return false;
}

class WindowPartition::RowContainerAccessor {
 public:
  explicit RowContainerAccessor(const WindowPartition& partition)
      : partition_(partition) {}

  vector_size_t startRow() const {
    return 0;
  }

  vector_size_t partitionEnd() const {
    return partition_.partition_.size();
  }

  // A complete partition holds all its rows, so no row from an earlier batch
  // is ever needed for peer comparison.
  bool hasPreviousRow() const {
    return false;
  }

  bool previousRowEquals(vector_size_t /*row*/) const {
    VELOX_UNREACHABLE();
  }

  bool rowsEqual(vector_size_t lhs, vector_size_t rhs) const {
    return !partition_.compareRowsWithSortKeys(rowAt(lhs), rowAt(rhs));
  }

  std::optional<int32_t> compareFrameValue(
      vector_size_t orderByRow,
      vector_size_t frameValueRow,
      column_index_t frameColumn,
      const CompareFlags& flags) const {
    return partition_.data_->compare(
        rowAt(orderByRow),
        rowAt(frameValueRow),
        orderByColumn(),
        partition_.inputMapping_[frameColumn],
        flags);
  }

  bool frameValueIsNull(vector_size_t row, column_index_t frameColumn) const {
    return isNullAt(rowAt(row), partition_.columns_[frameColumn]);
  }

  bool orderByValueIsNull(vector_size_t row) const {
    return isNullAt(rowAt(row), orderByRowColumn());
  }

  template <typename T>
  bool frameValueIsNan(vector_size_t row, column_index_t frameColumn) const {
    return partition_.data_->isNanAt<T>(
        rowAt(row), partition_.columns_[frameColumn]);
  }

  template <typename T>
  bool orderByValueIsNan(vector_size_t row) const {
    return partition_.data_->isNanAt<T>(rowAt(row), orderByRowColumn());
  }

 private:
  column_index_t orderByColumn() const {
    return partition_.sortKeyInfo_[0].first;
  }

  RowColumn orderByRowColumn() const {
    return partition_.data_->columnAt(orderByColumn());
  }

  char* rowAt(vector_size_t row) const {
    return partition_.partition_[row];
  }

  static bool isNullAt(const char* row, RowColumn column) {
    return RowContainer::isNullAt(row, column);
  }

  const WindowPartition& partition_;
};

std::pair<vector_size_t, vector_size_t> WindowPartition::computePeerBuffers(
    vector_size_t start,
    vector_size_t end,
    vector_size_t prevPeerStart,
    vector_size_t prevPeerEnd,
    vector_size_t* rawPeerStarts,
    vector_size_t* rawPeerEnds) {
  RowContainerAccessor rows{*this};
  const auto result = window::PeerGroupComputation::compute(
      rows, start, end, prevPeerStart, prevPeerEnd, rawPeerStarts, rawPeerEnds);
  return {result.peerStart, result.peerEnd};
}

void WindowPartition::computeKRangeFrameBounds(
    bool isStartBound,
    bool isPreceding,
    column_index_t frameColumn,
    vector_size_t startRow,
    vector_size_t numRows,
    const vector_size_t* rawPeerBuffer,
    vector_size_t* rawFrameBounds,
    SelectivityVector& validFrames) const {
  const auto sortOrder = sortKeyInfo_[0].second;
  const auto frameType = data_->columnTypes()[inputMapping_[frameColumn]];

  RowContainerAccessor rows{*this};
  window::KRangeFrameBound::compute(
      rows,
      isStartBound,
      isPreceding,
      frameColumn,
      sortOrder,
      frameType,
      startRow,
      numRows,
      rawPeerBuffer,
      rawFrameBounds,
      validFrames);
}

} // namespace facebook::velox::exec
