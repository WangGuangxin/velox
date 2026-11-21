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

#include "velox/exec/RowContainer.h"
#include "velox/vector/BaseVector.h"

/// Simple WindowPartition that builds over the RowContainer used for storing
/// the input rows in the Window Operator. This works completely in-memory.
/// Subclasses such as VectorWindowPartition provide partial partitions, which
/// let RowsStreamingWindowBuild start processing a subset of a partition's
/// rows before the rest of the partition arrives.

/// TODO: This implementation will be revised for Spill to disk semantics.

namespace facebook::velox::exec {

class WindowPartition {
 public:
  virtual ~WindowPartition() = default;

  /// The WindowPartition is used by the Window operator and WindowFunction
  /// objects to access the underlying data and columns of a partition of rows.
  /// The WindowPartition is constructed by WindowBuild from the input data.
  /// 'data' : Underlying RowContainer of the WindowBuild.
  /// 'rows' : Pointers to rows in the RowContainer belonging to this partition.
  /// 'inputMapping' : Mapping from Window input column to the column position
  /// in 'data' for it. This is required because the WindowBuild re-orders
  /// the columns in 'data' for use with the spiller.
  /// 'sortKeyInfo' : Order by columns used by the Window operator. Used to
  /// get peer rows from the input partition.
  WindowPartition(
      RowContainer* data,
      const folly::Range<char**>& rows,
      const std::vector<column_index_t>& inputMapping,
      const std::vector<std::pair<column_index_t, core::SortOrder>>&
          sortKeyInfo);

  /// Removes the first 'numRows' processed rows from a partial window
  /// partition. Only partial subclasses support this.
  virtual void removeProcessedRows(vector_size_t numRows);

  /// Returns the number of rows in the current WindowPartition.
  virtual vector_size_t numRows() const {
    return partition_.size();
  }

  /// Returns the number of rows in a window partition remaining for data
  /// processing.
  virtual vector_size_t numRowsForProcessing(
      vector_size_t partitionOffset) const;

  bool complete() const {
    return complete_;
  }

  bool partial() const {
    return partial_;
  }

  void setComplete() {
    VELOX_CHECK(!complete_);
    checkPartial();

    complete_ = true;
  }

  /// Copies the values at 'columnIndex' into 'result' (starting at
  /// 'resultOffset') for the absolute partition row positions in the
  /// 'rowNumbers' array. Negative row positions are copied as nulls.
  virtual void extractColumn(
      int32_t columnIndex,
      folly::Range<const vector_size_t*> rowNumbers,
      vector_size_t resultOffset,
      const VectorPtr& result) const;

  /// Copies the values at 'columnIndex' into 'result' (starting at
  /// 'resultOffset') for 'numRows' starting at positions 'partitionOffset'
  /// in the partition input data.
  virtual void extractColumn(
      int32_t columnIndex,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      vector_size_t resultOffset,
      const VectorPtr& result) const;

  /// Writes to 'nullsBuffer' a nulls buffer of the column at 'columnIndex'
  /// for 'numRows' rows starting at 'partitionOffset' in the partition.
  /// Returns the number of null rows.
  virtual vector_size_t extractNulls(
      int32_t columnIndex,
      vector_size_t partitionOffset,
      vector_size_t numRows,
      const BufferPtr& nullsBuffer) const;

  /// Writes to 'nulls' a nulls buffer of column 'col' for the rows from the
  /// smallest 'frameStarts' value to the greatest 'frameEnds' value of
  /// 'validRows'. Both 'frameStarts' and 'frameEnds' are buffers of
  /// vector_size_t.
  ///
  /// Returns the first row and the number of rows written if any of them is
  /// null, and std::nullopt otherwise.
  std::optional<std::pair<vector_size_t, vector_size_t>> extractNulls(
      column_index_t col,
      const SelectivityVector& validRows,
      const BufferPtr& frameStarts,
      const BufferPtr& frameEnds,
      BufferPtr* nulls) const;

  /// Sets in 'rawPeerStarts' and in 'rawPeerEnds' the peer start and peer end
  /// offsets of the rows between 'start' and 'end' of the current partition.
  /// 'peer' row are all the rows having the same value of the order by columns
  /// as the current row.
  /// computePeerBuffers is called multiple times for each partition. It is
  /// called in sequential order of start to end rows.
  /// The peerStarts/peerEnds of the startRow could be same as the last row in
  /// the previous call to computePeerBuffers (if they have the same order by
  /// keys). So peerStart and peerEnd of the last row of this call are returned
  /// to be passed as prevPeerStart and prevPeerEnd to the subsequent
  /// call to computePeerBuffers.
  virtual std::pair<vector_size_t, vector_size_t> computePeerBuffers(
      vector_size_t start,
      vector_size_t end,
      vector_size_t prevPeerStart,
      vector_size_t prevPeerEnd,
      vector_size_t* rawPeerStarts,
      vector_size_t* rawPeerEnds);

  /// Sets in 'rawFrameBounds' the frame boundary for the k range
  /// preceding/following frame.
  /// @param isStartBound start or end boundary of the frame.
  /// @param isPreceding preceding or following boundary.
  /// @param frameColumn column which has the range boundary for that row.
  /// @param startRow starting row in the partition for this buffer computation.
  /// @param numRows number of rows to compute buffer for.
  /// @param rawPeerStarts buffer of peer row values for each row. If the frame
  /// column is null, then its peer row value is the frame boundary.
  /// @param validFrames SelectivityVector to keep track of valid frames.
  /// This function unselect rows in validFrames where the frame bounds are NaN
  /// that are invalid.
  virtual void computeKRangeFrameBounds(
      bool isStartBound,
      bool isPreceding,
      column_index_t frameColumn,
      vector_size_t startRow,
      vector_size_t numRows,
      const vector_size_t* rawPeerStarts,
      vector_size_t* rawFrameBounds,
      SelectivityVector& validFrames) const;

 protected:
  // Constructs a non-RowContainer-backed partition for subclasses that provide
  // storage-specific accessors.
  WindowPartition(
      const std::vector<column_index_t>& inputMapping,
      const std::vector<std::pair<column_index_t, core::SortOrder>>&
          sortKeyInfo,
      bool partial);

  const std::vector<std::pair<column_index_t, core::SortOrder>>& sortKeyInfo()
      const {
    return sortKeyInfo_;
  }

 private:
  // Adapts RowContainer-backed rows to window partition computations.
  class RowContainerAccessor;

  bool compareRowsWithSortKeys(const char* lhs, const char* rhs) const;

  void checkPartial() const {
    VELOX_CHECK(partial_, "WindowPartition should be partial");
  }

  // Indicates if this is a partial partition for RowStreamWindowBuild
  // processing.
  const bool partial_;

  // The RowContainer associated with the partition. It is owned by the
  // WindowBuild that creates the partition. Null for subclasses that store
  // rows elsewhere.
  RowContainer* const data_;

  // folly::Range is for the partition rows iterator provided by the
  // Window operator. The pointers are to rows from a RowContainer owned
  // by the operator. We can assume these are valid values for the lifetime
  // of WindowPartition.
  const folly::Range<char**> partition_;

  // Indicates if a partial partition has received all the input rows. For a
  // non-partial partition, this is always true.
  bool complete_ = true;

  // Mapping from window input column -> index in data_. This is required
  // because the WindowBuild reorders data_ to place partition and sort keys
  // before other columns in data_. But the Window Operator and Function code
  // accesses WindowPartition using the indexes of Window input type.
  const std::vector<column_index_t> inputMapping_;

  // ORDER BY column info for this partition.
  const std::vector<std::pair<column_index_t, core::SortOrder>> sortKeyInfo_;

  // The locations in the rows of 'data_' of the input columns of the Window
  // operator, in input order, for checking values row by row without looking
  // up the column each time.
  std::vector<exec::RowColumn> columns_;
};
} // namespace facebook::velox::exec
