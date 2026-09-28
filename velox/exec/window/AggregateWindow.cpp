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

#include "velox/exec/window/AggregateWindow.h"

#include <functional>
#include <numeric>

#include "velox/common/base/Exceptions.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec::window {

namespace {

// Computes an aggregate over sliding frames whose start and end bounds are
// both non-decreasing, using the two-stack algorithm. Each partition row is
// added to the aggregate a constant number of times, so the cost per output
// row does not depend on the frame width.
//
// The window holds rows [leftRow_, rightRow_). Rows [leftRow_, backStartRow_)
// form the front stack: front_[k] holds the aggregate of rows
// [backStartRow_ - 1 - k, backStartRow_), so front_.back() covers every front
// row. Rows [backStartRow_, rightRow_) form the back stack, kept as a single
// accumulator 'backAccumulator_'. The window aggregate is
// front_.back() merged with 'backAccumulator_'. When the front stack is
// exhausted, it is rebuilt from the raw back rows.
//
// The aggregate has no API to copy an accumulator, so merging goes through
// extractAccumulators() and addIntermediateResults(), as spilling does. All
// calls go through one Aggregate object because it tracks which of its groups
// may be null. The calls are batched across an output batch wherever the data
// dependencies allow it, since their fixed cost dominates for narrow frames.
//
// Merges always add the older rows first, so the aggregate sees rows in
// window order.
class SlidingWindowAggregator {
 public:
  // Loads 'numRows' rows of the aggregate arguments starting at partition row
  // 'firstRow' into 'args'.
  using ArgsLoader = std::function<void(
      vector_size_t firstRow,
      vector_size_t numRows,
      std::vector<VectorPtr>& args)>;

  SlidingWindowAggregator(
      exec::Aggregate* aggregate,
      int32_t rowSize,
      const TypePtr& intermediateType,
      const TypePtr& resultType,
      const std::vector<VectorPtr>& argsPrototype,
      ArgsLoader argsLoader,
      memory::MemoryPool* pool)
      : aggregate_{aggregate},
        slotSize_{bits::roundUp(
            rowSize,
            std::max<int32_t>(
                aggregate->accumulatorAlignmentSize(),
                sizeof(int64_t)))},
        argsLoader_{std::move(argsLoader)},
        pool_{pool} {
    pushArgs_ = makeArgs(argsPrototype);
    rebuildArgs_ = makeArgs(argsPrototype);
    snapshot_ = BaseVector::create(intermediateType, 1, pool_);
    backSnapshots_ = BaseVector::create(intermediateType, 0, pool_);
    frontAccumulators_ = BaseVector::create(intermediateType, 0, pool_);
    batchResult_ = BaseVector::create(resultType, 0, pool_);
    pushRows_.resize(kArgsChunkRows, false);

    emptySlot_ = allocateSlot();
    initializeSlots(&emptySlot_, 1);
    backAccumulator_ = allocateSlot();
    initializeSlots(&backAccumulator_, 1);
  }

  ~SlidingWindowAggregator() {
    releaseSlots(front_);
    releaseSlots(pendingRelease_);
    aggregate_->destroy(folly::Range(&backAccumulator_, 1));
    aggregate_->destroy(folly::Range(&emptySlot_, 1));
  }

  // Empties the window and positions it to start at partition row 'row'.
  void reset(vector_size_t row) {
    releaseSlots(front_);
    front_.clear();
    resetBackAccumulator();
    leftRow_ = row;
    backStartRow_ = row;
    rightRow_ = row;
    // Arguments loaded for a previous partition are not valid anymore.
    pushArgsFirstRow_ = 0;
    pushArgsNumRows_ = 0;
  }

  // Computes the aggregate of each frame in 'validRows' into
  // result[resultOffset + i]. 'lastRow' is the largest frame end in the batch.
  void apply(
      const SelectivityVector& validRows,
      const vector_size_t* rawFrameStarts,
      const vector_size_t* rawFrameEnds,
      vector_size_t lastRow,
      vector_size_t resultOffset,
      const VectorPtr& result) {
    const auto numRows = validRows.end();
    frontPointers_.resize(numRows);
    std::fill(frontPointers_.begin(), frontPointers_.end(), emptySlot_);
    rowsWithFront_.resizeFill(numRows, false);
    rowsWithBack_.resizeFill(numRows, false);
    BaseVector::prepareForReuse(backSnapshots_, numRows);

    validRows.applyToSelected([&](auto i) {
      if (rightRow_ <= rawFrameEnds[i]) {
        addToBack(rightRow_, rawFrameEnds[i], lastRow);
        rightRow_ = rawFrameEnds[i] + 1;
      }
      while (leftRow_ < rawFrameStarts[i]) {
        popFront();
      }

      // Front slots referenced by this batch stay alive until the batch
      // results are extracted; see popFront().
      if (!front_.empty()) {
        frontPointers_[i] = front_.back();
        rowsWithFront_.setValid(i, true);
      }
      if (backStartRow_ < rightRow_) {
        BaseVector::prepareForReuse(snapshot_, 1);
        aggregate_->extractAccumulators(&backAccumulator_, 1, &snapshot_);
        backSnapshots_->copy(snapshot_.get(), i, 0, 1);
        rowsWithBack_.setValid(i, true);
      }
    });
    rowsWithFront_.updateBounds();
    rowsWithBack_.updateBounds();

    // Combine the front and back parts of every frame in batched calls. Rows
    // outside 'validRows' get an empty aggregate here. The caller overwrites
    // them.
    std::vector<char*> groups(numRows);
    for (auto& group : groups) {
      group = allocateSlot();
    }
    initializeSlots(groups.data(), numRows);
    if (rowsWithFront_.hasSelections()) {
      BaseVector::prepareForReuse(frontAccumulators_, numRows);
      aggregate_->extractAccumulators(
          frontPointers_.data(), numRows, &frontAccumulators_);
      aggregate_->addIntermediateResults(
          groups.data(), rowsWithFront_, {frontAccumulators_}, false);
    }
    if (rowsWithBack_.hasSelections()) {
      aggregate_->addIntermediateResults(
          groups.data(), rowsWithBack_, {backSnapshots_}, false);
    }
    BaseVector::prepareForReuse(batchResult_, numRows);
    aggregate_->extractValues(groups.data(), numRows, &batchResult_);
    result->copy(batchResult_.get(), resultOffset, 0, numRows);

    releaseSlots(groups);
    releaseSlots(pendingRelease_);
    pendingRelease_.clear();
  }

 private:
  // Rows of arguments loaded at a time. Bounds the cost of selecting rows in
  // 'pushRows_', which is linear in its size.
  static constexpr vector_size_t kArgsChunkRows{1'024};

  // Number of slots allocated together.
  static constexpr int32_t kSlotsPerAllocation{256};

  std::vector<VectorPtr> makeArgs(const std::vector<VectorPtr>& prototype) {
    std::vector<VectorPtr> args;
    args.reserve(prototype.size());
    for (const auto& arg : prototype) {
      // Constant arguments are shared. Loaded arguments get their own vector.
      args.push_back(
          arg->isConstantEncoding()
              ? arg
              : BaseVector::create(arg->type(), 0, pool_));
    }
    return args;
  }

  // Adds rows [firstRow, lastRow] to the back accumulator. Loads arguments in
  // chunks that extend up to 'batchLastRow' so that later calls in the same
  // batch find their rows already loaded.
  void addToBack(
      vector_size_t firstRow,
      vector_size_t lastRow,
      vector_size_t batchLastRow) {
    auto row = firstRow;
    while (row <= lastRow) {
      if (row < pushArgsFirstRow_ ||
          row >= pushArgsFirstRow_ + pushArgsNumRows_) {
        pushArgsFirstRow_ = row;
        pushArgsNumRows_ =
            std::min<vector_size_t>(kArgsChunkRows, batchLastRow + 1 - row);
        argsLoader_(pushArgsFirstRow_, pushArgsNumRows_, pushArgs_);
      }
      const auto endRow = std::min<vector_size_t>(
          lastRow + 1, pushArgsFirstRow_ + pushArgsNumRows_);
      pushRows_.setValidRange(selectedBegin_, selectedEnd_, false);
      selectedBegin_ = row - pushArgsFirstRow_;
      selectedEnd_ = endRow - pushArgsFirstRow_;
      pushRows_.setValidRange(selectedBegin_, selectedEnd_, true);
      pushRows_.updateBounds();
      aggregate_->addSingleGroupRawInput(
          backAccumulator_, pushRows_, pushArgs_, false);
      row = endRow;
    }
  }

  // Removes the oldest row from the window. The popped slot may be referenced
  // by an earlier row of the current batch, so it is released after the batch.
  void popFront() {
    if (front_.empty()) {
      rebuildFront();
    }
    pendingRelease_.push_back(front_.back());
    front_.pop_back();
    ++leftRow_;
  }

  // Moves all back rows to the front stack.
  void rebuildFront() {
    VELOX_CHECK_LT(backStartRow_, rightRow_);
    const auto numRows = rightRow_ - backStartRow_;

    // slots[k] starts with the raw input of row backStartRow_ + k, added in
    // batched calls.
    std::vector<char*> slots(numRows);
    for (auto& slot : slots) {
      slot = allocateSlot();
    }
    initializeSlots(slots.data(), numRows);
    SelectivityVector rows;
    for (vector_size_t offset = 0; offset < numRows; offset += kArgsChunkRows) {
      const auto numChunkRows =
          std::min<vector_size_t>(kArgsChunkRows, numRows - offset);
      argsLoader_(backStartRow_ + offset, numChunkRows, rebuildArgs_);
      rows.resize(numChunkRows, true);
      aggregate_->addRawInput(slots.data() + offset, rows, rebuildArgs_, false);
    }

    // Turn each slot into the aggregate of its row and all newer back rows.
    for (auto k = numRows - 2; k >= 0; --k) {
      merge(slots[k], slots[k + 1]);
    }

    front_.assign(slots.rbegin(), slots.rend());
    resetBackAccumulator();
    backStartRow_ = rightRow_;
  }

  // Merges the state of 'source' into 'target'.
  void merge(char* target, char* source) {
    BaseVector::prepareForReuse(snapshot_, 1);
    aggregate_->extractAccumulators(&source, 1, &snapshot_);
    aggregate_->addSingleGroupIntermediateResults(
        target, kSingleRow(), {snapshot_}, false);
  }

  static const SelectivityVector& kSingleRow() {
    static const SelectivityVector kRow(1);
    return kRow;
  }

  void resetBackAccumulator() {
    aggregate_->destroy(folly::Range(&backAccumulator_, 1));
    initializeSlots(&backAccumulator_, 1);
  }

  char* allocateSlot() {
    if (freeSlots_.empty()) {
      allocations_.push_back(
          AlignedBuffer::allocate<char>(
              slotSize_ * kSlotsPerAllocation, pool_));
      auto* base = allocations_.back()->asMutable<char>();
      for (auto i = kSlotsPerAllocation - 1; i >= 0; --i) {
        freeSlots_.push_back(base + i * slotSize_);
      }
    }
    auto* slot = freeSlots_.back();
    freeSlots_.pop_back();
    return slot;
  }

  void initializeSlots(char** slots, vector_size_t numSlots) {
    if (slotIndices_.size() < numSlots) {
      const auto oldSize = slotIndices_.size();
      slotIndices_.resize(numSlots);
      std::iota(slotIndices_.begin() + oldSize, slotIndices_.end(), oldSize);
    }
    aggregate_->initializeNewGroups(
        slots, folly::Range(slotIndices_.data(), numSlots));
  }

  void releaseSlots(const std::vector<char*>& slots) {
    if (slots.empty()) {
      return;
    }
    aggregate_->destroy(
        folly::Range(const_cast<char**>(slots.data()), slots.size()));
    freeSlots_.insert(freeSlots_.end(), slots.begin(), slots.end());
  }

  // kSingle aggregate that owns all slots.
  exec::Aggregate* const aggregate_;

  // Bytes per accumulator slot, rounded up to keep consecutive slots aligned.
  const int32_t slotSize_;

  const ArgsLoader argsLoader_;
  memory::MemoryPool* const pool_;

  // Window bounds; see the class comment.
  vector_size_t leftRow_{0};
  vector_size_t backStartRow_{0};
  vector_size_t rightRow_{0};

  // Front stack, oldest row last.
  std::vector<char*> front_;

  // Aggregate of the back rows.
  char* backAccumulator_{nullptr};

  // Initialized slot with no input. Fills 'frontPointers_' for rows without a
  // front part so that extractAccumulators() can process the batch in one call.
  char* emptySlot_{nullptr};

  // Front slots popped during the current batch.
  std::vector<char*> pendingRelease_;

  // Arguments for addToBack(), holding 'pushArgsNumRows_' rows starting at
  // partition row 'pushArgsFirstRow_'.
  std::vector<VectorPtr> pushArgs_;
  vector_size_t pushArgsFirstRow_{0};
  vector_size_t pushArgsNumRows_{0};

  // Selects the rows of 'pushArgs_' to add. [selectedBegin_, selectedEnd_) are
  // the rows selected last, cleared before the next selection.
  SelectivityVector pushRows_;
  vector_size_t selectedBegin_{0};
  vector_size_t selectedEnd_{0};

  // Arguments for rebuildFront(). Separate from 'pushArgs_' so that a rebuild
  // does not discard rows loaded for upcoming pushes.
  std::vector<VectorPtr> rebuildArgs_;

  // Per-batch state of apply(), indexed by row in the batch.
  std::vector<char*> frontPointers_;
  SelectivityVector rowsWithFront_;
  SelectivityVector rowsWithBack_;
  VectorPtr frontAccumulators_;
  VectorPtr backSnapshots_;
  VectorPtr batchResult_;

  // Holds the intermediate state of a single accumulator.
  VectorPtr snapshot_;

  // Slot memory. Slots are never returned to the pool individually.
  std::vector<BufferPtr> allocations_;
  std::vector<char*> freeSlots_;

  // 0, 1, 2, ... for initializeNewGroups().
  std::vector<vector_size_t> slotIndices_;
};

// Returns true if every value of 'type' takes the same number of bytes.
bool hasFixedWidthValues(const Type& type) {
  if (type.isFixedWidth()) {
    return true;
  }
  if (type.kind() != TypeKind::ROW) {
    return false;
  }
  for (const auto& child : type.asRow().children()) {
    if (!hasFixedWidthValues(*child)) {
      return false;
    }
  }
  return true;
}

// Average frame widths from which the sliding window path is used. See
// AggregateWindowFunction::minSlidingFrameRows().
constexpr int64_t kMinSlidingFrameRowsPrimitive{32};
constexpr int64_t kMinSlidingFrameRowsComplex{1'024};

// A generic way to compute any aggregation used as a window function.
// Creates an Aggregate function object for the window function invocation.
// At each row, computes the aggregation across all rows from the frameStart
// to frameEnd boundaries at that row using singleGroup.
class AggregateWindowFunction : public exec::WindowFunction {
 public:
  AggregateWindowFunction(
      const std::string& name,
      const std::vector<exec::WindowFunctionArg>& args,
      const TypePtr& resultType,
      bool ignoreNulls,
      velox::memory::MemoryPool* pool,
      HashStringAllocator* stringAllocator,
      const core::QueryConfig& config)
      : WindowFunction(resultType, pool, stringAllocator) {
    VELOX_USER_CHECK(
        !ignoreNulls, "Aggregate window functions do not support IGNORE NULLS");
    argTypes_.reserve(args.size());
    argIndices_.reserve(args.size());
    argVectors_.reserve(args.size());
    std::vector<VectorPtr> constantInputs;
    constantInputs.reserve(args.size());
    for (const auto& arg : args) {
      argTypes_.push_back(arg.type);
      constantInputs.push_back(arg.constantValue);
      if (arg.constantValue) {
        argIndices_.push_back(kConstantChannel);
        argVectors_.push_back(arg.constantValue);
      } else {
        VELOX_CHECK(arg.index.has_value());
        argIndices_.push_back(arg.index.value());
        argVectors_.push_back(BaseVector::create(arg.type, 0, pool_));
      }
    }
    // Create an Aggregate function object to do result computation. Window
    // function usage only requires single group aggregation for calculating
    // the function value for each row.
    aggregate_ = exec::Aggregate::create(
        name,
        core::AggregationNode::Step::kSingle,
        argTypes_,
        resultType,
        config,
        constantInputs);
    aggregate_->setAllocator(stringAllocator_);

    // Saved to resolve the intermediate type on the sliding window path.
    functionName_ = name;

    // Aggregate initialization.
    // Row layout is:
    //  - null flags - one bit per aggregate.
    //  - uint32_t row size,
    //  - fixed-width accumulators - one per aggregate
    //
    // Here we always make space for a row size since we only have one
    // row and no RowContainer. We also have a single aggregate here, so there
    // is only one null bit and one initialized bit.
    static const int32_t kAccumulatorFlagsOffset = 0;
    static const int32_t kRowSizeOffset = bits::nbytes(1);
    singleGroupRowSize_ = kRowSizeOffset + sizeof(int32_t);
    // Accumulator offset must be aligned by their alignment size.
    singleGroupRowSize_ = bits::roundUp(
        singleGroupRowSize_, aggregate_->accumulatorAlignmentSize());
    aggregate_->setOffsets(
        exec::RowColumn(singleGroupRowSize_, kAccumulatorFlagsOffset),
        /* needed for out of line allocations */ kRowSizeOffset);
    singleGroupRowSize_ += aggregate_->accumulatorFixedWidthSize();

    // Construct the single row in the MemoryPool.
    singleGroupRowBufferPtr_ =
        AlignedBuffer::allocate<char>(singleGroupRowSize_, pool_);
    rawSingleGroupRow_ = singleGroupRowBufferPtr_->asMutable<char>();

    // Constructing a vector of a single result value used for copying from
    // the aggregate to the final result.
    aggregateResultVector_ = BaseVector::create(resultType, 1, pool_);

    computeDefaultAggregateValue(resultType);
  }

  ~AggregateWindowFunction() {
    // Needed to delete any out-of-line storage for the accumulator in the
    // group row.
    if (aggregateInitialized_) {
      std::vector<char*> singleGroupRowVector = {rawSingleGroupRow_};
      aggregate_->destroy(folly::Range(singleGroupRowVector.data(), 1));
    }
  }

  void resetPartition(const exec::WindowPartition* partition) override {
    partition_ = partition;

    previousFrameMetadata_.reset();
    if (slidingWindowAggregator_) {
      slidingWindowAggregator_->reset(0);
    }
  }

  void apply(
      const BufferPtr& /*peerGroupStarts*/,
      const BufferPtr& /*peerGroupEnds*/,
      const BufferPtr& frameStarts,
      const BufferPtr& frameEnds,
      const SelectivityVector& validRows,
      vector_size_t resultOffset,
      const VectorPtr& result) override {
    if (handleAllEmptyFrames(validRows, resultOffset, result)) {
      return;
    }

    auto rawFrameStarts = frameStarts->as<vector_size_t>();
    auto rawFrameEnds = frameEnds->as<vector_size_t>();

    FrameMetadata frameMetadata =
        analyzeFrameValues(validRows, rawFrameStarts, rawFrameEnds);

    if (frameMetadata.incrementalAggregation) {
      vector_size_t startRow;
      if (frameMetadata.usePreviousAggregate) {
        // If incremental aggregation can be resumed from the previous block,
        // then the argument vectors also can be populated from the previous
        // frameEnd to the current frameEnd. Only the new values are
        // required for computing aggregates.
        startRow = previousFrameMetadata_->lastRow + 1;
      } else {
        startRow = frameMetadata.firstRow;

        // This is the start of a new incremental aggregation. So the
        // aggregate_ function object should be initialized.
        auto singleGroup = std::vector<vector_size_t>{0};
        aggregate_->destroy(folly::Range<char**>(&rawSingleGroupRow_, 1));
        aggregate_->initializeNewGroups(&rawSingleGroupRow_, singleGroup);
        aggregateInitialized_ = true;
      }

      fillArgVectors(startRow, frameMetadata.lastRow);
      incrementalAggregation(
          validRows,
          startRow,
          frameMetadata.lastRow,
          rawFrameEnds,
          resultOffset,
          result);
    } else if (frameMetadata.slidingWindow) {
      slidingWindowAggregation(
          validRows,
          frameMetadata,
          rawFrameStarts,
          rawFrameEnds,
          resultOffset,
          result);
    } else {
      fillArgVectors(frameMetadata.firstRow, frameMetadata.lastRow);
      simpleAggregation(
          validRows,
          frameMetadata.firstRow,
          frameMetadata.lastRow,
          rawFrameStarts,
          rawFrameEnds,
          resultOffset,
          result);
    }
    previousFrameMetadata_ = frameMetadata;
  }

 private:
  struct FrameMetadata {
    // Min frame start row required for aggregation.
    vector_size_t firstRow;

    // Max frame end required for the aggregation.
    vector_size_t lastRow;

    // If all the rows in the block have the same start row, and the
    // end frame rows are non-decreasing, then the aggregation can be done
    // incrementally. With incremental aggregation new frame rows are
    // accumulated over the previous result to obtain the new result.
    bool incrementalAggregation;

    // Resume incremental aggregation from the prior block.
    bool usePreviousAggregate;

    // Compute the aggregate with SlidingWindowAggregator. Applies when the
    // frame start and end rows are both non-decreasing.
    bool slidingWindow;

    // Resume the sliding window state from the prior block.
    bool usePreviousSlidingWindow;

    // Frame start and end of the last valid row of the block. The sliding
    // window state carries over to the next block only if its first frame
    // does not move backward from these.
    vector_size_t lastFrameStart;
    vector_size_t lastFrameEnd;
  };

  bool handleAllEmptyFrames(
      const SelectivityVector& validRows,
      vector_size_t resultOffset,
      const VectorPtr& result) {
    if (!validRows.hasSelections()) {
      setEmptyFramesResult(validRows, resultOffset, emptyResult_, result);
      return true;
    }
    return false;
  }

  // Computes the least frameStart row and the max frameEnds row
  // indices for the valid frames of this output block. These indices are used
  // as bounds when reading input parameter vectors for aggregation.
  // This method expects to have at least 1 valid frame in the block.
  // Blocks with all empty frames are handled before this point.
  FrameMetadata analyzeFrameValues(
      const SelectivityVector& validRows,
      const vector_size_t* rawFrameStarts,
      const vector_size_t* rawFrameEnds) {
    VELOX_DCHECK(validRows.hasSelections());

    // Use first valid frame row for the initialization.
    auto firstValidRow = validRows.begin();
    vector_size_t firstRow = rawFrameStarts[firstValidRow];
    vector_size_t fixedFrameStartRow = firstRow;
    vector_size_t lastRow = rawFrameEnds[firstValidRow];
    vector_size_t prevFrameStarts = firstRow;
    vector_size_t prevFrameEnds = lastRow;

    bool incrementalAggregation = true;
    bool startNonDecreasing = true;
    bool endNonDecreasing = true;
    int64_t numFrameRows{0};
    vector_size_t numFrames{0};

    validRows.applyToSelected([&](auto i) {
      firstRow = std::min(firstRow, rawFrameStarts[i]);
      lastRow = std::max(lastRow, rawFrameEnds[i]);

      // Incremental aggregation can be done if :
      // i) All rows have the same frameStart value.
      // ii) The frame end values are non-decreasing.
      incrementalAggregation &= (rawFrameStarts[i] == fixedFrameStartRow);
      incrementalAggregation &= rawFrameEnds[i] >= prevFrameEnds;

      startNonDecreasing &= (rawFrameStarts[i] >= prevFrameStarts);
      endNonDecreasing &= (rawFrameEnds[i] >= prevFrameEnds);

      prevFrameStarts = rawFrameStarts[i];
      prevFrameEnds = rawFrameEnds[i];
      numFrameRows += rawFrameEnds[i] + 1 - rawFrameStarts[i];
      ++numFrames;
    });

    // The sliding window path applies when both bounds are non-decreasing but
    // the start is not fixed, which the incremental path handles. It pays off
    // only for frames wide enough to amortize its higher fixed cost per row.
    // It reads back rows from the start of the window, which a partial
    // partition may have released.
    const bool slidingWindow = startNonDecreasing && endNonDecreasing &&
        !incrementalAggregation && !partition_->partial() &&
        numFrameRows / numFrames >= minSlidingFrameRows();

    bool usePreviousAggregate = false;
    if (previousFrameMetadata_.has_value()) {
      auto previousFrame = previousFrameMetadata_.value();
      // Incremental aggregation continues between blocks if :
      // i) Their starting firstRow values are the same.
      // ii) The nonDecreasing frameEnd property is also applicable between the
      // lastRow of the first block and the first row of the current block.
      if (incrementalAggregation && previousFrame.incrementalAggregation &&
          previousFrame.firstRow == firstRow &&
          previousFrame.lastRow <= rawFrameEnds[firstValidRow]) {
        usePreviousAggregate = true;
      }
    }

    bool usePreviousSlidingWindow = false;
    if (slidingWindow && previousFrameMetadata_.has_value()) {
      auto& prev = previousFrameMetadata_.value();
      // The sliding window state continues between blocks if the previous
      // block also used it and the frame bounds do not move backward between
      // the blocks.
      if (prev.slidingWindow &&
          rawFrameStarts[firstValidRow] >= prev.lastFrameStart &&
          rawFrameEnds[firstValidRow] >= prev.lastFrameEnd) {
        usePreviousSlidingWindow = true;
      }
    }

    return {
        firstRow,
        lastRow,
        incrementalAggregation,
        usePreviousAggregate,
        slidingWindow,
        usePreviousSlidingWindow,
        prevFrameStarts,
        prevFrameEnds,
    };
  }

  // Returns the average frame width from which the sliding window path is
  // faster than recomputing each frame.
  //
  // The path keeps one accumulator per row of the window, so it requires an
  // accumulator whose size does not grow with its input. Otherwise frames of W
  // rows would take O(W^2) memory. isFixedSize() alone does not ensure this,
  // for example for max_by(x, y, n). A fixed-width intermediate type does,
  // since the intermediate result serializes the whole accumulator.
  //
  // The cost per row is dominated by single-row extractAccumulators() and
  // copy() calls on the intermediate type. These are much more expensive for a
  // ROW, such as for avg, than for a primitive type. The thresholds were
  // measured with SlidingWindowAggregateBenchmark.
  int64_t minSlidingFrameRows() {
    if (!minSlidingFrameRows_.has_value()) {
      intermediateType_ = resolveIntermediateType(functionName_, argTypes_);
      if (!aggregate_->isFixedSize() ||
          !hasFixedWidthValues(*intermediateType_)) {
        minSlidingFrameRows_ = std::numeric_limits<int64_t>::max();
      } else if (intermediateType_->isPrimitiveType()) {
        minSlidingFrameRows_ = kMinSlidingFrameRowsPrimitive;
      } else {
        minSlidingFrameRows_ = kMinSlidingFrameRowsComplex;
      }
    }
    return minSlidingFrameRows_.value();
  }

  void fillArgVectors(vector_size_t firstRow, vector_size_t lastRow) {
    loadArgs(firstRow, lastRow + 1 - firstRow, argVectors_);
  }

  // Loads 'numRows' rows of the arguments starting at partition row
  // 'firstRow' into 'args'.
  void loadArgs(
      vector_size_t firstRow,
      vector_size_t numRows,
      std::vector<VectorPtr>& args) {
    for (int i = 0; i < argIndices_.size(); i++) {
      // Without the following call to `prepareForReuse`, if the type of
      // `args[i]` is VARCHAR, then the string buffers will accumulate
      // across calculations. As a result, memory consumption will increase over
      // time. So we call `prepareForReuse` to clear string buffers timely.
      args[i]->prepareForReuse();
      args[i]->resize(numRows);
      // Only non-constant field argument vectors need to be populated. The
      // constant vectors are correctly set during aggregate initialization
      // itself.
      if (argIndices_[i] != kConstantChannel) {
        partition_->extractColumn(
            argIndices_[i], firstRow, numRows, 0, args[i]);
      }
    }
  }

  void computeAggregate(
      SelectivityVector rows,
      vector_size_t startFrame,
      vector_size_t endFrame) {
    rows.clearAll();
    rows.setValidRange(startFrame, endFrame, true);
    rows.updateBounds();

    BaseVector::prepareForReuse(aggregateResultVector_, 1);

    aggregate_->addSingleGroupRawInput(
        rawSingleGroupRow_, rows, argVectors_, false);
    aggregate_->extractValues(&rawSingleGroupRow_, 1, &aggregateResultVector_);
  }

  void incrementalAggregation(
      const SelectivityVector& validRows,
      vector_size_t startFrame,
      vector_size_t endFrame,
      const vector_size_t* rawFrameEnds,
      vector_size_t resultOffset,
      const VectorPtr& result) {
    SelectivityVector rows;
    rows.resize(endFrame + 1 - startFrame);

    auto prevFrameEnd = 0;
    // This is a simple optimization for frames that have a fixed startFrame
    // and increasing frameEnd values. In that case, we can
    // incrementally aggregate over the new rows seen in the frame between
    // the previous and current row.
    validRows.applyToSelected([&](auto i) {
      auto currentFrameEnd = rawFrameEnds[i] - startFrame + 1;
      if (currentFrameEnd > prevFrameEnd) {
        computeAggregate(rows, prevFrameEnd, currentFrameEnd);
      }

      result->copy(aggregateResultVector_.get(), resultOffset + i, 0, 1);
      prevFrameEnd = currentFrameEnd;
    });

    // Set null values for empty (non valid) frames in the output block.
    setEmptyFramesResult(validRows, resultOffset, emptyResult_, result);
  }

  void slidingWindowAggregation(
      const SelectivityVector& validRows,
      const FrameMetadata& frameMetadata,
      const vector_size_t* rawFrameStarts,
      const vector_size_t* rawFrameEnds,
      vector_size_t resultOffset,
      const VectorPtr& result) {
    if (!slidingWindowAggregator_) {
      slidingWindowAggregator_ = std::make_unique<SlidingWindowAggregator>(
          aggregate_.get(),
          singleGroupRowSize_,
          intermediateType_,
          aggregateResultVector_->type(),
          argVectors_,
          [this](
              vector_size_t firstRow,
              vector_size_t numRows,
              std::vector<VectorPtr>& args) {
            loadArgs(firstRow, numRows, args);
          },
          pool_);
    }

    if (!frameMetadata.usePreviousSlidingWindow) {
      slidingWindowAggregator_->reset(rawFrameStarts[validRows.begin()]);
    }
    slidingWindowAggregator_->apply(
        validRows,
        rawFrameStarts,
        rawFrameEnds,
        frameMetadata.lastRow,
        resultOffset,
        result);

    // Set null values for empty (non valid) frames in the output block.
    setEmptyFramesResult(validRows, resultOffset, emptyResult_, result);
  }

  void simpleAggregation(
      const SelectivityVector& validRows,
      vector_size_t minFrame,
      vector_size_t maxFrame,
      const vector_size_t* frameStartsVector,
      const vector_size_t* frameEndsVector,
      vector_size_t resultOffset,
      const VectorPtr& result) {
    SelectivityVector rows;
    rows.resize(maxFrame + 1 - minFrame);
    static auto kSingleGroup = std::vector<vector_size_t>{0};

    validRows.applyToSelected([&](auto i) {
      // This is a very naive algorithm.
      // It evaluates the entire aggregation for each row by iterating over
      // input rows from frameStart to frameEnd in the SelectivityVector.
      aggregate_->destroy(folly::Range<char**>(&rawSingleGroupRow_, 1));
      aggregate_->initializeNewGroups(&rawSingleGroupRow_, kSingleGroup);
      aggregateInitialized_ = true;

      auto frameStartIndex = frameStartsVector[i] - minFrame;
      auto frameEndIndex = frameEndsVector[i] - minFrame + 1;
      computeAggregate(rows, frameStartIndex, frameEndIndex);
      result->copy(aggregateResultVector_.get(), resultOffset + i, 0, 1);
    });

    // Set null values for empty (non valid) frames in the output block.
    setEmptyFramesResult(validRows, resultOffset, emptyResult_, result);
  }

  // Precompute and save the aggregate output for empty input in emptyResult_.
  // This value is returned for rows with empty frames.
  void computeDefaultAggregateValue(const TypePtr& resultType) {
    aggregate_->clear();
    aggregate_->initializeNewGroups(
        &rawSingleGroupRow_, std::vector<vector_size_t>{0});
    aggregateInitialized_ = true;

    emptyResult_ = BaseVector::create(resultType, 1, pool_);
    aggregate_->extractValues(&rawSingleGroupRow_, 1, &emptyResult_);
    aggregate_->clear();
  }

  // Aggregate function object required for this window function evaluation.
  std::unique_ptr<exec::Aggregate> aggregate_;

  // Name of the aggregate function, used to resolve its intermediate type.
  std::string functionName_;

  // Intermediate type of the aggregate and the threshold derived from it. Set
  // on the first batch that could take the sliding window path.
  TypePtr intermediateType_;
  std::optional<int64_t> minSlidingFrameRows_;

  bool aggregateInitialized_{false};

  // Current WindowPartition used for accessing rows in the apply method.
  const exec::WindowPartition* partition_;

  // Args information : their types, column indexes in inputs and vectors
  // used to populate values to pass to the aggregate function.
  // For a constant argument a column index of kConstantChannel is used in
  // argIndices_, and its ConstantVector value from the Window operator
  // is saved in argVectors_.
  std::vector<TypePtr> argTypes_;
  std::vector<column_index_t> argIndices_;
  std::vector<VectorPtr> argVectors_;

  // This is a single aggregate row needed by the aggregate function for its
  // computation. These values are for the row and its various components.
  BufferPtr singleGroupRowBufferPtr_;
  char* rawSingleGroupRow_;
  vector_size_t singleGroupRowSize_;

  // Used for per-row aggregate computations.
  // This vector is used to copy from the aggregate to the result.
  VectorPtr aggregateResultVector_;

  // Stores metadata about the previous output block of the partition
  // to optimize aggregate computation and reading argument vectors.
  std::optional<FrameMetadata> previousFrameMetadata_;

  // Stores default result value for empty frame aggregation. Window functions
  // return the default value of an aggregate (aggregation with no rows) for
  // empty frames. e.g. count for empty frames should return 0 and not null.
  VectorPtr emptyResult_;

  // State of the sliding window path, created on first use.
  std::unique_ptr<SlidingWindowAggregator> slidingWindowAggregator_;
};

} // namespace

void registerAggregateWindowFunction(const std::string& name) {
  auto aggregateFunctionSignatures = exec::getAggregateFunctionSignatures(name);
  if (aggregateFunctionSignatures.has_value()) {
    // This copy is needed to obtain a vector of the base FunctionSignaturePtr
    // from the AggregateFunctionSignaturePtr type of
    // aggregateFunctionSignatures variable.
    std::vector<exec::FunctionSignaturePtr> signatures(
        aggregateFunctionSignatures.value().begin(),
        aggregateFunctionSignatures.value().end());

    exec::registerWindowFunction(
        name,
        std::move(signatures),
        {exec::WindowFunction::ProcessMode::kRows, true},
        [name](
            const std::vector<exec::WindowFunctionArg>& args,
            const TypePtr& resultType,
            bool ignoreNulls,
            velox::memory::MemoryPool* pool,
            HashStringAllocator* stringAllocator,
            const core::QueryConfig& config)
            -> std::unique_ptr<exec::WindowFunction> {
          return std::make_unique<AggregateWindowFunction>(
              name,
              args,
              resultType,
              ignoreNulls,
              pool,
              stringAllocator,
              config);
        });
  }
}
} // namespace facebook::velox::exec::window
