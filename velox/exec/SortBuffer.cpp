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

#include "SortBuffer.h"
#include "velox/exec/MemoryReclaimer.h"
#include "velox/exec/Spiller.h"

namespace facebook::velox::exec {

SortBuffer::SortBuffer(
    const RowTypePtr& input,
    const std::vector<column_index_t>& sortColumnIndices,
    const std::vector<CompareFlags>& sortCompareFlags,
    velox::memory::MemoryPool* pool,
    tsan_atomic<bool>* nonReclaimableSection,
    common::PrefixSortConfig prefixSortConfig,
    const common::SpillConfig* spillConfig,
    folly::Synchronized<velox::common::SpillStats>* spillStats)
    : input_(input),
      sortCompareFlags_(sortCompareFlags),
      pool_(pool),
      nonReclaimableSection_(nonReclaimableSection),
      prefixSortConfig_(prefixSortConfig),
      spillConfig_(spillConfig),
      spillStats_(spillStats),
      sortedRows_(0, memory::StlAllocator<char*>(*pool)) {
  VELOX_CHECK_GE(input_->size(), sortCompareFlags_.size());
  VELOX_CHECK_GT(sortCompareFlags_.size(), 0);
  VELOX_CHECK_EQ(sortColumnIndices.size(), sortCompareFlags_.size());
  VELOX_CHECK_NOT_NULL(nonReclaimableSection_);

  std::vector<TypePtr> sortedColumnTypes;
  std::vector<TypePtr> nonSortedColumnTypes;
  std::vector<std::string> sortedSpillColumnNames;
  std::vector<TypePtr> sortedSpillColumnTypes;
  sortedColumnTypes.reserve(sortColumnIndices.size());
  nonSortedColumnTypes.reserve(input->size() - sortColumnIndices.size());
  sortedSpillColumnNames.reserve(input->size());
  sortedSpillColumnTypes.reserve(input->size());
  std::unordered_set<column_index_t> sortedChannelSet;
  // Sorted key columns.
  for (column_index_t i = 0; i < sortColumnIndices.size(); ++i) {
    columnMap_.emplace_back(IdentityProjection(i, sortColumnIndices.at(i)));
    sortedColumnTypes.emplace_back(input_->childAt(sortColumnIndices.at(i)));
    sortedSpillColumnTypes.emplace_back(
        input_->childAt(sortColumnIndices.at(i)));
    sortedSpillColumnNames.emplace_back(input->nameOf(sortColumnIndices.at(i)));
    sortedChannelSet.emplace(sortColumnIndices.at(i));
  }
  // Non-sorted key columns.
  for (column_index_t i = 0, nonSortedIndex = sortCompareFlags_.size();
       i < input_->size();
       ++i) {
    if (sortedChannelSet.count(i) != 0) {
      continue;
    }
    columnMap_.emplace_back(nonSortedIndex++, i);
    nonSortedColumnTypes.emplace_back(input_->childAt(i));
    sortedSpillColumnTypes.emplace_back(input_->childAt(i));
    sortedSpillColumnNames.emplace_back(input->nameOf(i));
  }

  data_ = std::make_unique<RowContainer>(
      sortedColumnTypes, nonSortedColumnTypes, pool_);
  spillerStoreType_ =
      ROW(std::move(sortedSpillColumnNames), std::move(sortedSpillColumnTypes));
}

SortBuffer::~SortBuffer() {
  pool_->release();
}

void SortBuffer::addInput(const VectorPtr& input) {
  velox::common::testutil::TestValue::adjust(
      "facebook::velox::exec::SortBuffer::addInput", this);

  VELOX_CHECK(!noMoreInput_);
  ensureInputFits(input);

  SelectivityVector allRows(input->size());
  std::vector<char*> rows(input->size());
  for (int row = 0; row < input->size(); ++row) {
    rows[row] = data_->newRow();
  }
  auto* inputRow = input->as<RowVector>();
  for (const auto& columnProjection : columnMap_) {
    DecodedVector decoded(
        *inputRow->childAt(columnProjection.outputChannel), allRows);
    data_->store(
        decoded,
        folly::Range(rows.data(), input->size()),
        columnProjection.inputChannel);
  }
  numInputRows_ += allRows.size();
}

void SortBuffer::noMoreInput() {
  velox::common::testutil::TestValue::adjust(
      "facebook::velox::exec::SortBuffer::noMoreInput", this);
  VELOX_CHECK(!noMoreInput_);
  VELOX_CHECK_NULL(outputSpiller_);

  // It may trigger spill, make sure it's triggered before noMoreInput_ is set.
  ensureSortFits();

  noMoreInput_ = true;

  // No data.
  if (numInputRows_ == 0) {
    return;
  }

  if (inputSpiller_ == nullptr) {
    VELOX_CHECK_EQ(numInputRows_, data_->numRows());
    updateEstimatedOutputRowSize();
    // Sort the pointers to the rows in RowContainer (data_) instead of sorting
    // the rows.
    sortedRows_.resize(numInputRows_);
    RowContainerIterator iter;
    data_->listRows(&iter, numInputRows_, sortedRows_.data());
    PrefixSort::sort(
        data_.get(), sortCompareFlags_, prefixSortConfig_, pool_, sortedRows_);
  } else {
    // Spill the remaining in-memory state to disk if spilling has been
    // triggered on this sort buffer. This is to simplify query OOM prevention
    // when producing output as we don't support to spill during that stage as
    // for now.
    spill();

    finishSpill();
  }

  // Releases the unused memory reservation after procesing input.
  pool_->release();
}

RowVectorPtr SortBuffer::getOutput(vector_size_t maxOutputRows) {
  SCOPE_EXIT {
    pool_->release();
  };

  VELOX_CHECK(noMoreInput_);

  if (numOutputRows_ == numInputRows_) {
    return nullptr;
  }
  VELOX_CHECK_GT(maxOutputRows, 0);
  VELOX_CHECK_GT(numInputRows_, numOutputRows_);
  const vector_size_t batchSize =
      std::min<uint64_t>(numInputRows_ - numOutputRows_, maxOutputRows);
  ensureOutputFits(batchSize);
  prepareOutput(batchSize);
  if (hasSpilled()) {
    getOutputWithSpill();
  } else {
    getOutputWithoutSpill();
  }
  return output_;
}

bool SortBuffer::hasSpilled() const {
  if (inputSpiller_ != nullptr) {
    VELOX_CHECK_NULL(outputSpiller_);
    return true;
  }
  return outputSpiller_ != nullptr;
}

void SortBuffer::spill() {
  VELOX_CHECK_NOT_NULL(
      spillConfig_, "spill config is null when SortBuffer spill is called");

  // Check if sort buffer is empty or not, and skip spill if it is empty.
  if (data_->numRows() == 0) {
    return;
  }
  updateEstimatedOutputRowSize();

  if (sortedRows_.empty()) {
    spillInput();
  } else {
    spillOutput();
  }
}

std::optional<uint64_t> SortBuffer::estimateOutputRowSize() const {
  return estimatedOutputRowSize_;
}

void SortBuffer::ensureInputFits(const VectorPtr& input) {
  // Check if spilling is enabled or not.
  if (spillConfig_ == nullptr) {
    return;
  }

  const int64_t numRows = data_->numRows();
  if (numRows == 0) {
    // 'data_' is empty. Nothing to spill.
    return;
  }

  auto [freeRows, outOfLineFreeBytes] = data_->freeSpace();
  const auto outOfLineBytes =
      data_->stringAllocator().retainedSize() - outOfLineFreeBytes;
  const int64_t flatInputBytes = input->estimateFlatSize();

  // Test-only spill path.
  if (numRows > 0 && testingTriggerSpill(pool_->name())) {
    spill();
    return;
  }

  const auto currentMemoryUsage = pool_->usedBytes();
  const auto minReservationBytes =
      currentMemoryUsage * spillConfig_->minSpillableReservationPct / 100;
  const auto availableReservationBytes = pool_->availableReservation();
  const int64_t estimatedIncrementalBytes =
      data_->sizeIncrement(input->size(), outOfLineBytes ? flatInputBytes : 0);
  if (availableReservationBytes > minReservationBytes) {
    // If we have enough free rows for input rows and enough variable length
    // free space for the vector's flat size, no need for spilling.
    if (freeRows > input->size() &&
        (outOfLineBytes == 0 || outOfLineFreeBytes >= flatInputBytes)) {
      return;
    }

    // If the current available reservation in memory pool is 2X the
    // estimatedIncrementalBytes, no need to spill.
    if (availableReservationBytes > 2 * estimatedIncrementalBytes) {
      return;
    }
  }

  // Try reserving targetIncrementBytes more in memory pool, if succeed, no
  // need to spill.
  const auto targetIncrementBytes = std::max<int64_t>(
      estimatedIncrementalBytes * 2,
      currentMemoryUsage * spillConfig_->spillableReservationGrowthPct / 100);
  {
    memory::ReclaimableSectionGuard guard(nonReclaimableSection_);
    if (pool_->maybeReserve(targetIncrementBytes)) {
      return;
    }
  }
  LOG(WARNING) << "Failed to reserve " << succinctBytes(targetIncrementBytes)
               << " for memory pool " << pool()->name()
               << ", usage: " << succinctBytes(pool()->usedBytes())
               << ", reservation: " << succinctBytes(pool()->reservedBytes());
}

void SortBuffer::ensureOutputFits(vector_size_t batchSize) {
  VELOX_CHECK_GT(batchSize, 0);
  // Check if spilling is enabled or not.
  if (spillConfig_ == nullptr) {
    return;
  }

  // Test-only spill path.
  if (testingTriggerSpill(pool_->name())) {
    spill();
    return;
  }

  if (!estimatedOutputRowSize_.has_value() || hasSpilled()) {
    return;
  }

  const uint64_t outputBufferSizeToReserve =
      estimatedOutputRowSize_.value() * batchSize * 1.2;
  {
    memory::ReclaimableSectionGuard guard(nonReclaimableSection_);
    if (pool_->maybeReserve(outputBufferSizeToReserve)) {
      return;
    }
  }
  LOG(WARNING) << "Failed to reserve "
               << succinctBytes(outputBufferSizeToReserve)
               << " for memory pool " << pool_->name()
               << ", usage: " << succinctBytes(pool_->usedBytes())
               << ", reservation: " << succinctBytes(pool_->reservedBytes());
}

void SortBuffer::ensureSortFits() {
  // Check if spilling is enabled or not.
  if (spillConfig_ == nullptr) {
    return;
  }

  // Test-only spill path.
  if (testingTriggerSpill(pool_->name())) {
    spill();
    return;
  }

  if (numInputRows_ == 0 || inputSpiller_ != nullptr) {
    return;
  }

  // The memory for std::vector sorted rows and prefix sort required buffer.
  uint64_t sortBufferToReserve =
      numInputRows_ * sizeof(char*) +
      PrefixSort::maxRequiredBytes(
          data_.get(), sortCompareFlags_, prefixSortConfig_, pool_);
  {
    memory::ReclaimableSectionGuard guard(nonReclaimableSection_);
    if (pool_->maybeReserve(sortBufferToReserve)) {
      return;
    }
  }

  LOG(WARNING) << fmt::format(
      "Failed to reserve {} for memory pool {}, usage: {}, reservation: {}",
      succinctBytes(sortBufferToReserve),
      pool_->name(),
      succinctBytes(pool_->usedBytes()),
      succinctBytes(pool_->reservedBytes()));
}

void SortBuffer::updateEstimatedOutputRowSize() {
  const auto optionalRowSize = data_->estimateRowSize();
  if (!optionalRowSize.has_value() || optionalRowSize.value() == 0) {
    return;
  }

  const auto rowSize = optionalRowSize.value();
  if (!estimatedOutputRowSize_.has_value()) {
    estimatedOutputRowSize_ = rowSize;
  } else if (rowSize > estimatedOutputRowSize_.value()) {
    estimatedOutputRowSize_ = rowSize;
  }
}

void SortBuffer::spillInput() {
  if (inputSpiller_ == nullptr) {
    VELOX_CHECK(!noMoreInput_);
    const auto sortingKeys = SpillState::makeSortingKeys(sortCompareFlags_);
    inputSpiller_ = std::make_unique<SortInputSpiller>(
        data_.get(), spillerStoreType_, sortingKeys, spillConfig_, spillStats_);
  }
  inputSpiller_->spill();
  data_->clear();
}

void SortBuffer::spillOutput() {
  if (hasSpilled()) {
    // Already spilled.
    return;
  }
  if (numOutputRows_ == sortedRows_.size()) {
    // All the output has been produced.
    return;
  }

  outputSpiller_ = std::make_unique<SortOutputSpiller>(
      data_.get(), spillerStoreType_, spillConfig_, spillStats_);
  auto spillRows = SpillerBase::SpillRows(
      sortedRows_.begin() + numOutputRows_,
      sortedRows_.end(),
      *memory::spillMemoryPool());
  outputSpiller_->spill(spillRows);
  data_->clear();
  sortedRows_.clear();
  sortedRows_.shrink_to_fit();
  // Finish right after spilling as the output spiller only spills at most
  // once.
  finishSpill();
}

void SortBuffer::prepareOutput(vector_size_t batchSize) {
  if (output_ != nullptr) {
    VectorPtr output = std::move(output_);
    BaseVector::prepareForReuse(output, batchSize);
    output_ = std::static_pointer_cast<RowVector>(output);
  } else {
    output_ = std::static_pointer_cast<RowVector>(
        BaseVector::create(input_, batchSize, pool_));
  }

  for (auto& child : output_->children()) {
    child->resize(batchSize);
  }

  if (hasSpilled()) {
    spillSources_.resize(batchSize);
    spillSourceRows_.resize(batchSize);
    prepareOutputWithSpill();
  }

  VELOX_CHECK_GT(output_->size(), 0);
  VELOX_CHECK_LE(output_->size() + numOutputRows_, numInputRows_);
}

void SortBuffer::getOutputWithoutSpill() {
  VELOX_DCHECK_EQ(numInputRows_, sortedRows_.size());
  for (const auto& columnProjection : columnMap_) {
    data_->extractColumn(
        sortedRows_.data() + numOutputRows_,
        output_->size(),
        columnProjection.inputChannel,
        output_->childAt(columnProjection.outputChannel));
  }
  numOutputRows_ += output_->size();
}

void SortBuffer::getOutputWithSpill() {
  VELOX_CHECK_NOT_NULL(spillMerger_);
  VELOX_DCHECK_EQ(sortedRows_.size(), 0);

  int32_t outputRow = 0;
  int32_t outputSize = 0;
  bool isEndOfBatch = false;
  while (outputRow + outputSize < output_->size()) {
    SpillMergeStream* stream = spillMerger_->next();
    VELOX_CHECK_NOT_NULL(stream);

    spillSources_[outputSize] = &stream->current();
    spillSourceRows_[outputSize] = stream->currentIndex(&isEndOfBatch);
    ++outputSize;
    if (FOLLY_UNLIKELY(isEndOfBatch)) {
      // The stream is at end of input batch. Need to copy out the rows before
      // fetching next batch in 'pop'.
      gatherCopy(
          output_.get(),
          outputRow,
          outputSize,
          spillSources_,
          spillSourceRows_,
          columnMap_);
      outputRow += outputSize;
      outputSize = 0;
    }
    // Advance the stream.
    stream->pop();
  }
  VELOX_CHECK_EQ(outputRow + outputSize, output_->size());

  if (FOLLY_LIKELY(outputSize != 0)) {
    gatherCopy(
        output_.get(),
        outputRow,
        outputSize,
        spillSources_,
        spillSourceRows_,
        columnMap_);
  }

  numOutputRows_ += output_->size();
}

void SortBuffer::finishSpill() {
  VELOX_CHECK_NULL(spillMerger_);
  VELOX_CHECK(spillPartitionSet_.empty());
  VELOX_CHECK_EQ(
      !!(outputSpiller_ != nullptr) + !!(inputSpiller_ != nullptr),
      1,
      "inputSpiller_ {}, outputSpiller_ {}",
      inputSpiller_ == nullptr ? "set" : "null",
      outputSpiller_ == nullptr ? "set" : "null");
  if (inputSpiller_ != nullptr) {
    VELOX_CHECK(!inputSpiller_->finalized());
    inputSpiller_->finishSpill(spillPartitionSet_);
  } else {
    VELOX_CHECK(!outputSpiller_->finalized());
    outputSpiller_->finishSpill(spillPartitionSet_);
  }
  VELOX_CHECK_EQ(spillPartitionSet_.size(), 1);
}

void SortBuffer::prepareOutputWithSpill() {
  VELOX_CHECK(hasSpilled());
  if (spillMerger_ != nullptr) {
    VELOX_CHECK(spillPartitionSet_.empty());
    return;
  }

  VELOX_CHECK_EQ(spillPartitionSet_.size(), 1);
  spillMerger_ = spillPartitionSet_.begin()->second->createOrderedReader(
      spillConfig_->readBufferSize, pool(), spillStats_);
  spillPartitionSet_.clear();
}
} // namespace facebook::velox::exec
