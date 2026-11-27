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

#include "velox/exec/window/WholePartitionAggregation.h"

#include "velox/exec/Operator.h"
#include "velox/exec/Spiller.h"
#include "velox/exec/WindowFunction.h"

namespace facebook::velox::exec::window {

bool WholePartitionAggregation::supports(const core::WindowNode& windowNode) {
  if (!windowNode.inputsSorted() || windowNode.partitionKeys().empty()) {
    return false;
  }
  for (const auto& function : windowNode.windowFunctions()) {
    const auto& frame = function.frame;
    if (frame.startType != core::WindowNode::BoundType::kUnboundedPreceding ||
        frame.endType != core::WindowNode::BoundType::kUnboundedFollowing ||
        function.ignoreNulls) {
      return false;
    }
    for (const auto& input : function.functionCall->inputs()) {
      if (!input->isFieldAccessKind() && !input->isConstantKind()) {
        return false;
      }
    }
    if (!getWindowFunctionMetadata(function.functionCall->name()).isAggregate) {
      return false;
    }
  }
  return true;
}

WholePartitionAggregation::WholePartitionAggregation(
    const std::shared_ptr<const core::WindowNode>& windowNode,
    memory::MemoryPool* pool,
    const common::SpillConfig* spillConfig,
    exec::SpillStats* spillStats,
    const core::QueryConfig& config)
    : pool_(pool),
      spillConfig_(spillConfig),
      spillStats_(spillStats),
      inputType_(windowNode->inputType()),
      outputType_(windowNode->outputType()) {
  VELOX_CHECK(supports(*windowNode));
  for (const auto& key : windowNode->partitionKeys()) {
    const auto channel = exprToChannel(key.get(), inputType_);
    partitionKeyChannels_.push_back(channel);
  }
  lastPartitionKeys_.resize(partitionKeyChannels_.size());

  std::vector<Accumulator> accumulators;
  for (const auto& windowFunction : windowNode->windowFunctions()) {
    const auto& call = windowFunction.functionCall;
    Function function;
    std::vector<TypePtr> argumentTypes;
    for (const auto& input : call->inputs()) {
      argumentTypes.push_back(input->type());
      const auto channel = exprToChannel(input.get(), inputType_);
      function.channels.push_back(channel);
      function.constants.push_back(
          channel == kConstantChannel
              ? core::TypedExprs::asConstant(input)->toConstantVector(pool_)
              : nullptr);
    }
    function.resultType = call->type();
    // Same step as the generic aggregate window function, so both produce the
    // same results.
    function.aggregate = Aggregate::create(
        call->name(),
        core::AggregationNode::Step::kSingle,
        argumentTypes,
        function.resultType,
        config,
        function.constants);
    accumulators.emplace_back(function.aggregate.get(), nullptr);
    functions_.push_back(std::move(function));
  }

  groups_ = std::make_unique<RowContainer>(
      std::vector<TypePtr>{},
      accumulators,
      std::vector<TypePtr>{},
      pool_,
      RowContainer::Options{});
  for (auto i = 0; i < functions_.size(); ++i) {
    auto& aggregate = functions_[i].aggregate;
    aggregate->setAllocator(&groups_->stringAllocator());
    aggregate->setOffsets(groups_->columnAt(i), groups_->rowSizeOffset());
  }
}

bool WholePartitionAggregation::needsInput() const {
  return completeRowsEnd() == numOutputRows_;
}

bool WholePartitionAggregation::startsPartition(
    const RowVector& input,
    vector_size_t row) const {
  for (auto i = 0; i < partitionKeyChannels_.size(); ++i) {
    const auto& keys = input.childAt(partitionKeyChannels_[i]);
    const bool equal = row == 0
        ? keys->equalValueAt(lastPartitionKeys_[i].get(), 0, 0)
        : keys->equalValueAt(keys.get(), row, row - 1);
    if (!equal) {
      return true;
    }
  }
  return false;
}

void WholePartitionAggregation::addInput(const RowVectorPtr& input) {
  const auto numRows = input->size();
  if (numRows == 0) {
    return;
  }
  // Spilling serializes the batch, which needs loaded vectors.
  for (const auto& child : input->children()) {
    child->loadedVector();
  }

  rowGroups_.resize(numRows);
  newGroupRows_.clear();
  for (vector_size_t row = 0; row < numRows; ++row) {
    if (openGroup_ == nullptr || startsPartition(*input, row)) {
      if (openGroup_ != nullptr) {
        partitions_.push_back({numInputRows_ + row, openGroup_});
      }
      openGroup_ = groups_->newRow();
      newGroupRows_.push_back(row);
    }
    rowGroups_[row] = openGroup_;
  }

  inputRows_.resizeFill(numRows, true);
  for (auto& function : functions_) {
    if (!newGroupRows_.empty()) {
      function.aggregate->initializeNewGroups(
          rowGroups_.data(),
          folly::Range<const vector_size_t*>(
              newGroupRows_.data(), newGroupRows_.size()));
    }
    arguments_.clear();
    for (auto i = 0; i < function.channels.size(); ++i) {
      arguments_.push_back(
          function.channels[i] == kConstantChannel
              ? BaseVector::wrapInConstant(numRows, 0, function.constants[i])
              : input->childAt(function.channels[i]));
    }
    function.aggregate->addRawInput(
        rowGroups_.data(), inputRows_, arguments_, /*mayPushdown=*/false);
  }

  // New vectors each time: copying strings into the same vector would keep a
  // reference to the string buffers of every input batch.
  for (auto i = 0; i < partitionKeyChannels_.size(); ++i) {
    const auto& keys = input->childAt(partitionKeyChannels_[i]);
    lastPartitionKeys_[i] = BaseVector::create(keys->type(), 1, pool_);
    lastPartitionKeys_[i]->copy(keys.get(), 0, numRows - 1, 1);
  }

  const auto bytes = input->retainedSize();
  maxInputBytes_ = std::max(maxInputBytes_, bytes);
  inputs_.push_back(input);
  inputSizes_.push_back(bytes);
  inputBytes_ += bytes;
  numInputRows_ += numRows;
}

void WholePartitionAggregation::noMoreInput() {
  if (openGroup_ != nullptr) {
    partitions_.push_back({numInputRows_, openGroup_});
    openGroup_ = nullptr;
  }
}

uint64_t WholePartitionAggregation::spillReadBytes() const {
  if (currentBatch_ != nullptr || completeRowsEnd() == numOutputRows_ ||
      (spillReader_ == nullptr && spillRuns_.empty())) {
    return 0;
  }
  return maxInputBytes_ + spillConfig_->readBufferSize;
}

RowVectorPtr WholePartitionAggregation::nextBufferedBatch() {
  for (;;) {
    if (spillReader_ != nullptr) {
      RowVectorPtr batch;
      if (spillReader_->nextBatch(batch)) {
        return batch;
      }
      spillReader_.reset();
    }
    if (spillRuns_.empty()) {
      break;
    }
    spillReader_ = spillRuns_.front()->createUnorderedReader(
        spillConfig_->readBufferSize, pool_, spillStats_);
    spillRuns_.pop_front();
  }
  VELOX_CHECK(!inputs_.empty());
  auto batch = std::move(inputs_.front());
  inputs_.pop_front();
  inputBytes_ -= inputSizes_.front();
  inputSizes_.pop_front();
  return batch;
}

RowVectorPtr WholePartitionAggregation::getOutput() {
  const auto endRow = completeRowsEnd();
  if (endRow == numOutputRows_) {
    return nullptr;
  }
  if (currentBatch_ == nullptr) {
    currentBatch_ = nextBufferedBatch();
    currentBatchOffset_ = 0;
  }
  const auto numRows = static_cast<vector_size_t>(std::min<int64_t>(
      currentBatch_->size() - currentBatchOffset_, endRow - numOutputRows_));
  const auto outputEnd = numOutputRows_ + numRows;

  // Partitions of the output rows. The first starts at or before the first
  // output row because fully output partitions are removed.
  std::vector<char*> outputGroups;
  for (const auto& partition : partitions_) {
    outputGroups.push_back(partition.group);
    if (partition.endRow >= outputEnd) {
      break;
    }
  }
  const auto numGroups = static_cast<vector_size_t>(outputGroups.size());

  BufferPtr indices;
  if (numGroups > 1) {
    indices = allocateIndices(numRows, pool_);
    auto* rawIndices = indices->asMutable<vector_size_t>();
    vector_size_t group = 0;
    for (vector_size_t row = 0; row < numRows; ++row) {
      while (numOutputRows_ + row >= partitions_[group].endRow) {
        ++group;
      }
      rawIndices[row] = group;
    }
  }

  std::vector<VectorPtr> columns;
  columns.reserve(outputType_->size());
  const bool wholeBatch =
      currentBatchOffset_ == 0 && numRows == currentBatch_->size();
  for (const auto& child : currentBatch_->children()) {
    columns.push_back(
        wholeBatch ? child : child->slice(currentBatchOffset_, numRows));
  }
  for (auto& function : functions_) {
    auto values = BaseVector::create(function.resultType, numGroups, pool_);
    function.aggregate->extractValues(outputGroups.data(), numGroups, &values);
    columns.push_back(
        numGroups == 1
            ? BaseVector::wrapInConstant(numRows, 0, values)
            : BaseVector::wrapInDictionary(nullptr, indices, numRows, values));
  }

  currentBatchOffset_ += numRows;
  if (currentBatchOffset_ == currentBatch_->size()) {
    currentBatch_ = nullptr;
  }
  numOutputRows_ = outputEnd;

  std::vector<char*> doneGroups;
  while (!partitions_.empty() && partitions_.front().endRow <= numOutputRows_) {
    doneGroups.push_back(partitions_.front().group);
    partitions_.pop_front();
  }
  if (!doneGroups.empty()) {
    groups_->eraseRows(
        folly::Range<char**>(doneGroups.data(), doneGroups.size()));
  }

  return std::make_shared<RowVector>(
      pool_, outputType_, nullptr, numRows, std::move(columns));
}

void WholePartitionAggregation::spill() {
  if (inputs_.empty()) {
    return;
  }
  VELOX_CHECK_NOT_NULL(spillConfig_);
  NoRowContainerSpiller spiller(
      inputType_, std::nullopt, HashBitRange{}, spillConfig_, spillStats_);
  spiller.setPartitionsSpilled({SpillPartitionId(0)});
  for (const auto& batch : inputs_) {
    spiller.spill(SpillPartitionId(0), batch);
  }
  SpillPartitionSet spillPartitions;
  spiller.finishSpill(spillPartitions);
  VELOX_CHECK_EQ(spillPartitions.size(), 1);
  spillRuns_.push_back(std::move(spillPartitions.begin()->second));
  inputs_.clear();
  inputSizes_.clear();
  inputBytes_ = 0;
}

} // namespace facebook::velox::exec::window
