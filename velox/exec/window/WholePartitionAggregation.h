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

#include <deque>

#include "velox/common/base/SpillConfig.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/RowContainer.h"
#include "velox/exec/Spill.h"
#include "velox/exec/SpillStats.h"

namespace facebook::velox::exec::window {

/// Computes window functions that are all aggregates over the whole partition,
/// e.g. count(*) OVER (PARTITION BY k), when the input is sorted by the
/// partition keys.
///
/// The result of such a function is the same for every row of a partition, so
/// the rows do not need to be materialized: each input batch is added to one
/// accumulator per partition and kept as is until its partition is complete.
/// The kept batches can be spilled to disk at any time and are read back in
/// order. Memory therefore stays bounded however large a partition is, while
/// the generic window builds hold a whole partition in memory.
class WholePartitionAggregation {
 public:
  /// Returns true if 'windowNode' has partition keys, sorted input and only
  /// aggregate functions whose frames span the whole partition.
  static bool supports(const core::WindowNode& windowNode);

  WholePartitionAggregation(
      const std::shared_ptr<const core::WindowNode>& windowNode,
      memory::MemoryPool* pool,
      const common::SpillConfig* spillConfig,
      exec::SpillStats* spillStats,
      const core::QueryConfig& config);

  /// Returns false while rows of complete partitions wait to be output.
  bool needsInput() const;

  void addInput(const RowVectorPtr& input);

  /// Completes the last partition.
  void noMoreInput();

  /// Returns the next rows of complete partitions with the window function
  /// results appended, or nullptr if no such row is buffered. The rows of one
  /// output batch come from one input batch.
  RowVectorPtr getOutput();

  /// Writes the buffered input batches to disk. Must not be called during
  /// addInput() or getOutput().
  void spill();

  /// Returns the bytes of the input batches that spill() would release.
  uint64_t spillableBytes() const {
    return inputBytes_;
  }

  /// Returns the memory the next getOutput() needs to read a spilled batch, 0
  /// if it reads none.
  uint64_t spillReadBytes() const;

 private:
  // A complete partition whose rows are not all output yet.
  struct Partition {
    // Sequence number of the row after the last row of the partition.
    int64_t endRow;
    // Accumulators of the partition in 'groups_'.
    char* group;
  };

  // An aggregate and the input columns of one window function.
  struct Function {
    std::unique_ptr<Aggregate> aggregate;
    // Input channel of each argument, kConstantChannel for constants.
    std::vector<column_index_t> channels;
    // Constant value of each argument, nullptr for columns.
    std::vector<VectorPtr> constants;
    TypePtr resultType;
  };

  // Returns true if 'row' of 'input' is in a different partition than the
  // previous input row.
  bool startsPartition(const RowVector& input, vector_size_t row) const;

  // Returns the next buffered batch in input order, reading spilled batches
  // first.
  RowVectorPtr nextBufferedBatch();

  // Returns the sequence number after the last row of complete partitions.
  int64_t completeRowsEnd() const {
    return partitions_.empty() ? numOutputRows_ : partitions_.back().endRow;
  }

  memory::MemoryPool* const pool_;
  const common::SpillConfig* const spillConfig_;
  exec::SpillStats* const spillStats_;
  const RowTypePtr inputType_;
  const RowTypePtr outputType_;
  std::vector<column_index_t> partitionKeyChannels_;

  // Declared before 'groups_', which destroys accumulators through them.
  std::vector<Function> functions_;

  // One row of accumulators per partition that is not fully output yet.
  std::unique_ptr<RowContainer> groups_;

  // Partition keys of the last input row, one single-row vector per key. Null
  // before the first input row.
  std::vector<VectorPtr> lastPartitionKeys_;

  // Accumulators of the partition the next input row may continue. nullptr
  // before the first input row and after noMoreInput().
  char* openGroup_{nullptr};

  std::deque<Partition> partitions_;

  // Spilled batches, oldest first. Each entry holds the batches of one spill()
  // call.
  std::deque<std::unique_ptr<SpillPartition>> spillRuns_;

  // Reads the oldest spill run.
  std::unique_ptr<UnorderedStreamReader<BatchStream>> spillReader_;

  // Input batches in memory. These are newer than the spilled batches.
  std::deque<RowVectorPtr> inputs_;

  // Retained bytes of each batch in 'inputs_'.
  std::deque<uint64_t> inputSizes_;

  uint64_t inputBytes_{0};

  // Retained bytes of the largest input batch.
  uint64_t maxInputBytes_{0};

  // The batch being output and the offset of its next row.
  RowVectorPtr currentBatch_;
  vector_size_t currentBatchOffset_{0};

  int64_t numInputRows_{0};
  int64_t numOutputRows_{0};

  // Reused per input batch.
  std::vector<char*> rowGroups_;
  std::vector<vector_size_t> newGroupRows_;
  SelectivityVector inputRows_;
  std::vector<VectorPtr> arguments_;
};

} // namespace facebook::velox::exec::window
