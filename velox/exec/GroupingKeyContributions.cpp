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

#include "velox/exec/GroupingKeyContributions.h"

#include <algorithm>

namespace facebook::velox::exec {

GroupingKeyContributions::GroupingKeyContributions(
    int32_t numKeys,
    memory::MemoryPool* pool,
    int64_t maxBytes)
    : maxBytes_(maxBytes) {
  VELOX_CHECK_NOT_NULL(pool);
  VELOX_CHECK_GE(numKeys, 0);
  VELOX_CHECK_GE(maxBytes, 0);
  entries_.reserve(numKeys);
  for (int32_t i = 0; i < numKeys; ++i) {
    entries_.emplace_back(pool);
  }
}

GroupingKeyContributions::Entry& GroupingKeyContributions::entryAt(
    int32_t key) {
  VELOX_CHECK_LT(
      key,
      entries_.size(),
      "Grouping key contributions were sized for fewer keys than the "
      "table has");
  return entries_[key];
}

void GroupingKeyContributions::clear() {
  for (auto& entry : entries_) {
    entry.source.reset();
    entry.values.clear();
    entry.valid = false;
    entry.filling = false;
  }
}

const uint64_t* GroupingKeyContributions::find(
    int32_t key,
    const BaseVector& source,
    bool usesValueIds,
    const SelectivityVector& rows) {
  auto& entry = entryAt(key);
  if (!entry.valid || entry.usesValueIds != usesValueIds ||
      entry.source.get() != &source || !rows.isSubset(entry.rows)) {
    return nullptr;
  }
  numReusedRows_ += rows.countSelected();
  return entry.values.data();
}

raw_vector<uint64_t>* GroupingKeyContributions::prepareFill(
    int32_t key,
    const SelectivityVector& rows) {
  auto& entry = entryAt(key);
  // A fill replaces whatever the entry held. Dropping it first keeps find()
  // from ever comparing against a source the buffer no longer describes.
  entry.source.reset();
  entry.valid = false;
  entry.filling = false;
  const auto numRows = rows.end();
  const int64_t bytes = static_cast<int64_t>(numRows) * sizeof(uint64_t);
  if (bytes > maxBytes_ / std::max<int64_t>(1, entries_.size())) {
    numUncachedRows_ += rows.countSelected();
    return nullptr;
  }
  entry.values.resize(numRows);
  // computeValueIds() accumulates for every hasher past the first, so the
  // buffer has to start at zero for the contribution to stand alone.
  std::fill(entry.values.begin(), entry.values.end(), 0);
  entry.filling = true;
  return &entry.values;
}

void GroupingKeyContributions::publish(
    int32_t key,
    const VectorPtr& source,
    bool usesValueIds,
    const SelectivityVector& rows) {
  auto& entry = entryAt(key);
  VELOX_CHECK(entry.filling);
  entry.source = source;
  entry.rows = rows;
  entry.usesValueIds = usesValueIds;
  entry.valid = true;
  entry.filling = false;
  numComputedRows_ += rows.countSelected();
}

void GroupingKeyContributions::abandonFill(int32_t key) {
  auto& entry = entryAt(key);
  entry.valid = false;
  entry.filling = false;
}

} // namespace facebook::velox::exec
