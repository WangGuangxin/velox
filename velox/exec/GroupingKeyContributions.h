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

#include <cstdint>
#include <vector>

#include "velox/common/memory/RawVector.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::exec {

/// Caches what each grouping key contributes to a row's hash or value ID, so
/// that the grouping sets fanned out from one input batch compute it once.
///
/// An Expand operator emits one output batch per grouping set and hands every
/// one of them the same key vector, differing only in which keys it replaces
/// with a null constant. Without this cache the aggregation hashes each key
/// once per grouping set that keeps it.
///
/// Reuse is keyed on vector identity. Retaining a reference to the source is
/// what makes that sound: an upstream operator asked to reuse a vector sees a
/// non-unique reference and allocates a new one instead, so a surviving
/// address can never belong to different data.
class GroupingKeyContributions {
 public:
  static constexpr int64_t kDefaultMaxBytes = 8 << 20;

  /// Caches up to 'maxBytes' of contributions for 'numKeys' grouping keys.
  /// Keys whose batch would exceed the budget stay uncached.
  GroupingKeyContributions(
      int32_t numKeys,
      memory::MemoryPool* pool,
      int64_t maxBytes);

  /// Drops everything. Call whenever contributions computed earlier stop
  /// being comparable to new ones: a hash mode change, a rebuilt value-ID
  /// dictionary, or a spill.
  void clear();

  /// Returns 'key's contributions if they were computed from 'source' in the
  /// same mode and cover every row in 'rows'. Returns nullptr otherwise. The
  /// result is invalidated by the next prepareFill() for the same key.
  const uint64_t* FOLLY_NULLABLE find(
      int32_t key,
      const BaseVector& source,
      bool usesValueIds,
      const SelectivityVector& rows);

  /// Returns a zeroed buffer covering 'rows' for the caller to fill by
  /// passing it to VectorHasher::hash() or VectorHasher::computeValueIds().
  /// Returns nullptr if 'key' cannot be cached, in which case the caller
  /// computes into its own accumulator as usual.
  raw_vector<uint64_t>* FOLLY_NULLABLE
  prepareFill(int32_t key, const SelectivityVector& rows);

  /// Publishes the buffer the matching prepareFill() returned as 'key's
  /// contributions over 'rows'.
  void publish(
      int32_t key,
      const VectorPtr& source,
      bool usesValueIds,
      const SelectivityVector& rows);

  /// Discards an unpublished fill, for instance because computeValueIds()
  /// reported that the value IDs no longer fit.
  void abandonFill(int32_t key);

  /// Rows served from the cache.
  int64_t numReusedRows() const {
    return numReusedRows_;
  }

  /// Rows whose contribution had to be computed.
  int64_t numComputedRows() const {
    return numComputedRows_;
  }

  /// Rows for which no entry could be kept, for instance because the buffer
  /// would not fit the byte budget.
  int64_t numUncachedRows() const {
    return numUncachedRows_;
  }

 private:
  struct Entry {
    explicit Entry(memory::MemoryPool* pool) : values(pool) {}

    // Held so that an upstream operator cannot reuse the vector while its
    // contributions are still cached.
    VectorPtr source;
    raw_vector<uint64_t> values;
    // Rows 'values' was computed for. A later probe may select fewer.
    SelectivityVector rows;
    bool usesValueIds{false};
    bool valid{false};
    bool filling{false};
  };

  Entry& entryAt(int32_t key);

  const int64_t maxBytes_;
  std::vector<Entry> entries_;
  int64_t numReusedRows_{0};
  int64_t numComputedRows_{0};
  int64_t numUncachedRows_{0};
};

} // namespace facebook::velox::exec
