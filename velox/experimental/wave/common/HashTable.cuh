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

#include <stdint.h>
#include "velox/experimental/wave/common/Atomic.cuh"
#include "velox/experimental/wave/common/BitUtil.cuh"
#include "velox/experimental/wave/common/Hash.h"
#include "velox/experimental/wave/common/HashTable.h"

namespace facebook::velox::wave {

#define GPF() *(long*)0 = 0

template <typename T, typename U>
inline __device__ Atomic<T, MemoryScope::kDevice>* asDeviceAtomic(U* ptr) {
  return reinterpret_cast<Atomic<T, MemoryScope::kDevice>*>(ptr);
}

template <typename T>
inline bool __device__ atomicTryLock(T* lock) {
  T expected = 0;
  return asDeviceAtomic<int32_t>(lock)
      ->template compare_exchange<MemoryOrder::kAcquire>(expected, 1);
}

template <typename T>
inline void __device__ atomicUnlock(T* lock) {
  asDeviceAtomic<int32_t>(lock)->template store<MemoryOrder::kRelease>(0);
}

namespace detail {
template <typename T>
inline __device__ T* allocateFixed(AllocationRange& range, int32_t size) {
  if (range.fixedFull) {
    return nullptr;
  }
  auto offset = atomicAdd(&range.rowOffset, size);
  if (offset + size <= range.rowLimit) {
    return reinterpret_cast<T*>(range.base + offset);
  }
  range.fixedFull = true;
  return nullptr;
}

template <typename T>
inline __device__ T* allocate(AllocationRange& range, int32_t count) {
  if (range.variableFull) {
    return nullptr;
  }
  auto size = sizeof(T) * count;
  auto offset = atomicAdd(&range.stringOffset, -size);
  if (offset - size >= range.rowLimit) {
    return reinterpret_cast<T*>(range.base + offset);
  }
  range.variableFull = true;
  return nullptr;
}
} // namespace detail

/// Allocator subclass that defines device member functions.
struct RowAllocator : public HashPartitionAllocator {
  template <typename T>
  T* __device__ allocateRow() {
    if (!ranges[0].fixedFull) {
      auto ptr = detail::allocateFixed<T>(ranges[0], rowSize);
      if (ptr) {
        return ptr;
      }
      if (ranges[1].fixedFull) {
        return nullptr;
      }
    }
    return detail::allocateFixed<T>(ranges[1], rowSize);
  }

  template <typename T>
  T* __device__ allocate(int32_t count) {
    if (!ranges[0].variableFull) {
      auto ptr = detail::allocate<T>(ranges[0], count);
      if (ptr) {
        return ptr;
      }
      if (ranges[1].variableFull) {
        return nullptr;
      }
    }
    return detail::allocate<T>(ranges[1], count);
  }

  template <typename T>
  bool __device__ markRowFree(T* row) {
    auto ptr = reinterpret_cast<uintptr_t>(row);
    AllocationRange* rowRange;
    if (ptr >= ranges[0].base + ranges[0].firstRowOffset &&
        ptr < ranges[0].base + ranges[0].rowLimit) {
      rowRange = &ranges[0];
    } else if (
        ptr >= ranges[1].base + ranges[1].firstRowOffset &&
        ptr < ranges[1].base + ranges[1].rowLimit) {
      rowRange = &ranges[1];
    } else {
      return false;
    }
    int32_t idx = (ptr - (rowRange->base + rowRange->firstRowOffset)) / rowSize;
    atomicOr(
        reinterpret_cast<uint32_t*>(rowRange->base) + (idx >> 5),
        1 << (idx & 31));
    return true;
  }
};

inline uint8_t __device__ hashTag(uint64_t h) {
  return 0x80 | (h >> 32);
}

struct GpuBucket : public GpuBucketMembers {
  template <typename RowType>
  inline RowType* __device__ load(int32_t idx) const {
    uint64_t uptr = reinterpret_cast<const uint32_t*>(&data)[idx];
    if (uptr == 0) {
      return nullptr;
    }
    uptr |= static_cast<uint64_t>(data[idx + 8]) << 32;
    return reinterpret_cast<RowType*>(uptr);
  }

  template <typename RowType>
  inline RowType* __device__ loadConsume(int32_t idx) {
    uint64_t uptr = asDeviceAtomic<uint32_t>(&data)[idx]
                        .template load<MemoryOrder::kAcquire>();
    if (uptr == 0) {
      return nullptr;
    }
    uptr |= static_cast<uint64_t>(data[idx + 8]) << 32;
    return reinterpret_cast<RowType*>(uptr);
  }

  template <typename RowType>
  inline RowType* __device__ loadWithWait(int32_t idx) {
    RowType* hit;
    do {
      // It could be somebody inserted the tag but did not fill in the
      // pointer. The pointer is coming in a few clocks.
      hit = loadConsume<RowType>(idx);
    } while (!hit);
    return hit;
  }

  inline void __device__ store(int32_t idx, void* ptr) {
    auto uptr = reinterpret_cast<uint64_t>(ptr);
    data[8 + idx] = uptr >> 32;
    // The high part must be seen if the low part is seen.
    asDeviceAtomic<uint32_t>(&data)[idx].template store<MemoryOrder::kRelease>(
        uptr);
  }

  bool __device__ addNewTag(uint8_t tag, uint32_t oldTags, uint8_t tagShift) {
    uint32_t newTags = oldTags | ((static_cast<uint32_t>(tag) << tagShift));
    return (oldTags == atomicCAS(&tags, oldTags, newTags));
  }
};

class GpuHashTable : public GpuHashTableBase {
 public:
  static constexpr int32_t kExclusive = 1;

  template <typename RowType, typename Compare>
  RowType* __device__ joinProbe(uint64_t h, Compare compare) {
    uint32_t tagWord = hashTag(h);
    tagWord |= tagWord << 8;
    tagWord = tagWord | tagWord << 16;
    auto bucketIdx = h & sizeMask;
    for (;;) {
      GpuBucket* bucket = buckets + bucketIdx;
      auto tags = bucket->tags;
      auto hits = __vcmpeq4(tags, tagWord) & 0x01010101;
      while (hits) {
        auto hitIdx = (__ffs(hits) - 1) / 8;
        auto* hit = bucket->load<RowType>(hitIdx);
        if (compare(hit)) {
          return hit;
        }
        hits = hits & (hits - 1);
      }
      if (__vcmpeq4(tags, 0)) {
        return nullptr;
      }
      bucketIdx = (bucketIdx + 1) & sizeMask;
    }
  }

  template <typename RowType, typename Init>
  bool __device__ addJoinRow(Init init) {
    auto* row = allocators[0].allocateRow<RowType>();
    if (!row) {
      return false;
    }
    init(row);
    return true;
  }

  template <typename RowType, typename Ops>
  void __device__
  updatingProbe(int32_t i, int32_t lane, bool isLaneActive, Ops& ops) {
    uint32_t laneMask = __ballot_sync(0xffffffff, isLaneActive);
    if (!isLaneActive) {
      return;
    }
    auto h = ops.hash(i);
    uint32_t tagWord = hashTag(h);
    tagWord |= tagWord << 8;
    tagWord = tagWord | tagWord << 16;
    auto bucketIdx = h & sizeMask;
    uint32_t misses = 0;
    RowType* hit = nullptr;
    RowType* toInsert = nullptr;
    int32_t hitIdx;
    GpuBucket* bucket;
    uint32_t tags;
    for (;;) {
      bucket = buckets + bucketIdx;
    reprobe:
      tags = asDeviceAtomic<uint32_t>(&bucket->tags)
                 ->template load<MemoryOrder::kAcquire>();
      auto hits = __vcmpeq4(tags, tagWord) & 0x01010101;
      while (hits) {
        hitIdx = (__ffs(hits) - 1) / 8;
        auto candidate = bucket->loadWithWait<RowType>(hitIdx);
        if (ops.compare(this, candidate, i)) {
          if (toInsert) {
            ops.freeInsertable(this, toInsert, h);
          }
          hit = candidate;
          break;
        }
        hits = hits & (hits - 1);
      }
      if (hit) {
        break;
      }
      misses = __vcmpeq4(tags, 0);
      if (misses) {
        auto success = ops.insert(
            this, partitionIdx(h), bucket, misses, tags, tagWord, i, toInsert);
        if (success == ProbeState::kRetry) {
          goto reprobe;
        }
        if (success == ProbeState::kNeedSpace) {
          ops.addHostRetry(i);
          hit = nullptr;
          break;
        }
        hit = toInsert;
        break;
      }
      bucketIdx = (bucketIdx + 1) & sizeMask;
    }
    // Every lane has a hit, or a nullptr if out of space.
    uint32_t peers = __match_any_sync(laneMask, reinterpret_cast<int64_t>(hit));
    if (hit) {
      int32_t leader = (kWarpThreads - 1) - __clz(peers);
      RowType* writable = nullptr;
      if (lane == leader) {
        writable = ops.getExclusive(this, bucket, hit, hitIdx);
      }
      auto toUpdate = peers;
      while (toUpdate) {
        auto peer = __ffs(toUpdate) - 1;
        auto idxToUpdate = __shfl_sync(peers, i, peer);
        if (lane == leader) {
          ops.update(this, bucket, writable, idxToUpdate);
        }
        toUpdate &= toUpdate - 1;
      }
      if (lane == leader) {
        ops.writeDone(writable);
      }
    }
  }

  template <
      typename RowType,
      typename Ops,
      typename Compare,
      typename Init,
      typename Update>
  void __device__ updatingProbe(
      int32_t i,
      int32_t lane,
      bool isLaneActive,
      Ops& ops,
      Compare compare,
      Init init,
      Update update) {
    uint32_t laneMask = __ballot_sync(0xffffffff, isLaneActive);
    if (!isLaneActive) {
      return;
    }
    auto h = ops.hash(i);
    uint32_t tagWord = hashTag(h);
    tagWord |= tagWord << 8;
    tagWord = tagWord | tagWord << 16;
    auto bucketIdx = h & sizeMask;
    uint32_t misses = 0;
    RowType* hit = nullptr;
    RowType* toInsert = nullptr;
    int32_t hitIdx;
    GpuBucket* bucket;
    uint32_t tags;
    for (;;) {
      bucket = buckets + bucketIdx;
    reprobe:
      tags = asDeviceAtomic<uint32_t>(&bucket->tags)
                 ->template load<MemoryOrder::kAcquire>();
      auto hits = __vcmpeq4(tags, tagWord) & 0x01010101;
      while (hits) {
        hitIdx = (__ffs(hits) - 1) / 8;
        auto candidate = bucket->loadWithWait<RowType>(hitIdx);
        if (compare(candidate)) {
          if (toInsert) {
            ops.freeInsertable(this, toInsert, h);
          }
          hit = candidate;
          break;
        }
        hits = hits & (hits - 1);
      }
      if (hit) {
        break;
      }
      misses = __vcmpeq4(tags, 0);
      if (misses) {
        auto success = ops.insert(
            this,
            partitionIdx(h),
            bucket,
            misses,
            tags,
            tagWord,
            i,
            toInsert,
            init);
        if (success == ProbeState::kRetry) {
          goto reprobe;
        }
        if (success == ProbeState::kNeedSpace) {
          ops.addHostRetry(i);
          hit = nullptr;
          break;
        }
        hit = toInsert;
        break;
      }
      bucketIdx = (bucketIdx + 1) & sizeMask;
    }
    // Every lane has a hit, or a nullptr if out of space.
    uint32_t peers = __match_any_sync(laneMask, reinterpret_cast<int64_t>(hit));
    if (hit) {
      int32_t leader = (kWarpThreads - 1) - __clz(peers);
      RowType* writable = nullptr;
      if (lane == leader) {
        writable = ops.getExclusive(this, bucket, hit, hitIdx);
      }
      update(this, hit, peers, leader, lane);
      if (lane == leader) {
        ops.writeDone(writable);
      }
    }
  }

  template <typename RowType, typename Ops>
  void __device__
  rehash(GpuBucket* oldBuckets, int32_t numOldBuckets, Ops ops) {
    auto stride = blockDim.x * gridDim.x;
    for (auto idx = threadIdx.x + blockDim.x * blockIdx.x; idx < numOldBuckets;
         idx += stride) {
      for (auto slot = 0; slot < GpuBucketMembers::kNumSlots; ++slot) {
        auto* row = oldBuckets[idx].load<RowType>(slot);
        if (row) {
          uint64_t h = ops.hashRow(row);
          auto bucketIdx = h & sizeMask;
          uint32_t tagWord = hashTag(h);
          tagWord |= tagWord << 8;
          tagWord = tagWord | tagWord << 16;

          for (;;) {
            GpuBucket* bucket = buckets + bucketIdx;
          reprobe:
            uint32_t tags = asDeviceAtomic<uint32_t>(&bucket->tags)
                                ->template load<MemoryOrder::kAcquire>();
            auto misses = __vcmpeq4(tags, 0) & 0x01010101;
            while (misses) {
              auto missShift = __ffs(misses) - 1;
              if (!bucket->addNewTag(tagWord, tags, missShift)) {
                goto reprobe;
              }
              bucket->store(missShift / 8, row);
              goto next;
            }
            bucketIdx = (bucketIdx + 1) & sizeMask;
          }
        }
      next:;
      }
    }
    __syncthreads();
  }

  template <typename RowType, typename Ops>
  void __device__ joinBuild(RowType* rows, int32_t numRows, Ops ops) {
    auto stride = blockDim.x * gridDim.x;
    for (auto idx = threadIdx.x + blockDim.x * blockIdx.x; idx < numRows;
         idx += stride) {
      auto* row = rows + idx;
      uint64_t h = ops.hashRow(row);
      auto bucketIdx = h & sizeMask;
      uint32_t tagWord = hashTag(h);
      tagWord |= tagWord << 8;
      tagWord = tagWord | tagWord << 16;

      for (;;) {
        GpuBucket* bucket = buckets + bucketIdx;
      reprobe:
        uint32_t tags = asDeviceAtomic<uint32_t>(&bucket->tags)
                            ->template load<MemoryOrder::kAcquire>();
        auto hits = __vcmpeq4(tags, tagWord) & 0x01010101;
        while (hits) {
          auto hitIdx = (__ffs(hits) - 1) / 8;
          auto candidate = bucket->loadWithWait<RowType>(hitIdx);
          if (ops.compare(row, candidate)) {
            for (;;) {
              auto previous =
                  (RowType*)asDeviceAtomic<uintptr_t>(candidate->nextPtr())
                      ->template load<MemoryOrder::kRelaxed>();
              if ((unsigned long long)previous ==
                  atomicCAS(
                      (unsigned long long*)&candidate->next,
                      (unsigned long long)previous,
                      (unsigned long long)row)) {
                *row->nextPtr() = previous;
                // Set duplicates flag, no need to set if already set.
                atomicCAS(&hasDuplicates, 0, 1);
                goto next;
              }
            }
          }
          hits &= hits - 1;
        }
        auto misses = __vcmpeq4(tags, 0) & 0x01010101;
        if (misses) {
          auto missShift = __ffs(misses) - 1;
          if (!bucket->addNewTag(tagWord, tags, missShift)) {
            goto reprobe;
          }
          bucket->store(missShift / 8, row);
          goto next;
        }

        bucketIdx = (bucketIdx + 1) & sizeMask;
      }
    next:;
    }
    __syncthreads();
  }

  int32_t __device__ partitionIdx(uint64_t h) const {
    return partitionMask == 0 ? 0 : (h >> 41) & partitionMask;
  }
};
} // namespace facebook::velox::wave
