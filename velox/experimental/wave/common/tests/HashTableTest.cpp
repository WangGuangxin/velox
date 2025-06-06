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

#include <gtest/gtest.h>
#include "velox/common/time/Timer.h"
#include "velox/experimental/wave/common/Buffer.h"
#include "velox/experimental/wave/common/GpuArena.h"
#include "velox/experimental/wave/common/tests/BlockTest.h"
#include "velox/experimental/wave/common/tests/CpuTable.h"
#include "velox/experimental/wave/common/tests/HashTestUtil.h"

#include <iostream>

DEFINE_int32(
    hash_num_rows_per_thread,
    32,
    "Number of rows per thread in hash table tests");

namespace facebook::velox::wave {

class CpuMockGroupByOps {
 public:
  bool
  compare(CpuHashTable* table, TestingRow* row, int32_t i, HashProbe* probe) {
    return row->key == reinterpret_cast<int64_t**>(probe->keys)[0][i];
  }

  bool compare1(const CpuHashTable* table, TestingRow* row, int64_t key) {
    return key == row->key;
  }

  TestingRow* newRow(CpuHashTable* table, int32_t i, HashProbe* probe) {
    auto row = table->newRow<TestingRow>();
    row->key = reinterpret_cast<int64_t**>(probe->keys)[0][i];
    row->flags = 0;
    row->count = 0;
    new (&row->concatenation) ArrayAgg64();
    return row;
  }

  void
  update(CpuHashTable* table, TestingRow* row, int32_t i, HashProbe* probe) {
    auto* keys = reinterpret_cast<int64_t**>(probe->keys);
    row->count += keys[1][i];

#if 0
      int64_t arg = keys[1][i];
      int32_t part = table->partitionIdx(bucket - table->buckets);
      auto* allocator = &table->allocators[part];
      auto state = arrayAgg64Append(&row->concatenation, arg, allocator);
#endif
  }
};

class HashTableTest : public testing::Test {
 protected:
  void SetUp() override {
    device_ = getDevice();
    setDevice(device_);
    allocator_ = getAllocator(device_);
    arena_ = std::make_unique<GpuArena>(1 << 28, allocator_);
    streams_.push_back(std::make_unique<BlockTestStream>());
  }

  void prefetch(Stream& stream, WaveBufferPtr buffer) {
    stream.prefetch(device_, buffer->as<char>(), buffer->capacity());
  }

  // Tests different styles of updating a group by. Results are returned in
  // 'run'.
  void updateTestCase(int32_t numDistinct, int32_t numRows, HashRun& run) {
    run.numRows = numRows;
    run.numDistinct = numDistinct;
    run.numColumns = 2;
    run.numRowsPerThread = 32;

    initializeHashTestInput(run, arena_.get());
    fillHashTestInput(
        run.numRows,
        run.numDistinct,
        bits::nextPowerOfTwo(run.numDistinct),
        1,
        run.numColumns,
        reinterpret_cast<int64_t**>(run.probe->keys));
    std::vector<TestingRow> reference(run.numDistinct);
    for (auto i = 0; i < run.numDistinct; ++i) {
      reference[i].key = i;
    }
    gpuRowsBuffer_ = arena_->allocate<TestingRow>(run.numDistinct);
    TestingRow* gpuRows = gpuRowsBuffer_->as<TestingRow>();
    memcpy(gpuRows, reference.data(), sizeof(TestingRow) * run.numDistinct);
    prefetch(*streams_[0], gpuRowsBuffer_);
    prefetch(*streams_[0], run.gpuData);
    streams_[0]->wait();
    updateCpu(reference.data(), run);
    updateGpu(gpuRows, run, reference.data());
    std::cout << run.toString() << std::endl;
  }

  void updateCpu(TestingRow* rows, HashRun& run) {
    uint64_t micros = 0;
    {
      MicrosecondTimer t(&micros);
      switch (run.testCase) {
        case HashTestCase::kUpdateSum1: {
          int64_t** keys = reinterpret_cast<int64_t**>(run.probe->keys);
          int64_t* indices = keys[0];
          int64_t* data = keys[1];
          auto numRows = run.numRows;
          for (auto i = 0; i < numRows; ++i) {
            rows[indices[i]].count += data[i];
          }
          break;
        }
        default:
          VELOX_FAIL("Unsupported test case");
      }
    }
    run.addScore("cpu1t", micros);
  }

#define UPDATE_CASE(title, func, expectCorrect, nextFlags) \
  {                                                        \
    std::cout << title << std::endl;                       \
    MicrosecondTimer t(&micros);                           \
    streams_[0]->func(rows, run);                          \
    streams_[0]->wait();                                   \
  }                                                        \
  run.addScore(title, micros);                             \
  micros = 0;                                              \
  compareAndReset(                                         \
      reference, rows, run.numDistinct, title, expectCorrect, nextFlags);

  const char* jitMtxCoa =
      "#include \"velox/experimental/wave/common/Atomic.cuh\"\n"
      "#include \"velox/experimental/wave/common/HashTable.cuh\"\n"
      "\n"
      "namespace facebook::velox::wave {\n"
      "/// A mock aggregate that concatenates numbers, like array_agg of bigint.\n"
      "struct ArrayAgg64 {\n"
      "  struct Run {\n"
      "    Run* next;\n"
      "    int64_t data[16];\n"
      "  };\n"
      "\n"
      "  Run* first{nullptr};\n"
      "  Run* last{nullptr};\n"
      "  // Fill of 'last->data', all other runs are full.\n"
      "  int8_t numInLast{0};\n"
      "};\n"
      "\n"
      "/// A mock hash table content row to test HashTable.\n"
      "struct TestingRow {\n"
      "  // Single ke part.\n"
      "  int64_t key;\n"
      "\n"
      "  // Count of updates. Sample aggregate\n"
      "  int64_t count{0};\n"
      "\n"
      "  // A mock concatenating aggregate. Use for testing control flow in\n"
      "  // running out of space in updating a group.\n"
      "  ArrayAgg64 concatenation;\n"
      "\n"
      "  // Next pointer in the case simulating a non-unique join table.\n"
      "  TestingRow* next{nullptr};\n"
      "\n"
      "  // flags for updating the row. E.g. probed flag, marker for exclusive write.\n"
      "  int32_t flags{0};\n"
      "};\n"
      "using Mutex = AtomicMutex<MemoryScope::kDevice>;\n"
      "inline void __device__ testingLock(int32_t* mtx) {\n"
      "  reinterpret_cast<Mutex*>(mtx)->acquire();\n"
      "}\n"
      "\n"
      "inline void __device__ testingUnlock(int32_t* mtx) {\n"
      "  reinterpret_cast<Mutex*>(mtx)->release();\n"
      "}\n"
      "\n"
      " __global__ void jitSumMtxCoalesce(TestingRow* rows, HashProbe* probe) {\n"
      "  constexpr int32_t kWarpThreads = 32;\n"
      "  auto keys = reinterpret_cast<int64_t**>(probe->keys);\n"
      "  auto indices = keys[0];\n"
      "  auto deltas = keys[1];\n"
      "  int32_t base = probe->numRowsPerThread * blockDim.x * blockIdx.x;\n"
      "  int32_t lane = LaneId();\n"
      "  int32_t end = base + probe->numRows[blockIdx.x];\n"
      "\n"
      "  for (auto count = base; count < end; count += blockDim.x) {\n"
      "    auto i = threadIdx.x + count;\n"
      "\n"
      "    if (i < end) {\n"
      "      uint32_t laneMask = count + kWarpThreads <= end\n"
      "          ? 0xffffffff\n"
      "          : lowMask<uint32_t>(end - count);\n"
      "      auto index = indices[i];\n"
      "      auto delta = deltas[i];\n"
      "      uint32_t allPeers = __match_any_sync(laneMask, index);\n"
      "      int32_t leader = __ffs(allPeers) - 1;\n"
      "      auto peers = allPeers;\n"
      "      int64_t total = 0;\n"
      "      auto currentPeer = leader;\n"
      "      for (;;) {\n"
      "        total += __shfl_sync(allPeers, delta, currentPeer);\n"
      "        peers &= peers - 1;\n"
      "        if (peers == 0) {\n"
      "          break;\n"
      "        }\n"
      "        currentPeer = __ffs(peers) - 1;\n"
      "      }\n"
      "      if (lane == leader) {\n"
      "        auto* row = &rows[index];\n"
      "        testingLock(&row->flags);\n"
      "        row->count += total;\n"
      "        testingUnlock(&row->flags);\n"
      "      }\n"
      "    }\n"
      "  }\n"
      "  }\n"
      "}\n";

  void updateJitMtxCoa(TestingRow* rows, HashRun& run, TestingRow* reference) {
    std::cout << "updateJitMtxCoa" << std::endl;
    KernelSpec spec = {
        jitMtxCoa,
        {"facebook::velox::wave::jitSumMtxCoalesce"},
        "sum1Jit.cu",
        0,
        nullptr,
        nullptr};
    auto kernel = CompiledModule::create(spec);
    uint64_t micros = 0;
    {
      MicrosecondTimer t(&micros);
      void* params[] = {&rows, &run.probe};
      kernel->launch(
          0, run.numBlocks, run.blockSize, 0, streams_[0].get(), params);
      streams_[0]->wait();
    }
    run.addScore("sum1JitMtxCoa", micros);
    compareAndReset(reference, rows, run.numDistinct, "sum1JitMtxCoa", true, 1);
  }

  void updateGpu(TestingRow* rows, HashRun& run, TestingRow* reference) {
    uint64_t micros = 0;
    switch (run.testCase) {
      case HashTestCase::kUpdateSum1:
        UPDATE_CASE("sum1Atm", updateSum1Atomic, true, 0);
        UPDATE_CASE("sum1NoSync", updateSum1NoSync, false, 0);
        UPDATE_CASE("sum1AtmCoaShfl", updateSum1AtomicCoalesceShfl, true, 1);
        UPDATE_CASE("sum1AtmCoaShmem", updateSum1AtomicCoalesceShmem, true, 1);
        UPDATE_CASE("sum1Mtx", updateSum1Mtx, true, 1);
        updateJitMtxCoa(rows, run, reference);
        UPDATE_CASE("sum1MtxCoa", updateSum1MtxCoalesce, true, 0);
        UPDATE_CASE("sum1Part", updateSum1Part, true, 0);
        // UPDATE_CASE("sum1Order", updateSum1Order, true, 0);

        break;
      default:
        VELOX_FAIL("Unsupported test case");
    }
  }

  void compareAndReset(
      TestingRow* reference,
      TestingRow* rows,
      int32_t numRows,
      const char* title,
      bool expectCorrect,
      int32_t initFlags = 0) {
    int32_t numError = 0;
    int64_t errorSigned = 0;
    int64_t errorDelta = 0;
    for (auto i = 0; i < numRows; ++i) {
      if (rows[i].count == reference[i].count) {
        continue;
      }
      if (numError == 0 && expectCorrect) {
        std::cout << "In " << title << std::endl;
        EXPECT_EQ(reference[i].count, rows[i].count) << " at " << i;
      }
      ++numError;
      int64_t d = reference[i].count - rows[i].count;
      errorSigned += d;
      errorDelta += d < 0 ? -d : d;
    }
    if (numError) {
      std::cout << fmt::format(
                       "{}: numError={} errorDelta={} errorSigned={}",
                       title,
                       numError,
                       errorDelta,
                       errorSigned)
                << std::endl;
    }
    for (auto i = 0; i < numRows; ++i) {
      new (rows + i) TestingRow();
      rows[i].key = i;
      rows[i].flags = initFlags;
    }
    prefetch(*streams_[0], gpuRowsBuffer_);
    streams_[0]->wait();
  }

  void groupTestCase(int32_t numDistinct, int32_t numRows, HashRun& run) {
    run.numRows = numRows;
    run.numDistinct = numDistinct;
    if (!run.numSlots) {
      run.numSlots = bits::nextPowerOfTwo(numDistinct);
    }
    run.numColumns = 2;
    run.numRowsPerThread = FLAGS_hash_num_rows_per_thread;

    initializeHashTestInput(run, arena_.get());
    fillHashTestInput(
        run.numRows,
        run.numDistinct,
        bits::nextPowerOfTwo(run.numDistinct),
        1,
        run.numColumns,
        reinterpret_cast<int64_t**>(run.probe->keys));
    CpuHashTable cpuTable(run.numSlots, sizeof(TestingRow) * run.numDistinct);
    cpuGroupBy(cpuTable, run);
    gpuGroupBy(cpuTable, run);
    std::cout << run.toString() << std::endl;
  }

  void cpuGroupBy(CpuHashTable& table, HashRun& run) {
    uint64_t time = 0;
    {
      MicrosecondTimer t(&time);
      int64_t* key = reinterpret_cast<int64_t**>(run.probe->keys)[0];
      auto* hashes = run.probe->hashes;
      for (auto i = 0; i < run.numRows; ++i) {
        hashes[i] = bits::hashMix(1, key[i]);
      }
      table.updatingProbe<TestingRow>(
          run.numRows, run.probe, CpuMockGroupByOps());
    }
    run.addScore("cpu1T", time);
  }

  void gpuGroupBy(const CpuHashTable& reference, HashRun& run) {
    WaveBufferPtr gpuTableBuffer;
    GpuHashTableBase* gpuTable;
    setupGpuTable(
        run.numSlots,
        run.numRows,
        sizeof(TestingRow),
        arena_.get(),
        gpuTable,
        gpuTableBuffer);
    prefetch(*streams_[0], run.gpuData);
    prefetch(*streams_[0], gpuTableBuffer);
    streams_[0]->wait();
    uint64_t micros = 0;
    {
      MicrosecondTimer t(&micros);
      streams_[0]->hashTest(gpuTable, run, BlockTestStream::HashCase::kGroup);
      streams_[0]->wait();
    }
    run.addScore("gpu", micros);
    checkGroupBy(reference, gpuTable);
    auto size = gpuTable->sizeMask + 1;
    auto oldBuckets = gpuTable->buckets;
    WaveBufferPtr newBuckets = arena_->allocate<GpuBucketMembers>(size);
    gpuTable->buckets = newBuckets->as<GpuBucket>();

    streams_[0]->prefetch(getDevice(), gpuTable, sizeof(GpuHashTableBase));
    streams_[0]->memset(newBuckets->as<char>(), 0, newBuckets->size());
    streams_[0]->rehash(gpuTable, oldBuckets, size);
    streams_[0]->wait();
    checkGroupBy(reference, gpuTable);
  }

  void checkGroupBy(const CpuHashTable& reference, GpuHashTableBase* table) {
    int32_t numChecked = 0;
    for (auto i = 0; i <= table->sizeMask; ++i) {
      for (auto j = 0; j < 4; ++j) {
        auto* row = reinterpret_cast<GpuBucketMembers*>(table->buckets)[i]
                        .testingLoad<TestingRow>(j);
        if (row == nullptr) {
          continue;
        }
        ++numChecked;
        auto referenceRow = reference.find<TestingRow>(
            row->key, bits::hashMix(1, row->key), CpuMockGroupByOps());
        ASSERT_TRUE(referenceRow != nullptr);
        EXPECT_EQ(referenceRow->count, row->count);
      }
    }
    EXPECT_EQ(reference.size, numChecked);
  }

  Device* device_;
  GpuAllocator* allocator_;
  std::unique_ptr<GpuArena> arena_;
  std::vector<std::unique_ptr<BlockTestStream>> streams_;
  WaveBufferPtr gpuRowsBuffer_;
};

TEST_F(HashTableTest, allocator) {
  constexpr int32_t kNumThreads = 256;
  constexpr int32_t kTotal = 1 << 22;
  WaveBufferPtr data = arena_->allocate<char>(kTotal);
  auto* allocator = data->as<ArenaWithFreeBase>();
  auto freeSetSize = BlockTestStream::freeSetSize();
  new (allocator) ArenaWithFreeBase(
      data->as<char>() + sizeof(ArenaWithFreeBase) + freeSetSize,
      kTotal - sizeof(ArenaWithFreeBase) - freeSetSize,
      16,
      allocator + 1);
  memset(allocator->freeSet, 0, freeSetSize);
  WaveBufferPtr allResults = arena_->allocate<AllocatorTestResult>(kNumThreads);
  auto results = allResults->as<AllocatorTestResult>();
  for (auto i = 0; i < kNumThreads; ++i) {
    results[i].allocator = reinterpret_cast<ArenaWithFree*>(allocator);
    results[i].numRows = 0;
    results[i].numStrings = 0;
  }
  auto stream1 = std::make_unique<BlockTestStream>();
  auto stream2 = std::make_unique<BlockTestStream>();
  stream1->initAllocator(reinterpret_cast<ArenaWithFree*>(allocator));
  stream1->wait();
  stream1->rowAllocatorTest(2, 4, 3, 2, results);
  stream2->rowAllocatorTest(2, 4, 3, 2, results + 128);

  stream1->wait();
  stream2->wait();
  // Pointer to result idx, position in result;
  std::unordered_map<int64_t*, int32_t> uniques;
  for (auto resultIdx = 0; resultIdx < kNumThreads; ++resultIdx) {
    auto* result = results + resultIdx;
    for (auto i = 0; i < result->numRows; ++i) {
      auto row = result->rows[i];
      EXPECT_GE(reinterpret_cast<uint64_t>(row), allocator->base);
      EXPECT_LT(
          reinterpret_cast<uint64_t>(row),
          allocator->base + allocator->capacity);
      auto it = uniques.find(row);
      EXPECT_TRUE(it == uniques.end()) << fmt::format(
          "row {} is also at {} {}",
          reinterpret_cast<uint64_t>(row),
          it->second >> 24,
          it->second & bits::lowMask(24));

      uniques[row] = (resultIdx << 24) | i;
    }
    for (auto i = 0; i < result->numStrings; ++i) {
      auto string = result->strings[i];
      EXPECT_GE(reinterpret_cast<uint64_t>(string), allocator->base);
      EXPECT_LT(
          reinterpret_cast<uint64_t>(string),
          allocator->base + allocator->capacity);
      auto it = uniques.find(string);
      EXPECT_TRUE(it == uniques.end()) << fmt::format(
          "String {} is also at {} {}",
          reinterpret_cast<uint64_t>(string),
          it->second >> 24,
          it->second & bits::lowMask(24));
      uniques[string] = (resultIdx << 24) | i;
    }
  }
}

TEST_F(HashTableTest, update) {
  {
    HashRun run;
    run.testCase = HashTestCase::kUpdateSum1;
    updateTestCase(10000000, 2000000, run);
  }
  {
    HashRun run;
    run.testCase = HashTestCase::kUpdateSum1;
    updateTestCase(100000, 2000000, run);
  }
  {
    HashRun run;
    run.testCase = HashTestCase::kUpdateSum1;
    updateTestCase(1000, 2000000, run);
  }
  {
    HashRun run;
    run.testCase = HashTestCase::kUpdateSum1;
    updateTestCase(10, 2000000, run);
  }
  {
    HashRun run;
    run.testCase = HashTestCase::kUpdateSum1;
    updateTestCase(1, 2000000, run);
  }
}

TEST_F(HashTableTest, groupBy) {
  {
    HashRun run;
    run.testCase = HashTestCase::kGroupSum1;
    run.numSlots = 2048;
    groupTestCase(1000, 2000000, run);
  }
  {
    HashRun run;
    run.testCase = HashTestCase::kGroupSum1;
    run.numSlots = 8 << 20;
    groupTestCase(5000000, 50000000, run);
  }
}

} // namespace facebook::velox::wave
