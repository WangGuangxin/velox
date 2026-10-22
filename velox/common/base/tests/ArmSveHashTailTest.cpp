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

#include "velox/common/base/ArmSveHashTail.h"
#include "velox/common/base/BitUtil.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>
#include <array>
#include <random>
#include <stdexcept>
#include <vector>

DECLARE_bool(velox_arm_sve_hash_tail_enabled);

namespace {
namespace bits = facebook::velox::bits;
uint32_t crc32c(uint32_t seed, uint64_t word) {
  for (int i = 0; i < 8; ++i) {
    seed ^= word & 255;
    for (int bit = 0; bit < 8; ++bit) {
      seed = (seed >> 1) ^ (uint32_t(0 - (seed & 1)) & 0x82f63b78U);
    }
    word >>= 8;
  }
  return seed;
}

uint64_t reference(uint64_t seed, const char* input, size_t size) {
  auto word = [&](size_t offset, size_t bytes) {
    uint64_t value = 0;
    for (size_t i = 0; i < bytes; ++i) {
      value |= uint64_t(static_cast<uint8_t>(input[offset + i])) << (i * 8);
    }
    return value;
  };
  if (size < 8) {
    const auto value = word(0, size);
    return uint64_t(crc32c(seed, value)) |
        (uint64_t(crc32c(seed, value >> 32)) << 32);
  }
  uint64_t a0 = seed, a1 = seed << 32, a2 = seed >> 16;
  size_t offset = 0;
  while (size - offset >= 24) {
    a0 = crc32c(a0, word(offset, 8));
    a1 = crc32c(a1, word(offset + 8, 8));
    a2 = crc32c(a2, word(offset + 16, 8));
    offset += 24;
  }
  auto remaining = size - offset;
  if (remaining > 16) {
    a0 = crc32c(a0, word(offset, 8));
    a1 = crc32c(a1, word(offset + 8, 8));
    a2 = crc32c(a2, word(offset + 16, remaining - 16));
  } else if (remaining > 8) {
    a0 = crc32c(a0, word(offset, 8));
    a1 = crc32c(a1, word(offset + 8, remaining - 8));
  } else if (remaining) {
    a0 = crc32c(a0, word(offset, remaining));
  }
  constexpr uint64_t kMul = 0x9ddfea08eb382d69ULL;
  return a0 ^ (a1 * kMul) ^ (a2 * kMul);
}

class ArmSveHashTailTest : public testing::Test {
 protected:
  gflags::FlagSaver restoreFlags_;
  const std::array<uint64_t, 4> seeds_{0, 1, 0x123456789abcdef0, ~uint64_t{0}};

  void verify(const char* data, size_t size) {
    for (auto seed : seeds_) {
      const auto expected = reference(seed, data, size);
      for (bool enabled : {false, true}) {
        FLAGS_velox_arm_sve_hash_tail_enabled = enabled;
        ASSERT_EQ(bits::hashBytes(seed, data, size), expected)
            << "size=" << size << " enabled=" << enabled << " seed=" << seed;
      }
    }
  }
};

class GuardedPage {
 public:
  GuardedPage() : size_(sysconf(_SC_PAGESIZE)) {
    readable_ = ((8192 + size_ - 1) / size_) * size_;
    base_ = static_cast<char*>(mmap(
        nullptr,
        readable_ + size_,
        PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS,
        -1,
        0));
    if (base_ == MAP_FAILED) {
      throw std::runtime_error("mmap failed");
    }
    if (mprotect(base_ + readable_, size_, PROT_NONE)) {
      munmap(base_, readable_ + size_);
      throw std::runtime_error("mprotect failed");
    }
  }
  ~GuardedPage() {
    munmap(base_, readable_ + size_);
  }
  char* end() const {
    return base_ + readable_;
  }

 private:
  size_t size_;
  size_t readable_;
  char* base_;
};

TEST_F(ArmSveHashTailTest, independentReferenceUnalignedAndLongTails) {
  std::mt19937_64 random(0x9a73);
  std::vector<char> data(8192 + 64);
  for (auto& value : data) {
    value = random();
  }
  for (size_t size = 0; size < 128; ++size) {
    for (size_t align = 0; align < 32; ++align) {
      verify(data.data() + align, size);
    }
  }
  for (size_t size : {128, 129, 255, 256, 257, 1023, 1024, 8192}) {
    for (size_t align = 0; align < 32; ++align) {
      verify(data.data() + align, size);
    }
  }
}

TEST_F(ArmSveHashTailTest, noReadPastPageEnd) {
  GuardedPage page;
  std::mt19937_64 random(0x742a);
  for (size_t size = 0; size < 128; ++size) {
    auto* data = page.end() - size;
    for (size_t i = 0; i < size; ++i) {
      data[i] = random();
    }
    verify(data, size);
  }
  for (size_t size : {128, 129, 255, 256, 257, 1023, 1024, 8192}) {
    auto* data = page.end() - size;
    for (size_t i = 0; i < size; ++i) {
      data[i] = random();
    }
    verify(data, size);
  }
}

TEST_F(ArmSveHashTailTest, boundedHelperZeroFillsEveryTail) {
  GuardedPage page;
  EXPECT_EQ(bits::arm_sve_hash_tail::load0To7(nullptr, 0), 0);
  for (int size = 0; size < 8; ++size) {
    auto* data = page.end() - size;
    uint64_t expected = 0;
    for (int i = 0; i < size; ++i) {
      data[i] = static_cast<char>(0x81 + i);
      expected |= uint64_t(static_cast<uint8_t>(data[i])) << (8 * i);
    }
    EXPECT_EQ(
        bits::arm_sve_hash_tail::load0To7(
            reinterpret_cast<const uint8_t*>(data), size),
        expected);
  }
}

TEST_F(ArmSveHashTailTest, allByteValuesAndThreeChainBoundaries) {
  std::array<char, 512> data;
  for (unsigned byte = 0; byte < 256; ++byte) {
    data.fill(static_cast<char>(byte));
    for (size_t tail = 0; tail < 24; ++tail) {
      verify(data.data(), tail);
      verify(data.data(), 24 + tail);
      verify(data.data(), 240 + tail);
    }
  }
}

TEST_F(ArmSveHashTailTest, remainsOptIn) {
  gflags::CommandLineFlagInfo info;
  ASSERT_TRUE(
      gflags::GetCommandLineFlagInfo("velox_arm_sve_hash_tail_enabled", &info));
  EXPECT_EQ(info.default_value, "false");
}
} // namespace
