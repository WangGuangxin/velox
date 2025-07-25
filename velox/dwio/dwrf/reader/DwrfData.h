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

#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/FormatData.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/dwio/common/compression/Compression.h"
#include "velox/dwio/dwrf/common/ByteRLE.h"
#include "velox/dwio/dwrf/common/RLEv1.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"
#include "velox/dwio/dwrf/reader/EncodingContext.h"
#include "velox/dwio/dwrf/reader/StripeStream.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::dwrf {

// DWRF specific functions shared between all readers.
class DwrfData : public dwio::common::FormatData {
 public:
  DwrfData(
      std::shared_ptr<const dwio::common::TypeWithId> fileType,
      StripeStreams& stripe,
      const StreamLabels& streamLabels,
      FlatMapContext flatMapContext);

  void readNulls(
      vector_size_t numValues,
      const uint64_t* incomingNulls,
      BufferPtr& nulls,
      bool nullsOnly = false) override;

  uint64_t skipNulls(uint64_t numValues, bool nullsOnly = false) override;

  uint64_t skip(uint64_t numValues) override {
    return skipNulls(numValues);
  }

  void filterRowGroups(
      const common::ScanSpec& scanSpec,
      uint64_t rowsPerRowGroup,
      const dwio::common::StatsContext& writerContext,
      FilterRowGroupsResult& result) override;

  bool hasNulls() const override {
    return notNullDecoder_ != nullptr;
  }

  auto* notNullDecoder() const {
    return notNullDecoder_.get();
  }

  const FlatMapContext& flatMapContext() {
    return flatMapContext_;
  }

  const uint64_t* inMap() const {
    return flatMapContext_.inMapDecoder ? inMap_->as<uint64_t>() : nullptr;
  }

  const velox::BufferPtr& inMapBuffer() {
    return inMap_;
  }

  /// Seeks possible flat map in map streams and nulls to the row group
  /// and returns a PositionsProvider for the other streams.
  dwio::common::PositionProvider seekToRowGroup(int64_t index) override;

  int64_t stripeRows() const {
    return stripeRows_;
  }

  std::optional<int64_t> rowsPerRowGroup() const override {
    return rowsPerRowGroup_;
  }

  // Decodes the entry from row group index for 'this' in the stripe,
  // if not already decoded. Throws if no index.
  void ensureRowGroupIndex();

  auto& index() const {
    return *index_;
  }

 private:
  static std::vector<uint64_t> toPositionsInner(
      const proto::RowIndexEntry& entry) {
    return std::vector<uint64_t>(
        entry.positions().begin(), entry.positions().end());
  }

  memory::MemoryPool& memoryPool_;
  const std::shared_ptr<const dwio::common::TypeWithId> fileType_;
  FlatMapContext flatMapContext_;
  std::unique_ptr<BooleanRleDecoder> notNullDecoder_;
  std::unique_ptr<dwio::common::SeekableInputStream> indexStream_;
  std::unique_ptr<proto::RowIndex> index_;
  int64_t stripeRows_;
  // Number of rows in a row group. Last row group may have fewer rows.
  uint32_t rowsPerRowGroup_;

  // Storage for positions backing a PositionProvider returned from
  // seekToRowGroup().
  std::vector<uint64_t> positionsHolder_;

  // In map bitmap used in flat map encoding.
  BufferPtr inMap_;
};

/// DWRF specific initialization.
class DwrfParams : public dwio::common::FormatParams {
 public:
  explicit DwrfParams(
      StripeStreams& stripeStreams,
      const StreamLabels& streamLabels,
      dwio::common::ColumnReaderStatistics& stats,
      FlatMapContext context = {})
      : FormatParams(stripeStreams.getMemoryPool(), stats),
        streamLabels_(streamLabels),
        stripeStreams_(stripeStreams),
        flatMapContext_(context) {}

  std::unique_ptr<dwio::common::FormatData> toFormatData(
      const std::shared_ptr<const dwio::common::TypeWithId>& type,
      const common::ScanSpec& /*scanSpec*/) override {
    return std::make_unique<DwrfData>(
        type, stripeStreams_, streamLabels_, flatMapContext_);
  }

  StripeStreams& stripeStreams() {
    return stripeStreams_;
  }

  FlatMapContext flatMapContext() const {
    return flatMapContext_;
  }

  const StreamLabels& streamLabels() const {
    return streamLabels_;
  }

  const tz::TimeZone* sessionTimezone() const {
    return stripeStreams_.sessionTimezone();
  }

  bool adjustTimestampToTimezone() const {
    return stripeStreams_.adjustTimestampToTimezone();
  }

 private:
  const StreamLabels& streamLabels_;
  StripeStreams& stripeStreams_;
  FlatMapContext flatMapContext_;
};

inline RleVersion convertRleVersion(proto::ColumnEncoding_Kind kind) {
  switch (static_cast<int64_t>(kind)) {
    case proto::ColumnEncoding_Kind_DIRECT:
    case proto::ColumnEncoding_Kind_DICTIONARY:
      return RleVersion_1;
    case proto::ColumnEncoding_Kind_DIRECT_V2:
    case proto::ColumnEncoding_Kind_DICTIONARY_V2:
      return RleVersion_2;
    default:
      VELOX_FAIL(
          "Unknown encoding in convertRleVersion: {}",
          static_cast<int64_t>(kind));
  }
}

inline RleVersion convertRleVersion(proto::orc::ColumnEncoding_Kind kind) {
  switch (kind) {
    case proto::orc::ColumnEncoding_Kind_DIRECT:
    case proto::orc::ColumnEncoding_Kind_DICTIONARY:
      return RleVersion_1;
    case proto::orc::ColumnEncoding_Kind_DIRECT_V2:
    case proto::orc::ColumnEncoding_Kind_DICTIONARY_V2:
      return RleVersion_2;
    default:
      VELOX_FAIL(
          "Unknown encoding in convertRleVersion: {}",
          static_cast<int64_t>(kind));
  }
}

inline RleVersion convertRleVersion(
    const StripeStreams& stripe,
    const EncodingKey& encodingKey) {
  if (stripe.format() == DwrfFormat::kDwrf) {
    return convertRleVersion(stripe.getEncoding(encodingKey).kind());
  } else {
    return convertRleVersion(stripe.getEncodingOrc(encodingKey).kind());
  }
}

} // namespace facebook::velox::dwrf
