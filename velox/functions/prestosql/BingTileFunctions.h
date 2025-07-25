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

#include "velox/common/base/Status.h"
#include "velox/core/QueryConfig.h"
#include "velox/functions/Macros.h"
#include "velox/functions/prestosql/types/BingTileType.h"

namespace facebook::velox::functions {

FOLLY_ALWAYS_INLINE Status checkBingTileZoom(int32_t zoom) {
  if (FOLLY_UNLIKELY(zoom < 0)) {
    return Status::UserError(
        fmt::format("Bing tile zoom {} cannot be negative", zoom));
  }
  if (FOLLY_UNLIKELY(zoom > BingTileType::kBingTileMaxZoomLevel)) {
    return Status::UserError(fmt::format(
        "Bing tile zoom {} cannot be greater than max zoom {}",
        zoom,
        BingTileType::kBingTileMaxZoomLevel));
  }
  return Status::OK();
}

template <typename T>
struct BingTileFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status call(
      out_type<BingTile>& result,
      const arg_type<int32_t>& x,
      const arg_type<int32_t>& y,
      const arg_type<int32_t>& zoom) {
    if (FOLLY_UNLIKELY(x < 0)) {
      return Status::UserError(
          fmt::format("Bing tile X coordinate {} cannot be negative", x));
    }
    if (FOLLY_UNLIKELY(y < 0)) {
      return Status::UserError(
          fmt::format("Bing tile Y coordinate {} cannot be negative", y));
    }

    auto zoomCheck = checkBingTileZoom(zoom);
    if (FOLLY_UNLIKELY(!zoomCheck.ok())) {
      return std::move(zoomCheck);
    }

    uint64_t tile = BingTileType::bingTileCoordsToInt(
        static_cast<uint32_t>(x),
        static_cast<uint32_t>(y),
        static_cast<uint8_t>(zoom));
    if (FOLLY_UNLIKELY(!BingTileType::isBingTileIntValid(tile))) {
      std::optional<std::string> reason =
          BingTileType::bingTileInvalidReason(tile);
      if (reason.has_value()) {
        return Status::UserError(reason.value());
      } else {
        return Status::UnknownError(fmt::format(
            "Velox Error constructing BingTile from x {} y {} zoom {}; please report this.",
            x,
            y,
            zoom));
      }
    }
    result = tile;
    return Status::OK();
  }

  FOLLY_ALWAYS_INLINE Status
  call(out_type<BingTile>& result, const arg_type<Varchar>& quadKey) {
    folly::Expected<uint64_t, std::string> tile =
        BingTileType::bingTileFromQuadKey(std::string_view(quadKey));
    if (tile.hasError()) {
      return Status::UserError(tile.error());
    }
    VELOX_DCHECK(BingTileType::isBingTileIntValid(tile.value()));
    result = tile.value();
    return Status::OK();
  }
};

template <typename T>
struct BingTileZoomLevelFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<int8_t>& result,
      const arg_type<BingTile>& tile) {
    uint64_t tileInt = tile;
    VELOX_DCHECK(BingTileType::isBingTileIntValid(tileInt));
    result = BingTileType::bingTileZoom(tileInt);
  }
};

template <typename T>
struct BingTileCoordinatesFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Row<int32_t, int32_t>>& result,
      const arg_type<BingTile>& tile) {
    uint64_t tileInt = tile;
    VELOX_DCHECK(BingTileType::isBingTileIntValid(tileInt));
    result = std::make_tuple(
        static_cast<int32_t>(BingTileType::bingTileX(tileInt)),
        static_cast<int32_t>(BingTileType::bingTileY(tileInt)));
  }
};

template <typename T>
struct BingTileParentFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status
  call(out_type<BingTile>& result, const arg_type<BingTile>& tile) {
    uint64_t tileInt = tile;
    VELOX_DCHECK(BingTileType::isBingTileIntValid(tileInt));
    uint8_t tileZoom = BingTileType::bingTileZoom(tile);
    if (FOLLY_UNLIKELY(tileZoom == 0)) {
      return Status::UserError(
          fmt::format("Cannot call bing_tile_parent on zoom 0 tile"));
    }
    auto parent = BingTileType::bingTileParent(tileInt, tileZoom - 1);
    if (FOLLY_UNLIKELY(parent.hasError())) {
      return Status::UserError(parent.error());
    }
    result = parent.value();
    return Status::OK();
  }

  FOLLY_ALWAYS_INLINE Status call(
      out_type<BingTile>& result,
      const arg_type<BingTile>& tile,
      const arg_type<int8_t>& parentZoom) {
    uint64_t tileInt = tile;
    VELOX_DCHECK(BingTileType::isBingTileIntValid(tileInt));
    if (FOLLY_UNLIKELY(parentZoom < 0)) {
      return Status::UserError(
          fmt::format("Cannot call bing_tile_parent with negative zoom"));
    }
    auto parent = BingTileType::bingTileParent(tileInt, parentZoom);
    if (FOLLY_UNLIKELY(parent.hasError())) {
      return Status::UserError(parent.error());
    }
    result = parent.value();
    return Status::OK();
  }

  FOLLY_ALWAYS_INLINE Status call(
      out_type<BingTile>& result,
      const arg_type<BingTile>& tile,
      const arg_type<int32_t>& parentZoom) {
    if (FOLLY_UNLIKELY(parentZoom > BingTileType::kBingTileMaxZoomLevel)) {
      return Status::UserError(fmt::format(
          "newZoom {} is greater than max zoom {}",
          parentZoom,
          BingTileType::kBingTileMaxZoomLevel));
    }
    return call(result, tile, static_cast<int8_t>(parentZoom));
  }
};

template <typename T>
struct BingTileChildrenFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  uint8_t maxZoomShift = 5;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& config,
      const arg_type<BingTile>* /* tile */,
      const arg_type<int32_t>* /* childZoom */) {
    maxZoomShift =
        std::max<uint8_t>(config.debugBingTileChildrenMaxZoomShift(), 1);
  }

  FOLLY_ALWAYS_INLINE Status
  call(out_type<Array<BingTile>>& result, const arg_type<BingTile>& tile) {
    uint64_t tileInt = tile;
    VELOX_DCHECK(BingTileType::isBingTileIntValid(tileInt));
    uint8_t tileZoom = BingTileType::bingTileZoom(tile);
    if (FOLLY_UNLIKELY(tileZoom >= BingTileType::kBingTileMaxZoomLevel)) {
      return Status::UserError(
          fmt::format("Cannot call bing_tile_children on zoom 23 tile"));
    }
    auto childrenRes =
        BingTileType::bingTileChildren(tileInt, tileZoom + 1, maxZoomShift);
    if (FOLLY_UNLIKELY(childrenRes.hasError())) {
      return Status::UserError(childrenRes.error());
    }
    std::vector<uint64_t> children = childrenRes.value();
    result.reserve(children.size());
    result.add_items(children);
    return Status::OK();
  }

  FOLLY_ALWAYS_INLINE Status call(
      out_type<Array<BingTile>>& result,
      const arg_type<BingTile>& tile,
      const arg_type<int32_t>& childZoom) {
    uint64_t tileInt = tile;
    VELOX_DCHECK(BingTileType::isBingTileIntValid(tileInt));
    auto zoomCheck = checkBingTileZoom(childZoom);
    if (FOLLY_UNLIKELY(!zoomCheck.ok())) {
      return std::move(zoomCheck);
    }
    auto childrenRes =
        BingTileType::bingTileChildren(tileInt, childZoom, maxZoomShift);
    if (FOLLY_UNLIKELY(childrenRes.hasError())) {
      return Status::UserError(childrenRes.error());
    }
    std::vector<uint64_t> children = childrenRes.value();
    result.reserve(children.size());
    result.add_items(children);
    return Status::OK();
  }
};

template <typename T>
struct BingTileToQuadKeyFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<BingTile>& tile) {
    uint64_t tileInt = static_cast<uint64_t>(tile);
    VELOX_DCHECK(BingTileType::isBingTileIntValid(tileInt));
    result = BingTileType::bingTileToQuadKey(tileInt);
  }
};

template <typename T>
struct BingTileAtFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status call(
      out_type<BingTile>& result,
      const arg_type<double>& latitude,
      const arg_type<double>& longitude,
      const arg_type<int32_t>& zoomLevel) {
    auto zoomCheck = checkBingTileZoom(zoomLevel);
    if (FOLLY_UNLIKELY(!zoomCheck.ok())) {
      return std::move(zoomCheck);
    }
    auto latitudeLongitudeToTileResult = BingTileType::latitudeLongitudeToTile(
        latitude, longitude, static_cast<uint8_t>(zoomLevel));
    if (FOLLY_UNLIKELY(latitudeLongitudeToTileResult.hasError())) {
      return Status::UserError(latitudeLongitudeToTileResult.error());
    }
    result = latitudeLongitudeToTileResult.value();
    return Status::OK();
  }
};

template <typename T>
struct BingTilesAroundFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status call(
      out_type<Array<BingTile>>& result,
      const arg_type<double>& latitude,
      const arg_type<double>& longitude,
      const arg_type<int32_t>& zoomLevel) {
    auto zoomCheck = checkBingTileZoom(zoomLevel);
    if (FOLLY_UNLIKELY(!zoomCheck.ok())) {
      return std::move(zoomCheck);
    }
    auto bingTilesAroundResult = BingTileType::bingTilesAround(
        latitude, longitude, static_cast<uint8_t>(zoomLevel));
    if (FOLLY_UNLIKELY(bingTilesAroundResult.hasError())) {
      return Status::UserError(bingTilesAroundResult.error());
    }
    std::vector<uint64_t> tiles = bingTilesAroundResult.value();
    result.reserve(static_cast<int32_t>(tiles.size()));
    result.add_items(tiles);
    return Status::OK();
  }

  FOLLY_ALWAYS_INLINE Status call(
      out_type<Array<BingTile>>& result,
      const arg_type<double>& latitude,
      const arg_type<double>& longitude,
      const arg_type<int32_t>& zoomLevel,
      const arg_type<double>& radiusInKm) {
    auto zoomCheck = checkBingTileZoom(zoomLevel);
    if (FOLLY_UNLIKELY(!zoomCheck.ok())) {
      return std::move(zoomCheck);
    }
    auto bingTilesAroundResult = BingTileType::bingTilesAround(
        latitude, longitude, static_cast<uint8_t>(zoomLevel), radiusInKm);

    if (FOLLY_UNLIKELY(bingTilesAroundResult.hasError())) {
      return Status::UserError(bingTilesAroundResult.error());
    }
    std::vector<uint64_t> tiles = bingTilesAroundResult.value();
    result.reserve(static_cast<int32_t>(tiles.size()));
    result.add_items(tiles);
    return Status::OK();
  }
};

} // namespace facebook::velox::functions
