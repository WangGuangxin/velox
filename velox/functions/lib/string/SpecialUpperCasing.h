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

#include <algorithm>
#include <array>
#include <optional>
#include <string_view>

#include "folly/CPortability.h"
#include "velox/external/utf8proc/utf8procImpl.h"

namespace facebook::velox::functions {

/// A code point whose full uppercase mapping is a sequence of code points.
struct SpecialUpperCasingEntry {
  /// Code point to convert.
  utf8proc_int32_t codePoint;
  /// UTF-8 encoding of the uppercase sequence.
  std::string_view upper;
};

/// Unconditional multi code point uppercase mappings from Unicode
/// SpecialCasing.txt, sorted by code point. utf8proc_toupper only implements
/// the single code point mappings of UnicodeData.txt, while Java's
/// String.toUpperCase and ICU's UCharacter.toUpperCase, which Spark uses, apply
/// these as well. For example, U+00DF (sharp s) becomes "SS" and U+FB03 (ffi
/// ligature) becomes "FFI". No entry expands to more than 3 times its UTF-8
/// length, so a buffer of 4 bytes per input byte stays sufficient.
inline constexpr std::array<SpecialUpperCasingEntry, 102> kSpecialUpperCasing{{
    {0x00DF, "\x53\x53"}, // U+0053 U+0053
    {0x0149, "\xCA\xBC\x4E"}, // U+02BC U+004E
    {0x01F0, "\x4A\xCC\x8C"}, // U+004A U+030C
    {0x0390, "\xCE\x99\xCC\x88\xCC\x81"}, // U+0399 U+0308 U+0301
    {0x03B0, "\xCE\xA5\xCC\x88\xCC\x81"}, // U+03A5 U+0308 U+0301
    {0x0587, "\xD4\xB5\xD5\x92"}, // U+0535 U+0552
    {0x1E96, "\x48\xCC\xB1"}, // U+0048 U+0331
    {0x1E97, "\x54\xCC\x88"}, // U+0054 U+0308
    {0x1E98, "\x57\xCC\x8A"}, // U+0057 U+030A
    {0x1E99, "\x59\xCC\x8A"}, // U+0059 U+030A
    {0x1E9A, "\x41\xCA\xBE"}, // U+0041 U+02BE
    {0x1F50, "\xCE\xA5\xCC\x93"}, // U+03A5 U+0313
    {0x1F52, "\xCE\xA5\xCC\x93\xCC\x80"}, // U+03A5 U+0313 U+0300
    {0x1F54, "\xCE\xA5\xCC\x93\xCC\x81"}, // U+03A5 U+0313 U+0301
    {0x1F56, "\xCE\xA5\xCC\x93\xCD\x82"}, // U+03A5 U+0313 U+0342
    {0x1F80, "\xE1\xBC\x88\xCE\x99"}, // U+1F08 U+0399
    {0x1F81, "\xE1\xBC\x89\xCE\x99"}, // U+1F09 U+0399
    {0x1F82, "\xE1\xBC\x8A\xCE\x99"}, // U+1F0A U+0399
    {0x1F83, "\xE1\xBC\x8B\xCE\x99"}, // U+1F0B U+0399
    {0x1F84, "\xE1\xBC\x8C\xCE\x99"}, // U+1F0C U+0399
    {0x1F85, "\xE1\xBC\x8D\xCE\x99"}, // U+1F0D U+0399
    {0x1F86, "\xE1\xBC\x8E\xCE\x99"}, // U+1F0E U+0399
    {0x1F87, "\xE1\xBC\x8F\xCE\x99"}, // U+1F0F U+0399
    {0x1F88, "\xE1\xBC\x88\xCE\x99"}, // U+1F08 U+0399
    {0x1F89, "\xE1\xBC\x89\xCE\x99"}, // U+1F09 U+0399
    {0x1F8A, "\xE1\xBC\x8A\xCE\x99"}, // U+1F0A U+0399
    {0x1F8B, "\xE1\xBC\x8B\xCE\x99"}, // U+1F0B U+0399
    {0x1F8C, "\xE1\xBC\x8C\xCE\x99"}, // U+1F0C U+0399
    {0x1F8D, "\xE1\xBC\x8D\xCE\x99"}, // U+1F0D U+0399
    {0x1F8E, "\xE1\xBC\x8E\xCE\x99"}, // U+1F0E U+0399
    {0x1F8F, "\xE1\xBC\x8F\xCE\x99"}, // U+1F0F U+0399
    {0x1F90, "\xE1\xBC\xA8\xCE\x99"}, // U+1F28 U+0399
    {0x1F91, "\xE1\xBC\xA9\xCE\x99"}, // U+1F29 U+0399
    {0x1F92, "\xE1\xBC\xAA\xCE\x99"}, // U+1F2A U+0399
    {0x1F93, "\xE1\xBC\xAB\xCE\x99"}, // U+1F2B U+0399
    {0x1F94, "\xE1\xBC\xAC\xCE\x99"}, // U+1F2C U+0399
    {0x1F95, "\xE1\xBC\xAD\xCE\x99"}, // U+1F2D U+0399
    {0x1F96, "\xE1\xBC\xAE\xCE\x99"}, // U+1F2E U+0399
    {0x1F97, "\xE1\xBC\xAF\xCE\x99"}, // U+1F2F U+0399
    {0x1F98, "\xE1\xBC\xA8\xCE\x99"}, // U+1F28 U+0399
    {0x1F99, "\xE1\xBC\xA9\xCE\x99"}, // U+1F29 U+0399
    {0x1F9A, "\xE1\xBC\xAA\xCE\x99"}, // U+1F2A U+0399
    {0x1F9B, "\xE1\xBC\xAB\xCE\x99"}, // U+1F2B U+0399
    {0x1F9C, "\xE1\xBC\xAC\xCE\x99"}, // U+1F2C U+0399
    {0x1F9D, "\xE1\xBC\xAD\xCE\x99"}, // U+1F2D U+0399
    {0x1F9E, "\xE1\xBC\xAE\xCE\x99"}, // U+1F2E U+0399
    {0x1F9F, "\xE1\xBC\xAF\xCE\x99"}, // U+1F2F U+0399
    {0x1FA0, "\xE1\xBD\xA8\xCE\x99"}, // U+1F68 U+0399
    {0x1FA1, "\xE1\xBD\xA9\xCE\x99"}, // U+1F69 U+0399
    {0x1FA2, "\xE1\xBD\xAA\xCE\x99"}, // U+1F6A U+0399
    {0x1FA3, "\xE1\xBD\xAB\xCE\x99"}, // U+1F6B U+0399
    {0x1FA4, "\xE1\xBD\xAC\xCE\x99"}, // U+1F6C U+0399
    {0x1FA5, "\xE1\xBD\xAD\xCE\x99"}, // U+1F6D U+0399
    {0x1FA6, "\xE1\xBD\xAE\xCE\x99"}, // U+1F6E U+0399
    {0x1FA7, "\xE1\xBD\xAF\xCE\x99"}, // U+1F6F U+0399
    {0x1FA8, "\xE1\xBD\xA8\xCE\x99"}, // U+1F68 U+0399
    {0x1FA9, "\xE1\xBD\xA9\xCE\x99"}, // U+1F69 U+0399
    {0x1FAA, "\xE1\xBD\xAA\xCE\x99"}, // U+1F6A U+0399
    {0x1FAB, "\xE1\xBD\xAB\xCE\x99"}, // U+1F6B U+0399
    {0x1FAC, "\xE1\xBD\xAC\xCE\x99"}, // U+1F6C U+0399
    {0x1FAD, "\xE1\xBD\xAD\xCE\x99"}, // U+1F6D U+0399
    {0x1FAE, "\xE1\xBD\xAE\xCE\x99"}, // U+1F6E U+0399
    {0x1FAF, "\xE1\xBD\xAF\xCE\x99"}, // U+1F6F U+0399
    {0x1FB2, "\xE1\xBE\xBA\xCE\x99"}, // U+1FBA U+0399
    {0x1FB3, "\xCE\x91\xCE\x99"}, // U+0391 U+0399
    {0x1FB4, "\xCE\x86\xCE\x99"}, // U+0386 U+0399
    {0x1FB6, "\xCE\x91\xCD\x82"}, // U+0391 U+0342
    {0x1FB7, "\xCE\x91\xCD\x82\xCE\x99"}, // U+0391 U+0342 U+0399
    {0x1FBC, "\xCE\x91\xCE\x99"}, // U+0391 U+0399
    {0x1FC2, "\xE1\xBF\x8A\xCE\x99"}, // U+1FCA U+0399
    {0x1FC3, "\xCE\x97\xCE\x99"}, // U+0397 U+0399
    {0x1FC4, "\xCE\x89\xCE\x99"}, // U+0389 U+0399
    {0x1FC6, "\xCE\x97\xCD\x82"}, // U+0397 U+0342
    {0x1FC7, "\xCE\x97\xCD\x82\xCE\x99"}, // U+0397 U+0342 U+0399
    {0x1FCC, "\xCE\x97\xCE\x99"}, // U+0397 U+0399
    {0x1FD2, "\xCE\x99\xCC\x88\xCC\x80"}, // U+0399 U+0308 U+0300
    {0x1FD3, "\xCE\x99\xCC\x88\xCC\x81"}, // U+0399 U+0308 U+0301
    {0x1FD6, "\xCE\x99\xCD\x82"}, // U+0399 U+0342
    {0x1FD7, "\xCE\x99\xCC\x88\xCD\x82"}, // U+0399 U+0308 U+0342
    {0x1FE2, "\xCE\xA5\xCC\x88\xCC\x80"}, // U+03A5 U+0308 U+0300
    {0x1FE3, "\xCE\xA5\xCC\x88\xCC\x81"}, // U+03A5 U+0308 U+0301
    {0x1FE4, "\xCE\xA1\xCC\x93"}, // U+03A1 U+0313
    {0x1FE6, "\xCE\xA5\xCD\x82"}, // U+03A5 U+0342
    {0x1FE7, "\xCE\xA5\xCC\x88\xCD\x82"}, // U+03A5 U+0308 U+0342
    {0x1FF2, "\xE1\xBF\xBA\xCE\x99"}, // U+1FFA U+0399
    {0x1FF3, "\xCE\xA9\xCE\x99"}, // U+03A9 U+0399
    {0x1FF4, "\xCE\x8F\xCE\x99"}, // U+038F U+0399
    {0x1FF6, "\xCE\xA9\xCD\x82"}, // U+03A9 U+0342
    {0x1FF7, "\xCE\xA9\xCD\x82\xCE\x99"}, // U+03A9 U+0342 U+0399
    {0x1FFC, "\xCE\xA9\xCE\x99"}, // U+03A9 U+0399
    {0xFB00, "\x46\x46"}, // U+0046 U+0046
    {0xFB01, "\x46\x49"}, // U+0046 U+0049
    {0xFB02, "\x46\x4C"}, // U+0046 U+004C
    {0xFB03, "\x46\x46\x49"}, // U+0046 U+0046 U+0049
    {0xFB04, "\x46\x46\x4C"}, // U+0046 U+0046 U+004C
    {0xFB05, "\x53\x54"}, // U+0053 U+0054
    {0xFB06, "\x53\x54"}, // U+0053 U+0054
    {0xFB13, "\xD5\x84\xD5\x86"}, // U+0544 U+0546
    {0xFB14, "\xD5\x84\xD4\xB5"}, // U+0544 U+0535
    {0xFB15, "\xD5\x84\xD4\xBB"}, // U+0544 U+053B
    {0xFB16, "\xD5\x8E\xD5\x86"}, // U+054E U+0546
    {0xFB17, "\xD5\x84\xD4\xBD"}, // U+0544 U+053D
}};

/// Returns the UTF-8 encoded uppercase sequence of 'codePoint' if its full
/// uppercase mapping has more than one code point, std::nullopt otherwise.
FOLLY_ALWAYS_INLINE std::optional<std::string_view> specialUpperCase(
    utf8proc_int32_t codePoint) {
  if (codePoint < kSpecialUpperCasing.front().codePoint ||
      codePoint > kSpecialUpperCasing.back().codePoint) {
    return std::nullopt;
  }
  auto it = std::lower_bound(
      kSpecialUpperCasing.begin(),
      kSpecialUpperCasing.end(),
      codePoint,
      [](const SpecialUpperCasingEntry& entry, utf8proc_int32_t value) {
        return entry.codePoint < value;
      });
  if (it == kSpecialUpperCasing.end() || it->codePoint != codePoint) {
    return std::nullopt;
  }
  return it->upper;
}

} // namespace facebook::velox::functions
