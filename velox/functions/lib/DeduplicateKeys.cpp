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
 #include "velox/functions/lib/DeduplicateKeys.h"
#include <buffer/Buffer.h>
#include <vector/BaseVector.h>
#include <vector/DecodedVector.h>
#include <vector/TypeAliases.h>

 namespace facebook::velox::functions {
 
std::vector<vector_size_t>  deduplicateKeys(
    std::vector<vector_size_t> sortedIndices,
    const VectorPtr& keysElements) {
    std::vector<vector_size_t> dedupIndices;
    for (auto i = 0; i < sortedIndices.size()-1; ++i) {
      if(!keysElements->equalValueAt(
        keysElements.get(), sortedIndices[i], sortedIndices[i + 1])) {
        dedupIndices.push_back(sortedIndices[i]);
      }
    }
    dedupIndices.push_back(sortedIndices[sortedIndices.size() - 1]);

    return dedupIndices;
}

std::vector<vector_size_t>  deduplicateKeys(
    vector_size_t numKeys,
    vector_size_t keysOffset,
    const VectorPtr& keysElements) {
      // Sort indices of keys so that values at the indices are in ascending
      // order. Then compare adjacent values through these indices to check for
      // duplicate keys.
      std::vector<vector_size_t> sortedIndices(numKeys);
      std::iota(sortedIndices.begin(), sortedIndices.end(), keysOffset);
      keysElements->sortIndices(sortedIndices, CompareFlags());
      return deduplicateKeys(sortedIndices, keysElements);
    }

 } // namespace facebook::velox::functions
 