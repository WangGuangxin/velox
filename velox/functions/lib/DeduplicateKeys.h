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

 #include "velox/expression/EvalCtx.h"
 #include "velox/vector/ComplexVector.h"
 
 namespace facebook::velox::functions {
 
 std::vector<vector_size_t>  deduplicateKeys(
    std::vector<vector_size_t> sortedIndices,
    const VectorPtr& keysElements);
 
 /// Check map keys for duplicates. Mark rows with duplicate keys as 'failed' in
 /// the 'context'. Rows with null keys should not be passed to this function and
 /// should have been removed from rows.
 std::vector<vector_size_t>  deduplicateKeys(
    vector_size_t numKeys,
    vector_size_t keysOffset,
    const VectorPtr& keysElements);
 
 } // namespace facebook::velox::functions
 