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

#include <gflags/gflags.h>
#include <memory>
#include <mutex>

#include <velox/tpch/gen/dbgen/include/dbgen/dss.h>
#include <velox/tpch/gen/dbgen/include/dbgen/dsstypes.h>

DECLARE_int32(velox_tpch_text_pool_size_mb);

namespace facebook::velox::tpch {

/// This class exposes a thread-safe and reproducible iterator over TPC-H
/// synthetically generated data, backed by DBGEN.
class DBGenIterator {
 public:
  explicit DBGenIterator(double scaleFactor);

  // Before generating records using the gen*() functions below, call the
  // appropriate init*() function to correctly initialize the seed given the
  // offset to be generated.
  void initNation(size_t offset);
  void initRegion(size_t offset);
  void initOrder(size_t offset);
  void initSupplier(size_t offset);
  void initPart(size_t offset);
  void initCustomer(size_t offset);

  // Generate different types of records.
  void genNation(size_t index, dbgen::code_t& code);
  void genRegion(size_t index, dbgen::code_t& code);
  void genOrder(size_t index, dbgen::order_t& order);
  void genSupplier(size_t index, dbgen::supplier_t& supplier);
  void genPart(size_t index, dbgen::part_t& part);
  void genCustomer(size_t index, dbgen::customer_t& customer);

  dbgen::DBGenContext dbgenCtx_;
};

} // namespace facebook::velox::tpch
