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

#include "velox/expression/SpecialForm.h"

namespace facebook::velox::exec {

/// Expressions that are higher order functions that take arguments which are
/// functions themselves. These are used to implement computations over arrays
/// and maps and are of the form LAMBDA_EXPR(ARRAY/MAP, INNER_EXPR).
/// The inner expression is applied to elements of the array/map and can contain
/// references to other columns in the input row vector which are required to
/// evaluate the function. These references are called captures.
/// eg. filter(array[1, 2, 3, 4], x -> x % 2 = 0)
class LambdaExpr : public SpecialForm {
 public:
  LambdaExpr(
      TypePtr type,
      RowTypePtr&& signature,
      std::vector<std::shared_ptr<FieldReference>>&& capture,
      std::shared_ptr<Expr>&& body,
      bool trackCpuUsage);

  bool isConstantExpr() const override {
    return false;
  }

  std::string toString(bool recursive = true) const override;

  std::string toSql(
      std::vector<VectorPtr>* complexConstants = nullptr) const override;

  void evalSpecialForm(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

  const ExprPtr& body() const {
    return body_;
  }

 protected:
  void computeDistinctFields() override;

 private:
  /// Used to initialize captureChannels_ and typeWithCapture_ on first use.
  void makeTypeWithCapture(EvalCtx& context);

  void computePropagatesNulls() override {
    // A null capture does not result in a null function.
    propagatesNulls_ = false;
  }

  void extractSubfieldsImpl(
      folly::F14FastMap<std::string, int32_t>* shadowedNames,
      std::vector<common::Subfield>* subfields) const override;

  RowTypePtr signature_;

  /// The inner expression that will be applied to the elements of the input
  /// array/map.
  ExprPtr body_;

  // List of Shared Exprs that are decendants of 'body_' for which reset() needs
  // to be called before calling `body_->eval()`.This is because every
  // invocation of `body_->eval()` should treat its inputs like a fresh batch
  // similar to how we operate in `ExprSet::eval()`.
  std::vector<ExprPtr> sharedExprsToReset_;

  /// List of field references to columns in the input row vector.
  std::vector<std::shared_ptr<FieldReference>> capture_;

  /// These contain column indices of the captured columns with respect to the
  /// input row vector. Stored in the same order as in capture_. Filled on first
  /// use.
  std::vector<column_index_t> captureChannels_;

  /// A row type representing column types in the order starting with inner
  /// types of the array/map it operates on followed by types of the columns
  /// that it captures (in the same order as that in capture_). This is used to
  /// create an input row vector which is fed to the inner expression. Filled on
  /// first use.
  RowTypePtr typeWithCapture_;
};
} // namespace facebook::velox::exec
