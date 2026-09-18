/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#pragma once

#include <duckdb/common/types.hpp>
#include <duckdb/common/vector.hpp>

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace sirius::scan {

/// An owned snapshot of the full bind-time schema, before GPU type conversion or projection.
/// DuckDB's type serialization owns metadata only; equality uses an explicit canonical encoding.
/// Unsupported semantic metadata is retained, but has no usable canonical identity.
class bound_schema {
 public:
  bound_schema(std::vector<std::string> names, const duckdb::vector<duckdb::LogicalType>& types);

  bound_schema(const bound_schema&)            = delete;
  bound_schema& operator=(const bound_schema&) = delete;
  bound_schema(bound_schema&&)                 = delete;
  bound_schema& operator=(bound_schema&&)      = delete;

  [[nodiscard]] const std::vector<std::string>& names() const noexcept { return _names; }

  /// Return independently owned types. A LogicalType copy alone shares mutable ExtraTypeInfo.
  [[nodiscard]] duckdb::vector<duckdb::LogicalType> types() const;

  /// Versioned, typed, byte-length-prefixed identity; absent means unsupported, never empty.
  [[nodiscard]] const std::optional<std::string>& canonical_identity() const noexcept
  {
    return _canonical_identity;
  }

  /// Copies sharing this snapshot compare equal even when no canonical encoder is available.
  /// Independently captured unsupported schemas cannot establish equality.
  [[nodiscard]] bool equals(const bound_schema& other) const noexcept;

 private:
  std::vector<std::string> _names;
  std::vector<duckdb::data_t> _serialized_types;
  std::optional<std::string> _canonical_identity;
};

using bound_schema_ptr = std::shared_ptr<const bound_schema>;

}  // namespace sirius::scan
