/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#pragma once

#include "scan/bound_schema.hpp"
#include "scan/source_profile.hpp"

#include <duckdb/function/table_function.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace sirius::scan {

class source_registry;

enum class capture_status { complete, unavailable, unsupported, unverified };
enum class capture_origin { logical_original, physical_original, candidate };

/// Reserved for provider inventory bridges. Legacy enumeration never supplies this evidence.
struct file_inventory_entry {
  std::string path;
  std::vector<std::uint8_t> serialized_options;
};

/// Binding correspondence only. File evidence never enters canonical_identity.
struct bound_read_view {
  const source_kind kind;
  const std::string profile;
  const bound_schema_ptr schema;
  const std::string canonical_identity;
  const std::vector<file_inventory_entry> files;
  const std::uint64_t identity_hash = std::hash<std::string>{}(canonical_identity);
};

/// Records capture status and provenance without binding or I/O. Standard Parquet and Iceberg
/// remain unverified until supported provider bridges expose original inventory and options.
class read_view_capture {
 public:
  read_view_capture(std::uint64_t instance,
                    std::uint64_t generation,
                    capture_origin origin,
                    capture_status status                       = capture_status::unverified,
                    std::shared_ptr<const bound_read_view> view = nullptr);

  [[nodiscard]] capture_status status() const noexcept { return _status; }
  [[nodiscard]] const std::shared_ptr<const bound_read_view>& view() const noexcept
  {
    return _view;
  }
  [[nodiscard]] std::uint64_t instance() const noexcept { return _instance; }
  [[nodiscard]] std::uint64_t generation() const noexcept { return _generation; }
  [[nodiscard]] capture_origin origin() const noexcept { return _origin; }

 private:
  const std::uint64_t _instance;
  const std::uint64_t _generation;
  const capture_origin _origin;
  capture_status _status = capture_status::unverified;
  std::shared_ptr<const bound_read_view> _view;
};

/// Does not call a binder, serializer callback, filesystem or provider metadata query.
[[nodiscard]] read_view_capture capture_read_view(source_registry& registry,
                                                  std::uint64_t generation,
                                                  capture_origin origin,
                                                  const duckdb::TableFunction& function,
                                                  const duckdb::FunctionData* bind_data,
                                                  const duckdb::vector<std::string>& names,
                                                  const duckdb::vector<duckdb::LogicalType>& types);

/// Typed evaluated selector encoding. No evaluation and no display-string equality.
[[nodiscard]] std::optional<std::string> capture_selector(
  const duckdb::vector<duckdb::Value>& positional, const duckdb::named_parameter_map_t& named);

}  // namespace sirius::scan
