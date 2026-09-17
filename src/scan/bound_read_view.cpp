/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#include "scan/bound_read_view.hpp"

#include "scan/source_registry.hpp"

#include <duckdb/common/exception.hpp>
#include <duckdb/common/string_util.hpp>

#include <cstring>
#include <map>
#include <utility>

namespace sirius::scan {
namespace {

void field(std::string& out, const std::string& tag, const std::string& value)
{
  out += std::to_string(tag.size()) + ":" + tag + std::to_string(value.size()) + ":" + value;
}

template <class T>
void number(std::string& out, const std::string& tag, T value)
{
  field(out, tag, std::to_string(value));
}

bool type(std::string& out, const duckdb::LogicalType& value)
{
  bound_schema schema({""}, {value});
  if (!schema.canonical_identity()) { return false; }
  field(out, "type", *schema.canonical_identity());
  return true;
}

bool value(std::string& out, const duckdb::Value& input)
{
  using id = duckdb::LogicalTypeId;
  if (input.type().id() == id::SQLNULL) {
    if (!input.IsNull() || input.type().AuxInfo()) { return false; }
    field(out, "sql_null", "");
    return true;
  }
  if (!type(out, input.type())) { return false; }
  number(out, "null", input.IsNull());
  if (input.IsNull()) { return true; }
  switch (input.type().id()) {
    case id::BOOLEAN: number(out, "bool", input.GetValueUnsafe<bool>()); break;
    case id::TINYINT: number(out, "signed", input.GetValueUnsafe<int8_t>()); break;
    case id::SMALLINT: number(out, "signed", input.GetValueUnsafe<int16_t>()); break;
    case id::INTEGER: number(out, "signed", input.GetValueUnsafe<int32_t>()); break;
    case id::BIGINT: number(out, "signed", input.GetValueUnsafe<int64_t>()); break;
    case id::UTINYINT: number(out, "unsigned", input.GetValueUnsafe<uint8_t>()); break;
    case id::USMALLINT: number(out, "unsigned", input.GetValueUnsafe<uint16_t>()); break;
    case id::UINTEGER: number(out, "unsigned", input.GetValueUnsafe<uint32_t>()); break;
    case id::UBIGINT: number(out, "unsigned", input.GetValueUnsafe<uint64_t>()); break;
    case id::FLOAT: {
      auto scalar = input.GetValueUnsafe<float>();
      std::uint32_t bits;
      std::memcpy(&bits, &scalar, sizeof(bits));
      number(out, "float_bits", bits);
      break;
    }
    case id::DOUBLE: {
      auto scalar = input.GetValueUnsafe<double>();
      std::uint64_t bits;
      std::memcpy(&bits, &scalar, sizeof(bits));
      number(out, "double_bits", bits);
      break;
    }
    case id::VARCHAR:
    case id::BLOB: field(out, "bytes", duckdb::StringValue::Get(input)); break;
    case id::DATE: number(out, "days", duckdb::DateValue::Get(input).days); break;
    case id::TIMESTAMP: number(out, "ticks", duckdb::TimestampValue::Get(input).value); break;
    case id::TIMESTAMP_TZ: number(out, "ticks", duckdb::TimestampTZValue::Get(input).value); break;
    case id::TIMESTAMP_SEC: number(out, "ticks", duckdb::TimestampSValue::Get(input).value); break;
    case id::TIMESTAMP_MS: number(out, "ticks", duckdb::TimestampMSValue::Get(input).value); break;
    case id::TIMESTAMP_NS: number(out, "ticks", duckdb::TimestampNSValue::Get(input).value); break;
    case id::LIST:
    case id::MAP: {
      const auto& children = duckdb::ListValue::GetChildren(input);
      number(out, "children", children.size());
      for (const auto& child : children) {
        if (!value(out, child)) { return false; }
      }
      break;
    }
    case id::STRUCT: {
      const auto& children = duckdb::StructValue::GetChildren(input);
      number(out, "children", children.size());
      for (const auto& child : children) {
        if (!value(out, child)) { return false; }
      }
      break;
    }
    default: return false;
  }
  return true;
}

}  // namespace

read_view_capture::read_view_capture(std::uint64_t instance,
                                     std::uint64_t generation,
                                     capture_origin origin,
                                     capture_status status,
                                     std::shared_ptr<const bound_read_view> view)
  : _instance(instance),
    _generation(generation),
    _origin(origin),
    _status(status),
    _view(std::move(view))
{
  if (!instance || !generation) {
    throw duckdb::InvalidInputException(
      "read view capture requires an instance and planning generation");
  }
  if ((_status == capture_status::complete) != static_cast<bool>(_view)) {
    throw duckdb::InternalException("only a complete capture can own a bound read view");
  }
}

read_view_capture capture_read_view(source_registry& registry,
                                    std::uint64_t generation,
                                    capture_origin origin,
                                    const duckdb::TableFunction& function,
                                    const duckdb::FunctionData* bind_data,
                                    const duckdb::vector<std::string>& names,
                                    const duckdb::vector<duckdb::LogicalType>& types)
{
  const auto* adapter = registry.lookup(function, bind_data);
  if (!adapter) { return {registry.instance_id(), generation, origin}; }
  return adapter->try_capture_bound_view({registry.instance_id(),
                                          generation,
                                          origin,
                                          {registry.database(), function, bind_data},
                                          names,
                                          types});
}

std::optional<std::string> capture_selector(const duckdb::vector<duckdb::Value>& positional,
                                            const duckdb::named_parameter_map_t& named)
{
  std::string result;
  number(result, "selector_version", 1);
  number(result, "positional", positional.size());
  for (const auto& parameter : positional) {
    if (!value(result, parameter)) { return std::nullopt; }
  }
  std::map<std::string, duckdb::Value> ordered;
  for (const auto& parameter : named) {
    ordered.emplace(duckdb::StringUtil::Lower(parameter.first), parameter.second);
  }
  number(result, "named", ordered.size());
  for (const auto& parameter : ordered) {
    field(result, "name", parameter.first);
    if (!value(result, parameter.second)) { return std::nullopt; }
  }
  return result;
}

}  // namespace sirius::scan
