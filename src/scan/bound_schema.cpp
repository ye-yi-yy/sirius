/*
 * Copyright 2026, Sirius Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * See the LICENSE file at the repo root for the full text.
 */

#include "scan/bound_schema.hpp"

#include <duckdb/common/exception.hpp>
#include <duckdb/common/extra_type_info.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>

#include <cstdint>
#include <utility>

namespace sirius::scan {
namespace {

void field(std::string& out, const std::string& tag, const std::string& value)
{
  out += std::to_string(tag.size()) + ":" + tag + std::to_string(value.size()) + ":" + value;
}

void number(std::string& out, const std::string& tag, std::uint64_t value)
{
  field(out, tag, std::to_string(value));
}

bool encode_type(std::string& out, const duckdb::LogicalType& type)
{
  using id         = duckdb::LogicalTypeId;
  using info       = duckdb::ExtraTypeInfoType;
  const auto extra = type.AuxInfo();
  // Extension modifiers/properties require a separately qualified encoder. DuckDB's Equals
  // treats missing properties as wildcards, which cannot establish binding identity.
  if (extra && extra->extension_info) { return false; }

  number(out, "logical_type", static_cast<std::uint8_t>(type.id()));
  number(out, "physical_type", static_cast<std::uint8_t>(type.InternalType()));
  field(out, "alias", type.GetAlias());

  const auto has_info   = [&](info expected) { return extra && extra->type == expected; };
  const auto plain_info = [&] {
    return !extra || extra->type == info::INVALID_TYPE_INFO ||
           extra->type == info::GENERIC_TYPE_INFO;
  };

  switch (type.id()) {
    case id::DECIMAL:
      if (!has_info(info::DECIMAL_TYPE_INFO)) { return false; }
      number(out, "width", duckdb::DecimalType::GetWidth(type));
      number(out, "scale", duckdb::DecimalType::GetScale(type));
      return true;
    case id::VARCHAR:
    case id::CHAR:
      if (!plain_info() && !has_info(info::STRING_TYPE_INFO)) { return false; }
      // LogicalType::operator== deliberately ignores collation. Read only a known payload.
      field(
        out,
        "collation",
        has_info(info::STRING_TYPE_INFO) ? extra->Cast<duckdb::StringTypeInfo>().collation : "");

      return true;
    case id::LIST:
    case id::MAP:
      if (!has_info(info::LIST_TYPE_INFO)) { return false; }
      return encode_type(out, duckdb::ListType::GetChildType(type));
    case id::ARRAY:
      if (!has_info(info::ARRAY_TYPE_INFO)) { return false; }
      number(out, "array_size", duckdb::ArrayType::GetSize(type));
      return encode_type(out, duckdb::ArrayType::GetChildType(type));
    case id::STRUCT:
    case id::UNION: {
      if (!has_info(info::STRUCT_TYPE_INFO)) { return false; }
      // UNION's complete stored representation also includes its tag column.
      const auto& children = duckdb::StructType::GetChildTypes(type);
      number(out, "children", children.size());
      for (const auto& child : children) {
        field(out, "child_name", child.first);
        if (!encode_type(out, child.second)) { return false; }
      }
      return true;
    }
    case id::ENUM:
      if (!has_info(info::ENUM_TYPE_INFO)) { return false; }
      number(out, "enum_values", duckdb::EnumType::GetSize(type));
      for (duckdb::idx_t i = 0; i < duckdb::EnumType::GetSize(type); ++i) {
        field(out, "enum_value", duckdb::EnumType::GetString(type, i).GetString());
      }
      return true;
    case id::SQLNULL:
    case id::BOOLEAN:
    case id::TINYINT:
    case id::SMALLINT:
    case id::INTEGER:
    case id::BIGINT:
    case id::HUGEINT:
    case id::UTINYINT:
    case id::USMALLINT:
    case id::UINTEGER:
    case id::UBIGINT:
    case id::UHUGEINT:
    case id::FLOAT:
    case id::DOUBLE:
    case id::DATE:
    case id::TIME:
    case id::TIME_NS:
    case id::TIME_TZ:
    case id::TIMESTAMP:
    case id::TIMESTAMP_SEC:
    case id::TIMESTAMP_MS:
    case id::TIMESTAMP_NS:
    case id::TIMESTAMP_TZ:
    case id::INTERVAL:
    case id::BLOB:
    case id::BIT:
    case id::BIGNUM:
    case id::UUID: return plain_info();
    default: return false;
  }
}

}  // namespace

bound_schema::bound_schema(std::vector<std::string> names,
                           const duckdb::vector<duckdb::LogicalType>& types)
  : _names(std::move(names))
{
  if (_names.size() != types.size()) {
    throw duckdb::InvalidInputException(
      "bound_schema: column names and types must have equal size");
  }

  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer(stream);
  serializer.Begin();
  serializer.WriteProperty(100, "types", types);
  serializer.End();
  _serialized_types.assign(stream.GetData(), stream.GetData() + stream.GetPosition());

  std::string key;
  field(key, "schema_version", "1");
  number(key, "columns", types.size());
  for (std::size_t i = 0; i < types.size(); ++i) {
    field(key, "column_name", _names[i]);
    if (!encode_type(key, types[i])) { return; }
  }
  _canonical_identity = std::move(key);
}

duckdb::vector<duckdb::LogicalType> bound_schema::types() const
{
  duckdb::MemoryStream stream;
  stream.WriteData(_serialized_types.data(), _serialized_types.size());
  stream.Rewind();
  duckdb::BinaryDeserializer deserializer(stream);
  deserializer.Begin();
  auto result = deserializer.ReadProperty<duckdb::vector<duckdb::LogicalType>>(100, "types");
  deserializer.End();
  return result;
}

bool bound_schema::equals(const bound_schema& other) const noexcept
{
  return this == &other || (_canonical_identity && other._canonical_identity &&
                            *_canonical_identity == *other._canonical_identity);
}

}  // namespace sirius::scan
