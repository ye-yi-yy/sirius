/*
 * Copyright 2026, Sirius Contributors.
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

#include "op/scan/table_scan/bound_read_view.hpp"

#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>

#include <algorithm>
#include <numeric>
#include <stdexcept>
#include <string_view>

namespace sirius::op::scan {
namespace {
void append(std::string& out, char tag, std::string_view value)
{
  out += tag;
  out += std::to_string(value.size());
  out += ':';
  out.append(value);
}
void number(std::string& out, uint64_t value) { append(out, 'u', std::to_string(value)); }
}  // namespace

std::shared_ptr<bound_read_identity const> make_bound_read_identity(
  bound_read_identity identity, std::span<std::string const> paths)
{
  if (identity.bound_types.size() != identity.bound_names.size()) {
    throw std::invalid_argument("Bound read schema has different type and name counts");
  }
  std::string tail;
  number(tail, identity.bound_types.size());
  for (size_t i = 0; i < identity.bound_types.size(); ++i) {
    duckdb::MemoryStream buffer;
    duckdb::BinarySerializer::Serialize(identity.bound_types[i], buffer);
    append(tail,
           't',
           std::string_view(reinterpret_cast<char const*>(buffer.GetData()), buffer.GetPosition()));
    append(tail, 's', identity.bound_names[i]);
  }
  append(tail, 'o', identity.selector);
  auto& text = identity.fingerprint.canonical;
  text.clear();
  append(text, 'v', "sirius.read-view.1");
  append(text, 's', identity.source.function_name);
  number(text, static_cast<uint8_t>(identity.source.kind));
  append(text, 's', identity.source.registry_profile);
  number(text, identity.data_view.index());
  if (auto const* native = std::get_if<native_table_identity>(&identity.data_view)) {
    if (!paths.empty()) throw std::invalid_argument("Native read view has file inventory");
    append(text, 's', native->catalog_name);
    number(text, native->catalog_oid);
    append(text, 's', native->schema_name);
    append(text, 's', native->table_name);
    number(text, native->table_oid);
    append(text, 's', native->db_path);
  } else if (auto const* files = std::get_if<file_inventory>(&identity.data_view)) {
    if (files->count != paths.size()) throw std::invalid_argument("Read-view file count mismatch");
    number(text, files->count);
    std::vector<size_t> order(paths.size());
    std::iota(order.begin(), order.end(), 0);
    std::sort(order.begin(), order.end(), [&](size_t a, size_t b) { return paths[a] < paths[b]; });
    size_t path_bytes = 0;
    for (auto const& path : paths)
      path_bytes += path.size() + std::to_string(path.size()).size() + 2;
    text.reserve(text.size() + path_bytes + tail.size());
    for (auto index : order)
      append(text, 'p', paths[index]);
  } else {
    if (!paths.empty()) throw std::invalid_argument("Stream read view has file inventory");
    number(text, std::get<stream_identity>(identity.data_view).stream_id);
  }
  text += tail;
  uint64_t hash = 14695981039346656037ULL;
  for (unsigned char byte : text) {
    hash ^= byte;
    hash *= 1099511628211ULL;
  }
  identity.fingerprint.hash = hash;
  return std::make_shared<bound_read_identity const>(std::move(identity));
}

std::string canonical_value_text(duckdb::Value const& value)
{
  duckdb::MemoryStream buffer;
  duckdb::BinarySerializer::Serialize(value, buffer);
  std::string text;
  append(text,
         'v',
         std::string_view(reinterpret_cast<char const*>(buffer.GetData()), buffer.GetPosition()));
  return text;
}

std::string canonical_read_view_text(bound_read_view const& view)
{
  if (!view.identity) throw std::invalid_argument("Bound read view has no captured identity");
  return view.identity->fingerprint.canonical;
}
}  // namespace sirius::op::scan
