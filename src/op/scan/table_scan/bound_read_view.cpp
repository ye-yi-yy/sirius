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

#include "exec/stream_plan_bindings.hpp"
#include "helper/type_conversions.hpp"
#include "op/sirius_physical_table_scan.hpp"
#include "planner/scan_source_registry.hpp"
#include "sirius_registration.hpp"

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/common/multi_file/multi_file_function.hpp>
#include <duckdb/common/multi_file/multi_file_list.hpp>
#include <duckdb/common/multi_file/multi_file_reader.hpp>
#include <duckdb/common/multi_file/multi_file_states.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/execution/operator/scan/physical_table_scan.hpp>
#include <duckdb/function/table/table_scan.hpp>
#include <duckdb/planner/logical_operator.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <parquet_crypto.hpp>
#include <parquet_multi_file_info.hpp>
#include <parquet_reader.hpp>

#include <algorithm>
#include <iomanip>
#include <numeric>
#include <sstream>
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

void boolean(std::string& out, bool value) { append(out, 'b', value ? "1" : "0"); }

template <typename MAP, typename VALUE>
void append_sorted_map(std::string& out, MAP const& map, VALUE&& encode_value)
{
  std::vector<std::string> keys;
  keys.reserve(map.size());
  for (auto const& [key, unused] : map) {
    (void)unused;
    keys.push_back(key);
  }
  std::sort(keys.begin(), keys.end());
  number(out, keys.size());
  for (auto const& key : keys) {
    append(out, 'k', key);
    encode_value(out, map.at(key));
  }
}

void append_multi_file_options(std::string& out, duckdb::MultiFileOptions const& options)
{
  boolean(out, options.filename);
  boolean(out, options.hive_partitioning);
  boolean(out, options.auto_detect_hive_partitioning);
  boolean(out, options.union_by_name);
  boolean(out, options.hive_types_autocast);
  number(out, static_cast<uint8_t>(options.mapping));
  append(out, 's', options.filename_column);
  append_sorted_map(out, options.hive_types_schema, [](std::string& target, auto const& value) {
    append(target, 't', value.ToString());
  });
  append_sorted_map(out, options.custom_options, [](std::string& target, auto const& value) {
    append(target, 'v', canonical_value_text(value));
  });
}

// ParquetEncryptionConfig deliberately exposes its contents only through DuckDB's public
// serializer. Capture that small schema structurally so the unordered column-key map can be
// sorted before it becomes identity text.
class canonical_encryption_serializer final : public duckdb::Serializer {
 public:
  explicit canonical_encryption_serializer(std::string footer_key)
    : footer_key_(std::move(footer_key))
  {
  }

  std::string finish() const
  {
    std::string out;
    append(out, 's', footer_key_);
    append_sorted_map(out, column_keys_, [](std::string& target, auto const& value) {
      append(target, 's', value);
    });
    return out;
  }

 private:
  void OnPropertyBegin(duckdb::field_id_t, char const* tag) override { property_ = tag ? tag : ""; }
  void OnPropertyEnd() override { property_.clear(); }
  void OnOptionalPropertyBegin(duckdb::field_id_t, char const* tag, bool) override
  {
    property_ = tag ? tag : "";
  }
  void OnOptionalPropertyEnd(bool) override { property_.clear(); }
  void OnObjectBegin() override {}
  void OnObjectEnd() override {}
  void OnListBegin(duckdb::idx_t) override {}
  void OnListEnd() override {}
  void OnNullableBegin(bool) override {}
  void OnNullableEnd() override {}

  void capture(std::string value)
  {
    if (property_ == "footer_key") {
      footer_key_ = std::move(value);
    } else if (property_ == "key") {
      pending_key_ = std::move(value);
    } else if (property_ == "value") {
      column_keys_.insert_or_assign(pending_key_, std::move(value));
    }
  }
  [[noreturn]] static void unexpected()
  {
    throw std::runtime_error("Parquet encryption config serialized an unexpected value type");
  }
  void WriteNull() override { unexpected(); }
  void WriteValue(char) override { unexpected(); }
  void WriteValue(bool) override { unexpected(); }
  void WriteValue(uint8_t) override { unexpected(); }
  void WriteValue(int8_t) override { unexpected(); }
  void WriteValue(uint16_t) override { unexpected(); }
  void WriteValue(int16_t) override { unexpected(); }
  void WriteValue(uint32_t) override { unexpected(); }
  void WriteValue(int32_t) override { unexpected(); }
  void WriteValue(uint64_t) override { unexpected(); }
  void WriteValue(int64_t) override { unexpected(); }
  void WriteValue(duckdb::hugeint_t) override { unexpected(); }
  void WriteValue(duckdb::uhugeint_t) override { unexpected(); }
  void WriteValue(float) override { unexpected(); }
  void WriteValue(double) override { unexpected(); }
  void WriteValue(duckdb::string_t value) override { capture(value.GetString()); }
  void WriteValue(std::string const& value) override { capture(value); }
  void WriteValue(char const* value) override { capture(value ? value : ""); }
  void WriteDataPtr(duckdb::const_data_ptr_t, duckdb::idx_t) override { unexpected(); }

  std::string property_;
  std::string pending_key_;
  std::string footer_key_;
  std::unordered_map<std::string, std::string> column_keys_;
};

std::string canonical_encryption_config(duckdb::ParquetEncryptionConfig const& config)
{
  canonical_encryption_serializer serializer(config.GetFooterKey());
  config.Serialize(serializer);
  return serializer.finish();
}

duckdb::ParquetOptionsSerialization export_parquet_options(duckdb::TableFunction const& function,
                                                           duckdb::MultiFileBindData const& bind)
{
  if (!function.serialize || !bind.bind_data) {
    throw std::runtime_error("verified parquet source has no serializable format bind data");
  }
  duckdb::MultiFileBindData temporary;
  auto format_copy = bind.bind_data->Copy();
  auto* typed_copy = dynamic_cast<duckdb::TableFunctionData*>(format_copy.get());
  if (!typed_copy) {
    throw std::runtime_error("verified parquet source copied an invalid format bind payload");
  }
  (void)format_copy.release();
  temporary.bind_data = duckdb::unique_ptr<duckdb::TableFunctionData>(typed_copy);
  temporary.file_list =
    duckdb::make_shared_ptr<duckdb::SimpleMultiFileList>(duckdb::vector<duckdb::OpenFileInfo>{});
  temporary.file_options = bind.file_options;
  temporary.initial_reader.reset();
  temporary.union_readers.clear();

  duckdb::MemoryStream buffer;
  duckdb::BinarySerializer serializer(buffer);
  serializer.Begin();
  function.serialize(serializer, &temporary, function);
  serializer.End();
  buffer.Rewind();

  duckdb::BinaryDeserializer deserializer(buffer);
  deserializer.Begin();
  (void)deserializer.ReadProperty<duckdb::vector<std::string>>(100, "files");
  (void)deserializer.ReadProperty<duckdb::vector<duckdb::LogicalType>>(101, "types");
  (void)deserializer.ReadProperty<duckdb::vector<std::string>>(102, "names");
  auto options =
    deserializer.ReadProperty<duckdb::ParquetOptionsSerialization>(103, "parquet_options");
  deserializer.End();
  return options;
}

duckdb::TableFunction const& trusted_parquet_serializer()
{
  static duckdb::TableFunction const function = [] {
    auto functions = duckdb::ParquetScanFunction::GetFunctionSet().functions;
    if (functions.empty() || !functions.front().serialize) {
      throw std::runtime_error("trusted Parquet function has no serializer");
    }
    return functions.front();
  }();
  return function;
}

std::string parquet_selector(duckdb::TableFunction const& function,
                             duckdb::MultiFileBindData const& bind)
{
  auto const exported = export_parquet_options(function, bind);
  std::string out;
  append(out, 'v', "sirius.parquet-options.1");
  append_multi_file_options(out, exported.file_options);
  auto const& options = exported.parquet_options;
  boolean(out, options.binary_as_string);
  boolean(out, options.file_row_number);
  number(out, options.explicit_cardinality);
  boolean(out, options.can_have_nan);
  number(out, options.schema.size());
  for (auto const& column : options.schema) {
    number(out, static_cast<uint32_t>(column.field_id));
    append(out, 's', column.name);
    append(out, 't', column.type.ToString());
    append(out, 'v', canonical_value_text(column.default_value));
    append(out, 'v', canonical_value_text(column.identifier));
  }
  boolean(out, options.encryption_config != nullptr);
  if (options.encryption_config) {
    append(out, 'e', canonical_encryption_config(*options.encryption_config));
  }
  return out;
}

std::string selector_evidence(duckdb::vector<duckdb::Value> const& parameters,
                              duckdb::named_parameter_map_t const& named_parameters)
{
  std::string out;
  append(out, 'v', "sirius.selector-evidence.1");
  number(out, parameters.size());
  for (auto const& value : parameters)
    append(out, 'v', canonical_value_text(value));
  append_sorted_map(out, named_parameters, [](std::string& target, auto const& value) {
    append(target, 'v', canonical_value_text(value));
  });
  return out;
}

uint64_t transaction_id(duckdb::ClientContext& context)
{
  return context.transaction.HasActiveTransaction()
           ? context.ActiveTransaction().global_transaction_id
           : 0;
}

struct capture_input {
  duckdb::TableFunction const& function;
  duckdb::FunctionData const* bind_data;
  duckdb::vector<duckdb::Value> const& parameters;
  duckdb::named_parameter_map_t const* named_parameters;
  std::span<std::string const> resolved_paths;
  bool collect_evidence;
};

bound_read_view capture(capture_input input, duckdb::ClientContext& context)
{
  auto const* source = planner::lookup_scan_source(input.function, input.bind_data, context);
  if (!source) { throw std::runtime_error("cannot capture an unverified scan source"); }

  bound_read_identity identity;
  identity.source = {source->function_name, source->kind, source->registry_profile};
  bound_read_view view;
  view.transaction_id = transaction_id(context);
  std::vector<std::string> owned_paths;
  std::span<std::string const> paths = input.resolved_paths;

  if (source->kind == source_kind::duckdb_native) {
    auto const* typed = dynamic_cast<duckdb::TableScanBindData const*>(input.bind_data);
    if (!typed) throw std::runtime_error("seq_scan capture has no TableScanBindData");
    auto& table          = typed->table.Cast<duckdb::DuckTableEntry>();
    auto const& columns  = table.GetColumns();
    identity.bound_types = columns.GetColumnTypes();
    identity.bound_names = columns.GetColumnNames();
    auto& attached       = table.GetStorage().GetAttached();
    identity.data_view   = native_table_identity{table.ParentCatalog().GetName(),
                                               table.ParentCatalog().GetOid(),
                                               table.ParentSchema().name,
                                               table.name,
                                               table.oid,
                                               attached.GetStorageManager().GetDBPath()};
    view.provider        = provider_borrow{
      context.transaction.GetActiveQuery(), view.transaction_id, &context, &table.GetStorage()};
  } else if (source->kind == source_kind::stream_source) {
    auto const* typed = dynamic_cast<exec::stream_source_bind_data const*>(input.bind_data);
    if (!typed) throw std::runtime_error("stream capture has no stream_source_bind_data");
    auto const& binding  = exec::catalog_for(context)->get(typed->stream_id);
    identity.bound_types = sirius::to_duckdb_vec(binding.types);
    identity.bound_names = duckdb::vector<std::string>(binding.names.begin(), binding.names.end());
    identity.data_view   = stream_identity{typed->stream_id};
  } else if (source->kind == source_kind::parquet_s3) {
    auto const* typed = dynamic_cast<duckdb::SiriusReadParquetBindData const*>(input.bind_data);
    if (!typed) throw std::runtime_error("Sirius S3 capture has no typed bind data");
    if (paths.empty()) {
      owned_paths.push_back(typed->uri);
      paths = owned_paths;
    }
    identity.bound_types = typed->bound_types;
    identity.bound_names = typed->bound_names;
    identity.selector    = canonical_value_text(duckdb::Value(typed->uri));
    identity.data_view   = file_inventory{static_cast<uint32_t>(paths.size())};
  } else {
    auto const* typed = dynamic_cast<duckdb::MultiFileBindData const*>(input.bind_data);
    if (!typed || !typed->file_list) {
      throw std::runtime_error("parquet capture has no MultiFileBindData file list");
    }
    std::vector<duckdb::OpenFileInfo> files;
    if (paths.empty()) {
      for (auto const& file : typed->file_list->Files()) {
        owned_paths.push_back(file.path);
        if (input.collect_evidence) files.push_back(file);
      }
      paths = owned_paths;
    }
    identity.bound_types = typed->types;
    identity.bound_names = typed->names;
    // iceberg_scan owns a different outer serializer. Its nested format payload is still the
    // Parquet bind payload, so export it through the verified built-in Parquet protocol.
    identity.selector  = parquet_selector(trusted_parquet_serializer(), *typed);
    identity.data_view = file_inventory{static_cast<uint32_t>(paths.size())};

    if (input.collect_evidence) {
      std::vector<std::size_t> order(paths.size());
      std::iota(order.begin(), order.end(), 0);
      std::sort(order.begin(), order.end(), [&](auto left, auto right) {
        return paths[left] < paths[right];
      });
      auto evidence = std::make_shared<file_evidence_arrays>();
      evidence->size.resize(paths.size());
      evidence->last_modified.resize(paths.size());
      evidence->size_present.resize(paths.size());
      evidence->last_modified_present.resize(paths.size());
      evidence->etag.resize(paths.size());
      bool any_size = false;
      bool any_tag  = false;
      for (std::size_t i = 0; i < order.size(); ++i) {
        auto const file_index = order[i];
        if (!files[file_index].extended_info) continue;
        auto const& options = files[file_index].extended_info->options;
        if (auto found = options.find("file_size");
            found != options.end() && !found->second.IsNull()) {
          evidence->size[i]         = found->second.GetValue<int64_t>();
          evidence->size_present[i] = 1;
          any_size                  = true;
        }
        if (auto found = options.find("last_modified");
            found != options.end() && !found->second.IsNull()) {
          evidence->last_modified[i]         = found->second.GetValue<duckdb::timestamp_t>().value;
          evidence->last_modified_present[i] = 1;
        }
        if (auto found = options.find("etag"); found != options.end() && !found->second.IsNull()) {
          evidence->etag[i] = found->second.GetValue<std::string>();
          any_tag           = true;
        }
      }
      view.evidence = std::move(evidence);
      view.depth    = any_tag    ? evidence_depth::path_size_and_tag
                      : any_size ? evidence_depth::path_and_size
                                 : evidence_depth::path;
    }
  }

  if (source->selector_outside_bind_data && input.named_parameters) {
    view.logical_selector_evidence = selector_evidence(input.parameters, *input.named_parameters);
  }
  view.identity = make_bound_read_identity(std::move(identity), paths);
  return view;
}
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

bound_read_view capture_bound_read_view(duckdb::LogicalGet const& get,
                                        duckdb::ClientContext& context)
{
  return capture(
    {get.function, get.bind_data.get(), get.parameters, &get.named_parameters, {}, true}, context);
}

std::vector<logical_bound_read_view> capture_bound_read_views(duckdb::LogicalOperator const& root,
                                                              duckdb::ClientContext& context)
{
  std::vector<logical_bound_read_view> captured;
  auto visit = [&](auto&& self, duckdb::LogicalOperator const& op) -> void {
    if (op.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
      auto const& get = op.Cast<duckdb::LogicalGet>();
      if (planner::lookup_scan_source(get, context)) {
        captured.push_back({get.table_index, capture_bound_read_view(get, context)});
      }
    }
    for (auto const& child : op.children) {
      self(self, *child);
    }
  };
  visit(visit, root);
  return captured;
}

bound_read_view capture_bound_read_view(duckdb::PhysicalTableScan const& get,
                                        duckdb::ClientContext& context)
{
  return capture({get.function, get.bind_data.get(), get.parameters, nullptr, {}, true}, context);
}

bound_read_view capture_bound_read_view(sirius::op::sirius_physical_table_scan const& get,
                                        duckdb::ClientContext& context,
                                        std::span<std::string const> resolved_paths)
{
  return capture({get.function,
                  get.bind_data.get(),
                  get.parameters,
                  &get.named_parameters,
                  resolved_paths,
                  false},
                 context);
}
}  // namespace sirius::op::scan
