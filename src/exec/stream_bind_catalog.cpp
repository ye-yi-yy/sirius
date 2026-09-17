/*
 * Copyright 2025, Sirius Contributors.
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

#include "exec/stream_bind_catalog.hpp"

#include "helper/type_conversions.hpp"
#include "sirius/exception.hpp"

#include <atomic>
#include <limits>
#include <string>
#include <utility>

namespace sirius::exec {
namespace {

std::uint64_t next_catalog_instance()
{
  static std::atomic<std::uint64_t> last{0};
  auto previous = last.load(std::memory_order_relaxed);
  do {
    if (previous == std::numeric_limits<std::uint64_t>::max()) {
      throw sirius::invalid_input_exception("stream_bind_catalog: catalog identity exhausted");
    }
  } while (!last.compare_exchange_weak(previous, previous + 1, std::memory_order_relaxed));
  return previous + 1;
}

[[noreturn]] void throw_undeclared(stream_id_t id)
{
  throw sirius::invalid_input_exception("stream_bind_catalog: no input stream declared with id " +
                                        std::to_string(id));
}

[[noreturn]] void throw_stale(stream_id_t id)
{
  throw sirius::invalid_input_exception("stream_bind_catalog: stale declaration for input stream " +
                                        std::to_string(id));
}

bool needs_full_schema(const sirius::logical_type& type)
{
  if (type.id() == sirius::type_id::STRUCT || type.id() == sirius::type_id::LIST) { return true; }
  return type.is_array() && type.has_child() && needs_full_schema(type.array_child());
}

}  // namespace

stream_declaration::stream_declaration(std::uint64_t catalog_instance,
                                       stream_id_t id,
                                       std::uint64_t generation,
                                       stream_input_binding binding)
  : catalog_instance(catalog_instance),
    stream_id(id),
    generation(generation),
    schema(std::move(binding.schema)),
    types(std::move(binding.types)),
    repository(std::move(binding.repository)),
    expected_senders(std::move(binding.expected_senders))
{
}

stream_bind_catalog::stream_bind_catalog() : _instance(next_catalog_instance()) {}

void stream_bind_catalog::require_unused(const entry& value)
{
  if (value.built || value.declaration.use_count() != 1) {
    throw sirius::invalid_input_exception("stream_binding_in_use: input stream " +
                                          std::to_string(value.declaration->stream_id) +
                                          " still has a live binding or operator");
  }
}

std::uint64_t stream_bind_catalog::declare(stream_id_t id, stream_input_binding binding)
{
  if (!binding.repository) {
    throw sirius::invalid_input_exception("stream_bind_catalog: input stream " +
                                          std::to_string(id) +
                                          " must be declared with a repository");
  }
  if (binding.names.size() != binding.types.size() || binding.names.empty()) {
    throw sirius::invalid_input_exception(
      "stream_bind_catalog: input stream " + std::to_string(id) +
      " must declare matching non-empty column names and types");
  }
  if (binding.built) {
    throw sirius::invalid_input_exception(
      "stream_bind_catalog: a declaration cannot attach an operator");
  }
  if (!binding.schema) {
    for (const auto& type : binding.types) {
      if (needs_full_schema(type)) {
        throw sirius::invalid_input_exception(
          "stream_bind_catalog: nested input types require a full bound schema");
      }
    }
    binding.schema = std::make_shared<const scan::bound_schema>(
      binding.names, sirius::to_duckdb_vec(binding.types));
  } else if (binding.schema->names() != binding.names ||
             sirius::from_duckdb_vec(binding.schema->types()) != binding.types) {
    throw sirius::invalid_input_exception(
      "stream_bind_catalog: full schema disagrees with input declaration");
  }

  std::lock_guard<std::mutex> guard(_mutex);
  const auto existing = _entries.find(id);
  if (existing != _entries.end()) { require_unused(existing->second); }
  if (_last_generation == std::numeric_limits<std::uint64_t>::max()) {
    throw sirius::invalid_input_exception("stream_bind_catalog: declaration generation exhausted");
  }
  const auto generation = ++_last_generation;
  auto declaration =
    stream_declaration_ptr(new stream_declaration(_instance, id, generation, std::move(binding)));
  _entries.insert_or_assign(id, entry{std::move(declaration), nullptr});
  return generation;
}

void stream_bind_catalog::clear()
{
  std::lock_guard<std::mutex> guard(_mutex);
  for (const auto& [id, value] : _entries) {
    require_unused(value);
  }
  _entries.clear();
}

void stream_bind_catalog::erase(stream_id_t id)
{
  std::lock_guard<std::mutex> guard(_mutex);
  const auto it = _entries.find(id);
  if (it == _entries.end()) { return; }
  require_unused(it->second);
  _entries.erase(it);
}

void stream_bind_catalog::erase(stream_id_t id, std::uint64_t generation)
{
  std::lock_guard<std::mutex> guard(_mutex);
  const auto it = _entries.find(id);
  if (it == _entries.end() || it->second.declaration->generation != generation) { return; }
  require_unused(it->second);
  _entries.erase(it);
}

bool stream_bind_catalog::contains(stream_id_t id) const
{
  std::lock_guard<std::mutex> guard(_mutex);
  return _entries.find(id) != _entries.end();
}

stream_input_binding stream_bind_catalog::get(stream_id_t id) const
{
  std::lock_guard<std::mutex> guard(_mutex);
  const auto it = _entries.find(id);
  if (it == _entries.end()) { throw_undeclared(id); }
  const auto& declaration = *it->second.declaration;
  return {declaration.schema->names(),
          declaration.types,
          declaration.repository,
          declaration.expected_senders,
          it->second.built,
          declaration.schema};
}

stream_declaration_ptr stream_bind_catalog::get_declaration(stream_id_t id) const
{
  std::lock_guard<std::mutex> guard(_mutex);
  const auto it = _entries.find(id);
  if (it == _entries.end()) { throw_undeclared(id); }
  return it->second.declaration;
}

void stream_bind_catalog::attach(entry& value, op::sirius_physical_streaming_source* built)
{
  if (!built) {
    throw sirius::invalid_input_exception("stream_bind_catalog: cannot attach a null operator");
  }
  if (value.built) {
    throw sirius::invalid_input_exception(
      "stream_bind_catalog: input stream " + std::to_string(value.declaration->stream_id) +
      " is read by more than one operator in the same plan — fan-out reads of a single "
      "declared stream are not supported");
  }
  value.built = built;
}

void stream_bind_catalog::set_built(stream_id_t id, op::sirius_physical_streaming_source* built)
{
  std::lock_guard<std::mutex> guard(_mutex);
  const auto it = _entries.find(id);
  if (it == _entries.end()) { throw_undeclared(id); }
  attach(it->second, built);
}

void stream_bind_catalog::set_built(const stream_declaration& declaration,
                                    op::sirius_physical_streaming_source* built)
{
  std::lock_guard<std::mutex> guard(_mutex);
  const auto it = _entries.find(declaration.stream_id);
  if (declaration.catalog_instance != _instance || it == _entries.end() ||
      it->second.declaration.get() != &declaration) {
    throw_stale(declaration.stream_id);
  }
  attach(it->second, built);
}

void stream_bind_catalog::clear_built(const stream_declaration& declaration,
                                      op::sirius_physical_streaming_source* built) noexcept
{
  std::lock_guard<std::mutex> guard(_mutex);
  const auto it = _entries.find(declaration.stream_id);
  if (it != _entries.end() && it->second.declaration.get() == &declaration &&
      it->second.built == built) {
    it->second.built = nullptr;
  }
}

op::sirius_physical_streaming_source* stream_bind_catalog::get_built(stream_id_t id,
                                                                     std::uint64_t generation) const
{
  std::lock_guard<std::mutex> guard(_mutex);
  const auto it = _entries.find(id);
  if (it == _entries.end() || it->second.declaration->generation != generation) { throw_stale(id); }
  return it->second.built;
}

std::vector<stream_id_t> stream_bind_catalog::declared_streams() const
{
  std::lock_guard<std::mutex> guard(_mutex);
  std::vector<stream_id_t> ids;
  ids.reserve(_entries.size());
  for (const auto& [id, value] : _entries) {
    ids.push_back(id);
  }
  return ids;
}

stream_source_attachment::stream_source_attachment(duckdb::shared_ptr<stream_bind_catalog> catalog,
                                                   stream_declaration_ptr declaration,
                                                   op::sirius_physical_streaming_source* source)
  : _catalog(std::move(catalog)), _declaration(std::move(declaration)), _source(source)
{
  if (!_catalog || !_declaration) {
    throw sirius::invalid_input_exception(
      "stream source attachment requires a catalog and declaration");
  }
  _catalog->set_built(*_declaration, _source);
}

stream_source_attachment::~stream_source_attachment()
{
  _catalog->clear_built(*_declaration, _source);
}

duckdb::shared_ptr<stream_bind_catalog> catalog_for(duckdb::ClientContext& context)
{
  auto catalog = context.registered_state->Get<stream_bind_catalog>(stream_bind_catalog::kStateKey);
  if (!catalog) {
    throw sirius::invalid_input_exception(
      "no stream catalog on this connection — the fragment must declare its input streams before "
      "the plan is bound");
  }
  return catalog;
}

}  // namespace sirius::exec
