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

#pragma once

#include "duckdb/main/client_context_state.hpp"
#include "exec/batch_stream.hpp"
#include "exec/stream_session.hpp"
#include "scan/bound_schema.hpp"

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace sirius::exec {

/// Caller-supplied declaration, or a detached catalog snapshot returned by get().
struct stream_input_binding {
  std::vector<std::string> names;
  duckdb::vector<sirius::logical_type> types;
  std::shared_ptr<cucascade::shared_data_repository> repository;
  std::set<sender_id_t> expected_senders;
  /// Observational back-pointer only. declare() never accepts a pre-built operator.
  op::sirius_physical_streaming_source* built = nullptr;
  /// Full DuckDB schema when available at the caller's binding boundary.
  scan::bound_schema_ptr schema = nullptr;
};

/// One immutable declaration. Logical bindings and runtime attachments retain this exact owner.
class stream_declaration {
 public:
  const std::uint64_t catalog_instance;
  const stream_id_t stream_id;
  const std::uint64_t generation;
  const scan::bound_schema_ptr schema;
  const duckdb::vector<sirius::logical_type> types;
  const std::shared_ptr<cucascade::shared_data_repository> repository;
  const std::set<sender_id_t> expected_senders;

 private:
  friend class stream_bind_catalog;
  stream_declaration(std::uint64_t catalog_instance,
                     stream_id_t id,
                     std::uint64_t generation,
                     stream_input_binding binding);
};

using stream_declaration_ptr = std::shared_ptr<const stream_declaration>;

/// Per-connection catalog. The mutable operator attachment is separate from immutable metadata.
/// Replacing/removing a declaration with live binding or runtime handles is an error.
class stream_bind_catalog : public duckdb::ClientContextState {
 public:
  static constexpr const char* kStateKey = "sirius_stream_catalog";

  stream_bind_catalog();

  /// Validate and publish a new generation; never reuse a generation, including after erase().
  /// @throws sirius::invalid_input_exception on malformed input or stream_binding_in_use.
  std::uint64_t declare(stream_id_t id, stream_input_binding binding);

  /// All-or-nothing: reject if any declaration has live handles.
  void clear();
  void erase(stream_id_t id);
  /// Erase only a generation owned by this caller. A different/current generation is untouched.
  void erase(stream_id_t id, std::uint64_t generation);

  [[nodiscard]] bool contains(stream_id_t id) const;
  /// Detached snapshot: no reference into a map unlocked before its caller uses it.
  [[nodiscard]] stream_input_binding get(stream_id_t id) const;
  [[nodiscard]] stream_declaration_ptr get_declaration(stream_id_t id) const;

  /// Register one plan leaf. Generation-aware callers must use the declaration overload.
  void set_built(stream_id_t id, op::sirius_physical_streaming_source* built);
  void set_built(const stream_declaration& declaration,
                 op::sirius_physical_streaming_source* built);
  void clear_built(const stream_declaration& declaration,
                   op::sirius_physical_streaming_source* built) noexcept;
  [[nodiscard]] op::sirius_physical_streaming_source* get_built(stream_id_t id,
                                                                std::uint64_t generation) const;

  [[nodiscard]] std::vector<stream_id_t> declared_streams() const;

 private:
  struct entry {
    stream_declaration_ptr declaration;
    op::sirius_physical_streaming_source* built = nullptr;
  };

  static void require_unused(const entry& value);
  static void attach(entry& value, op::sirius_physical_streaming_source* built);

  const std::uint64_t _instance;
  std::uint64_t _last_generation{0};
  mutable std::mutex _mutex;
  std::map<stream_id_t, entry> _entries;
};

/// Plan-owned attachment. Its destructor only clears its own generation/operator pair.
class stream_source_attachment {
 public:
  stream_source_attachment(duckdb::shared_ptr<stream_bind_catalog> catalog,
                           stream_declaration_ptr declaration,
                           op::sirius_physical_streaming_source* source);
  ~stream_source_attachment();

  stream_source_attachment(const stream_source_attachment&)            = delete;
  stream_source_attachment& operator=(const stream_source_attachment&) = delete;

 private:
  duckdb::shared_ptr<stream_bind_catalog> _catalog;
  stream_declaration_ptr _declaration;
  op::sirius_physical_streaming_source* _source;
};

/// The catalog registered on the binding context, or an explanatory error.
duckdb::shared_ptr<stream_bind_catalog> catalog_for(duckdb::ClientContext& context);

}  // namespace sirius::exec
