#pragma once

#include <duckdb/common/local_file_system.hpp>
#include <duckdb/common/virtual_file_system.hpp>

#include <atomic>

namespace sirius::test {
struct planning_io_result {
  uint64_t opens = 0, read_calls = 0, bytes_read = 0;
  uint64_t metadata_requests = 0, directory_lists = 0, files_enumerated = 0, directory_entries = 0;
};
// Observe DuckDB local filesystem operations without forcing lazy file lists.
// These are API requests (including cached operations), not physical disk or network I/O.
class planning_file_system final : public duckdb::LocalFileSystem {
 public:
  void start() noexcept
  {
    opens_ = reads_ = bytes_ = metadata_ = lists_ = entries_ = files_ = 0;
    enabled_.store(true);
  }
  planning_io_result stop() noexcept
  {
    enabled_.store(false);
    return {opens_.load(),
            reads_.load(),
            bytes_.load(),
            metadata_.load(),
            lists_.load(),
            files_.load(),
            entries_.load()};
  }
  duckdb::unique_ptr<duckdb::FileHandle> OpenFile(
    duckdb::string const& path,
    duckdb::FileOpenFlags flags,
    duckdb::optional_ptr<duckdb::FileOpener> opener) override
  {
    if (enabled_) ++opens_;
    return LocalFileSystem::OpenFile(path, flags, opener);
  }
  void Read(duckdb::FileHandle& handle, void* buffer, int64_t bytes, duckdb::idx_t offset) override
  {
    bool count = enabled_.load();
    if (count) ++reads_;
    LocalFileSystem::Read(handle, buffer, bytes, offset);
    if (count) bytes_ += bytes;
  }
  int64_t Read(duckdb::FileHandle& handle, void* buffer, int64_t bytes) override
  {
    bool count = enabled_.load();
    if (count) ++reads_;
    auto read = LocalFileSystem::Read(handle, buffer, bytes);
    if (count && read > 0) bytes_ += read;
    return read;
  }
  int64_t GetFileSize(duckdb::FileHandle& handle) override
  {
    if (enabled_) ++metadata_;
    return LocalFileSystem::GetFileSize(handle);
  }
  duckdb::timestamp_t GetLastModifiedTime(duckdb::FileHandle& handle) override
  {
    if (enabled_) ++metadata_;
    return LocalFileSystem::GetLastModifiedTime(handle);
  }
  duckdb::string GetVersionTag(duckdb::FileHandle& handle) override
  {
    if (enabled_) ++metadata_;
    return LocalFileSystem::GetVersionTag(handle);
  }
  duckdb::FileType GetFileType(duckdb::FileHandle& handle) override
  {
    if (enabled_) ++metadata_;
    return LocalFileSystem::GetFileType(handle);
  }
  duckdb::FileMetadata Stats(duckdb::FileHandle& handle) override
  {
    if (enabled_) ++metadata_;
    return LocalFileSystem::Stats(handle);
  }
  bool FileExists(duckdb::string const& path,
                  duckdb::optional_ptr<duckdb::FileOpener> opener) override
  {
    if (enabled_) ++metadata_;
    return LocalFileSystem::FileExists(path, opener);
  }
  bool DirectoryExists(duckdb::string const& path,
                       duckdb::optional_ptr<duckdb::FileOpener> opener) override
  {
    if (enabled_) ++metadata_;
    return LocalFileSystem::DirectoryExists(path, opener);
  }

 protected:
  bool ListFilesExtended(duckdb::string const& directory,
                         std::function<void(duckdb::OpenFileInfo&)> const& callback,
                         duckdb::optional_ptr<duckdb::FileOpener> opener) override
  {
    bool count = enabled_.load();
    if (count) ++lists_;
    return LocalFileSystem::ListFilesExtended(
      directory,
      [&](duckdb::OpenFileInfo& info) {
        if (count) {
          ++entries_;
          if (info.extended_info) {
            auto type = info.extended_info->options.find("type");
            if (type != info.extended_info->options.end() &&
                type->second.GetValue<std::string>() == "file")
              ++files_;
          }
        }
        callback(info);
      },
      opener);
  }

 private:
  std::atomic<bool> enabled_{false};
  std::atomic<uint64_t> opens_{0}, reads_{0}, bytes_{0}, metadata_{0}, lists_{0}, entries_{0},
    files_{0};
};
}  // namespace sirius::test
