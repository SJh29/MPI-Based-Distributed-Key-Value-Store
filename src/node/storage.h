#pragma once

#include <cstdint>
#include <chrono>
#include <string>
#include <unordered_map>
#include <fstream>

struct VersionedValue {
  std::string value;
  uint64_t version{0};
};

class Storage {
 public:
  Storage(std::string snapshot_path = "",
          std::string wal_path = "",
          uint64_t snapshot_interval_seconds = 1800);
  ~Storage();

  uint64_t put(const std::string& key, const std::string& value);
  bool get(const std::string& key, VersionedValue& out) const;
  bool del(const std::string& key, uint64_t* deleted_version = nullptr);

  bool load_snapshot();
  bool save_snapshot();

 private:
  bool replay_wal();
  bool save_snapshot_sync() const;
  bool ensure_wal_open();
  bool append_wal_put_sync(const std::string& key,
                           const std::string& value,
                           uint64_t version);
  bool append_wal_del_sync(const std::string& key, uint64_t version);
  bool append_wal_put(const std::string& key, const std::string& value, uint64_t version);
  bool append_wal_del(const std::string& key, uint64_t version);
  bool is_snapshot_due();
  bool truncate_wal();
  bool truncate_wal_sync();

  std::string snapshot_path_;
  std::string wal_path_;
  bool snapshot_enabled_{false};
  bool wal_enabled_{false};
  uint64_t snapshot_interval_seconds_{1800};
  std::chrono::steady_clock::time_point last_snapshot_time_{};
  std::unordered_map<std::string, VersionedValue> kv_;
  std::ofstream wal_out_{};
  bool wal_initialized_{false};
  uint32_t wal_pending_ops_{0};
  static constexpr uint32_t kWalFlushEveryOps = 128;
};
