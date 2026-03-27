#include "node/storage.h"

#include <cstdint>
#include <fstream>
#include <utility>

namespace {
constexpr uint32_t SNAPSHOT_MAGIC = 0x4B565330;  // "KVS0"
constexpr uint32_t WAL_MAGIC = 0x57414C30;       // "WAL0"
constexpr uint8_t WAL_OP_PUT = 1;
constexpr uint8_t WAL_OP_DEL = 2;

}  // namespace

Storage::Storage(std::string snapshot_path,
                 std::string wal_path,
                 uint64_t snapshot_interval_seconds)
    : snapshot_path_(std::move(snapshot_path)),
      wal_path_(std::move(wal_path)),
      snapshot_enabled_(!snapshot_path_.empty()),
      wal_enabled_(!wal_path_.empty()),
      snapshot_interval_seconds_(snapshot_interval_seconds),
      last_snapshot_time_(std::chrono::steady_clock::now()) {}

Storage::~Storage() {
  if (wal_out_.is_open()) {
    wal_out_.flush();
    wal_out_.close();
  }
}

uint64_t Storage::put(const std::string& key, const std::string& value) {
  auto it = kv_.find(key);
  if (it == kv_.end()) {
    kv_[key] = VersionedValue{value, 1};
    append_wal_put(key, value, 1);
    return 1;
  }

  it->second.value = value;
  it->second.version += 1;
  append_wal_put(key, value, it->second.version);
  return it->second.version;
}

bool Storage::get(const std::string& key, VersionedValue& out) const {
  auto it = kv_.find(key);
  if (it == kv_.end()) return false;

  out = it->second;
  return true;
}

bool Storage::del(const std::string& key, uint64_t* deleted_version) {
  auto it = kv_.find(key);
  if (it == kv_.end()) return false;

  const uint64_t version = it->second.version;
  if (deleted_version != nullptr) {
    *deleted_version = version;
  }

  kv_.erase(it);
  append_wal_del(key, version);
  return true;
}

bool Storage::load_snapshot() {
  if (snapshot_enabled_) {
    std::ifstream in(snapshot_path_, std::ios::binary);
    if (in.is_open()) {
      uint32_t magic = 0;
      in.read(reinterpret_cast<char*>(&magic), sizeof(magic));
      if (!in || magic != SNAPSHOT_MAGIC) return false;

      uint64_t count = 0;
      in.read(reinterpret_cast<char*>(&count), sizeof(count));
      if (!in) return false;

      std::unordered_map<std::string, VersionedValue> restored;

      for (uint64_t i = 0; i < count; ++i) {
        uint32_t key_len = 0;
        uint32_t val_len = 0;
        uint64_t version = 0;

        in.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
        in.read(reinterpret_cast<char*>(&val_len), sizeof(val_len));
        in.read(reinterpret_cast<char*>(&version), sizeof(version));
        if (!in) return false;

        std::string key(key_len, '\0');
        std::string value(val_len, '\0');

        in.read(&key[0], static_cast<std::streamsize>(key_len));
        in.read(&value[0], static_cast<std::streamsize>(val_len));
        if (!in) return false;

        restored[key] = VersionedValue{value, version};
      }

      kv_ = std::move(restored);
    }
  }

  if (!wal_enabled_) {
    return true;
  }

  return replay_wal();
}

bool Storage::save_snapshot() {
  if (!snapshot_enabled_) return true;

  if (!is_snapshot_due()) {
    return true;
  }

  if (!save_snapshot_sync()) {
    return false;
  }

  if (wal_enabled_) {
    return truncate_wal();
  }

  return true;
}

bool Storage::save_snapshot_sync() const {
  std::ofstream out(snapshot_path_, std::ios::binary | std::ios::trunc);
  if (!out.is_open()) return false;

  const uint32_t magic = SNAPSHOT_MAGIC;
  const uint64_t count = static_cast<uint64_t>(kv_.size());
  out.write(reinterpret_cast<const char*>(&magic), sizeof(magic));
  out.write(reinterpret_cast<const char*>(&count), sizeof(count));

  for (const auto& entry : kv_) {
    const std::string& key = entry.first;
    const VersionedValue& vv = entry.second;

    const uint32_t key_len = static_cast<uint32_t>(key.size());
    const uint32_t val_len = static_cast<uint32_t>(vv.value.size());

    out.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
    out.write(reinterpret_cast<const char*>(&val_len), sizeof(val_len));
    out.write(reinterpret_cast<const char*>(&vv.version), sizeof(vv.version));
    out.write(key.data(), static_cast<std::streamsize>(key_len));
    out.write(vv.value.data(), static_cast<std::streamsize>(val_len));
  }

  out.flush();
  return static_cast<bool>(out);
}

bool Storage::replay_wal() {
  if (!wal_enabled_) return true;

  std::ifstream in(wal_path_, std::ios::binary);
  if (!in.is_open()) {
    // Missing WAL is okay.
    return true;
  }

  uint32_t magic = 0;
  in.read(reinterpret_cast<char*>(&magic), sizeof(magic));
  if (!in || magic != WAL_MAGIC) {
    return false;
  }

  while (true) {
    uint8_t op = 0;
    in.read(reinterpret_cast<char*>(&op), sizeof(op));
    if (!in) {
      if (in.eof()) {
        in.clear();
        return true;
      }
      return false;
    }

    uint32_t key_len = 0;
    uint32_t val_len = 0;
    uint64_t version = 0;

    in.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
    in.read(reinterpret_cast<char*>(&val_len), sizeof(val_len));
    in.read(reinterpret_cast<char*>(&version), sizeof(version));
    if (!in) return false;

    std::string key(key_len, '\0');
    in.read(&key[0], static_cast<std::streamsize>(key_len));
    if (!in) return false;

    if (op == WAL_OP_PUT) {
      std::string value(val_len, '\0');
      in.read(&value[0], static_cast<std::streamsize>(val_len));
      if (!in) return false;
      kv_[key] = VersionedValue{value, version};
    } else if (op == WAL_OP_DEL) {
      kv_.erase(key);
    } else {
      return false;
    }
  }
}

bool Storage::ensure_wal_open() {
  if (!wal_enabled_) {
    return true;
  }

  if (wal_out_.is_open()) {
    return true;
  }

  const bool wal_exists = std::ifstream(wal_path_, std::ios::binary).good();
  wal_out_.open(wal_path_, std::ios::binary | std::ios::app);
  if (!wal_out_.is_open()) {
    return false;
  }

  if (!wal_initialized_) {
    if (!wal_exists) {
      const uint32_t magic = WAL_MAGIC;
      wal_out_.write(reinterpret_cast<const char*>(&magic), sizeof(magic));
      wal_out_.flush();
      if (!wal_out_) {
        return false;
      }
    }
    wal_initialized_ = true;
  }

  return true;
}

bool Storage::append_wal_put_sync(const std::string& key,
                                  const std::string& value,
                                  uint64_t version) {
  if (!ensure_wal_open()) return false;

  const uint8_t op = WAL_OP_PUT;
  const uint32_t key_len = static_cast<uint32_t>(key.size());
  const uint32_t val_len = static_cast<uint32_t>(value.size());

  wal_out_.write(reinterpret_cast<const char*>(&op), sizeof(op));
  wal_out_.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
  wal_out_.write(reinterpret_cast<const char*>(&val_len), sizeof(val_len));
  wal_out_.write(reinterpret_cast<const char*>(&version), sizeof(version));
  wal_out_.write(key.data(), static_cast<std::streamsize>(key_len));
  wal_out_.write(value.data(), static_cast<std::streamsize>(val_len));

  ++wal_pending_ops_;
  if (wal_pending_ops_ >= kWalFlushEveryOps) {
    wal_out_.flush();
    wal_pending_ops_ = 0;
  }

  return static_cast<bool>(wal_out_);
}

bool Storage::append_wal_del_sync(const std::string& key, uint64_t version) {
  if (!ensure_wal_open()) return false;

  const uint8_t op = WAL_OP_DEL;
  const uint32_t key_len = static_cast<uint32_t>(key.size());
  const uint32_t val_len = 0;

  wal_out_.write(reinterpret_cast<const char*>(&op), sizeof(op));
  wal_out_.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
  wal_out_.write(reinterpret_cast<const char*>(&val_len), sizeof(val_len));
  wal_out_.write(reinterpret_cast<const char*>(&version), sizeof(version));
  wal_out_.write(key.data(), static_cast<std::streamsize>(key_len));

  ++wal_pending_ops_;
  if (wal_pending_ops_ >= kWalFlushEveryOps) {
    wal_out_.flush();
    wal_pending_ops_ = 0;
  }

  return static_cast<bool>(wal_out_);
}

bool Storage::append_wal_put(const std::string& key,
                             const std::string& value,
                             uint64_t version) {
  if (!wal_enabled_) return true;

  return append_wal_put_sync(key, value, version);
}

bool Storage::append_wal_del(const std::string& key, uint64_t version) {
  if (!wal_enabled_) return true;

  return append_wal_del_sync(key, version);
}

bool Storage::is_snapshot_due() {
  if (!snapshot_enabled_) return false;

  const auto now = std::chrono::steady_clock::now();
  const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_snapshot_time_);
  if (elapsed.count() < static_cast<long long>(snapshot_interval_seconds_)) {
    return false;
  }

  last_snapshot_time_ = now;
  return true;
}

bool Storage::truncate_wal_sync() {
  if (wal_out_.is_open()) {
    wal_out_.flush();
    wal_out_.close();
  }

  std::ofstream out(wal_path_, std::ios::binary | std::ios::trunc);
  if (!out.is_open()) return false;

  const uint32_t magic = WAL_MAGIC;
  out.write(reinterpret_cast<const char*>(&magic), sizeof(magic));
  out.flush();
  if (!out) return false;

  wal_out_.open(wal_path_, std::ios::binary | std::ios::app);
  if (!wal_out_.is_open()) {
    return false;
  }
  wal_initialized_ = true;
  wal_pending_ops_ = 0;
  return true;
}

bool Storage::truncate_wal() {
  if (!wal_enabled_) return true;

  return truncate_wal_sync();
}
