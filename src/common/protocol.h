#pragma once

#include <cstdint>
#include <cstring>
#include <string_view>

constexpr int KEY_MAX = 64;
constexpr int VAL_MAX = 256;


enum class Status : int32_t {
  OK = 0,
  NOT_FOUND = 1,
  ERROR = 2,
  INVALID = 3
};

enum class MsgType : int32_t {
  // Client -> node 
  PUT_REQ = 1,
  GET_REQ = 2,
  DEL_REQ = 3,

  // Node -> Node (replication)
  REPL_PUT = 10,
  REPL_DEL = 11,

  // Node -> Client 
  RESP = 20,

  // control
  SHUTDOWN = 99
};


// A single fixed-size message struct that can represent both requests and responses.
// Designed to be trivially sendable via MPI as bytes (MPI_BYTE).
// IMPORTANT: Prefer to send as raw bytes rather than defining a custom MPI datatype
// to avoid padding issues.
struct Message {
  MsgType type{MsgType::RESP};

  // For routing responses back to the client/coordinator rank.
  int32_t client_rank{-1};

  // Useful for matching responses to requests.
  int32_t request_id{0};

  // The rank that originated the request or replication update.
  int32_t origin_rank{-1};

  // For sharding/routing debug or replicas: who is the primary for this key?
  int32_t primary_rank{-1};

  // Used when replication wants to indicate which replica index this is (0..R-1)
  int32_t replica_index{-1};

  // Response status (only meaningful for RESP)
  Status status{Status::OK};

  // Versioning support (useful later)
  uint64_t version{0};

  // Payload: key/value.
  // For GET requests, value is unused.
  // For GET responses, value contains the result.
  char key[KEY_MAX]{0};
  char value[VAL_MAX]{0};
};

// ---------- helpers ----------

// Safe copy into fixed buffers (always NUL-terminated).
inline void set_field(char* dst, int dst_len, std::string_view src) {
  if (dst_len <= 0) return;
  const int n = static_cast<int>(src.size());
  const int copy_n = (n < (dst_len - 1)) ? n : (dst_len - 1);
  std::memcpy(dst, src.data(), copy_n);
  dst[copy_n] = '\0';
}

inline std::string_view view_field(const char* src, int src_len) {
  // strnlen-like behavior without relying on POSIX strnlen
  int n = 0;
  while (n < src_len && src[n] != '\0') ++n;
  return std::string_view(src, static_cast<size_t>(n));
}

// Constructors for common message types
inline Message make_put_req(int client_rank, int request_id, int origin_rank,
                            std::string_view key, std::string_view value) {
  Message m;
  m.type = MsgType::PUT_REQ;
  m.client_rank = client_rank;
  m.request_id = request_id;
  m.origin_rank = origin_rank;
  set_field(m.key, KEY_MAX, key);
  set_field(m.value, VAL_MAX, value);
  return m;
}

inline Message make_get_req(int client_rank, int request_id, int origin_rank,
                            std::string_view key) {
  Message m;
  m.type = MsgType::GET_REQ;
  m.client_rank = client_rank;
  m.request_id = request_id;
  m.origin_rank = origin_rank;
  set_field(m.key, KEY_MAX, key);
  return m;
}

inline Message make_del_req(int client_rank, int request_id, int origin_rank,
                            std::string_view key) {
  Message m;
  m.type = MsgType::DEL_REQ;
  m.client_rank = client_rank;
  m.request_id = request_id;
  m.origin_rank = origin_rank;
  set_field(m.key, KEY_MAX, key);
  return m;
}

inline Message make_resp(int client_rank, int request_id, Status st,
                         std::string_view key = {}, std::string_view value = {},
                         uint64_t version = 0) {
  Message m;
  m.type = MsgType::RESP;
  m.client_rank = client_rank;
  m.request_id = request_id;
  m.status = st;
  m.version = version;
  if (!key.empty()) set_field(m.key, KEY_MAX, key);
  if (!value.empty()) set_field(m.value, VAL_MAX, value);
  return m;
}

inline Message make_repl_put(int origin_rank, int primary_rank, int replica_index,
                             std::string_view key, std::string_view value,
                             uint64_t version = 0) {
  Message m;
  m.type = MsgType::REPL_PUT;
  m.origin_rank = origin_rank;
  m.primary_rank = primary_rank;
  m.replica_index = replica_index;
  m.version = version;
  set_field(m.key, KEY_MAX, key);
  set_field(m.value, VAL_MAX, value);
  return m;
}

inline Message make_repl_del(int origin_rank, int primary_rank, int replica_index,
                             std::string_view key, uint64_t version = 0) {
  Message m;
  m.type = MsgType::REPL_DEL;
  m.origin_rank = origin_rank;
  m.primary_rank = primary_rank;
  m.replica_index = replica_index;
  m.version = version;
  set_field(m.key, KEY_MAX, key);
  return m;
}

inline Message make_shutdown() {
  Message m;
  m.type = MsgType::SHUTDOWN;
  return m;
}

// Get key/value as string_views (no allocation)
inline std::string_view key_view(const Message& m)   { return view_field(m.key, KEY_MAX); }
inline std::string_view value_view(const Message& m) { return view_field(m.value, VAL_MAX); }

// Byte size for MPI sends (use this with MPI_Send(..., MSG_BYTES, MPI_BYTE, ...))
constexpr int MSG_BYTES = static_cast<int>(sizeof(Message));
static_assert(MSG_BYTES > 0, "Message size should be > 0");
