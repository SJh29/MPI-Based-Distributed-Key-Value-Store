#include <mpi.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "common/mpi_comm.h"
#include "node/node_server.h"
#include "router/router.h"

using namespace mpi_comm;

namespace {

struct DecodedSocketRequest {
  uint32_t client_id = 0;
  MsgType type = MsgType::GET_REQ;
  std::string key;
  std::string value;
};

bool has_flag(int argc, char** argv, const std::string& flag) {
  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == flag) {
      return true;
    }
  }
  return false;
}

std::string parse_string_arg(int argc,
                             char** argv,
                             const std::string& arg_name,
                             const std::string& fallback) {
  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == arg_name && i + 1 < argc) {
      return argv[++i];
    }
  }
  return fallback;
}

int parse_int_arg(int argc, char** argv, const std::string& arg_name, int fallback) {
  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == arg_name && i + 1 < argc) {
      return std::atoi(argv[++i]);
    }
  }
  return fallback;
}

int validate_port(int port, const std::string& arg_name) {
  if (port < 1 || port > 65535) {
    throw std::invalid_argument(arg_name + " must be between 1 and 65535");
  }
  return port;
}

uint64_t parse_snapshot_interval_seconds(int argc, char** argv) {
  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == "--snapshot-interval-sec" && i + 1 < argc) {
      const int value = std::atoi(argv[++i]);
      if (value > 0) {
        return static_cast<uint64_t>(value);
      }
      throw std::invalid_argument("--snapshot-interval-sec must be > 0");
    }
  }
  return 1800;
}

bool resolve_component_persistence(int argc,
                                   char** argv,
                                   bool default_enabled,
                                   const std::string& enable_flag,
                                   const std::string& disable_flag) {
  if (has_flag(argc, argv, disable_flag)) {
    return false;
  }
  if (has_flag(argc, argv, enable_flag)) {
    return true;
  }
  return default_enabled;
}

int open_listener(const std::string& bind_address, int port) {
  const int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::runtime_error("socket() failed: " + std::string(std::strerror(errno)));
  }

  const int reuse = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    close(fd);
    throw std::runtime_error("setsockopt(SO_REUSEADDR) failed: " + std::string(std::strerror(errno)));
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  if (inet_pton(AF_INET, bind_address.c_str(), &addr.sin_addr) != 1) {
    close(fd);
    throw std::invalid_argument("invalid --bind-address value: " + bind_address);
  }

  if (bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    close(fd);
    throw std::runtime_error("bind() failed on port " + std::to_string(port) + ": " +
                             std::string(std::strerror(errno)));
  }

  if (listen(fd, 128) < 0) {
    close(fd);
    throw std::runtime_error("listen() failed on port " + std::to_string(port) + ": " +
                             std::string(std::strerror(errno)));
  }

  return fd;
}

void send_all(int fd, const char* data, size_t size) {
  size_t written = 0;
  while (written < size) {
    const ssize_t n = send(fd, data + written, size - written, MSG_NOSIGNAL);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error("send() failed: " + std::string(std::strerror(errno)));
    }
    written += static_cast<size_t>(n);
  }
}

void send_text(int fd, const std::string& text) {
  send_all(fd, text.data(), text.size());
}

void recv_exact(int fd, char* buffer, size_t size) {
  size_t received = 0;
  while (received < size) {
    const ssize_t n = recv(fd, buffer + received, size - received, 0);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error("recv() failed: " + std::string(std::strerror(errno)));
    }
    if (n == 0) {
      throw std::runtime_error("client disconnected before request was fully received");
    }
    received += static_cast<size_t>(n);
  }
}

std::vector<char> recv_framed_message(int fd) {
  uint32_t frame_size_net = 0;
  recv_exact(fd, reinterpret_cast<char*>(&frame_size_net), sizeof(frame_size_net));
  const uint32_t frame_size = ntohl(frame_size_net);
  if (frame_size < 9 || frame_size > 64 * 1024) {
    throw std::runtime_error("invalid frame size");
  }

  std::vector<char> body(frame_size);
  recv_exact(fd, body.data(), body.size());
  return body;
}

DecodedSocketRequest decode_socket_request(const std::vector<char>& body) {
  if (body.size() < 9) {
    throw std::runtime_error("request body too small");
  }

  size_t offset = 0;
  const auto read_u32 = [&](size_t at) -> uint32_t {
    uint32_t value = 0;
    std::memcpy(&value, body.data() + at, sizeof(value));
    return ntohl(value);
  };
  const auto read_u16 = [&](size_t at) -> uint16_t {
    uint16_t value = 0;
    std::memcpy(&value, body.data() + at, sizeof(value));
    return ntohs(value);
  };

  DecodedSocketRequest request;
  request.client_id = read_u32(offset);
  offset += sizeof(uint32_t);

  const uint8_t op = static_cast<uint8_t>(body[offset++]);
  if (op == 1) {
    request.type = MsgType::PUT_REQ;
  } else if (op == 2) {
    request.type = MsgType::GET_REQ;
  } else if (op == 3) {
    request.type = MsgType::DEL_REQ;
  } else {
    throw std::runtime_error("invalid op in request");
  }

  const uint16_t key_size = read_u16(offset);
  offset += sizeof(uint16_t);
  const uint16_t value_size = read_u16(offset);
  offset += sizeof(uint16_t);

  const size_t expected_size = offset + static_cast<size_t>(key_size) + static_cast<size_t>(value_size);
  if (body.size() != expected_size) {
    throw std::runtime_error("request body length mismatch");
  }

  request.key.assign(body.data() + offset, body.data() + offset + key_size);
  offset += key_size;
  request.value.assign(body.data() + offset, body.data() + offset + value_size);

  if (request.type != MsgType::PUT_REQ && !request.value.empty()) {
    throw std::runtime_error("value is only allowed for PUT");
  }

  return request;
}

Message make_operation_request(MsgType type,
                               int request_id,
                               std::string_view key,
                               std::string_view value,
                               int primary_rank) {
  Message req{};
  if (type == MsgType::PUT_REQ) {
    req = make_put_req(0, request_id, 0, key, value);
  } else if (type == MsgType::GET_REQ) {
    req = make_get_req(0, request_id, 0, key);
  } else {
    req = make_del_req(0, request_id, 0, key);
  }
  req.primary_rank = primary_rank;
  return req;
}

bool handle_socket_request(int client_fd,
                           int world_size,
                           int request_id,
                           bool verbose) {
  const DecodedSocketRequest request = decode_socket_request(recv_framed_message(client_fd));
  const std::string& key = request.key;
  const std::string& value = request.value;
  const MsgType op_type = request.type;
  const char* op_name = (op_type == MsgType::PUT_REQ) ? "PUT" : (op_type == MsgType::GET_REQ ? "GET" : "DEL");

  if (key.empty()) {
    send_text(client_fd, "status=INVALID error=empty_key\\n");
    return true;
  }

  if (op_type == MsgType::DEL_REQ && key == "__shutdown__") {
    for (int rank = 1; rank < world_size; ++rank) {
      send_msg(rank, make_shutdown());
    }
    send_text(client_fd, "status=OK action=shutdown\\n");
    return false;
  }

  const int primary_rank = route_key_to_primary(key, world_size);
  if (primary_rank < 1) {
    send_text(client_fd, "status=ERROR error=failed_to_route_key\\n");
    return true;
  }

  const Message req =
      make_operation_request(op_type, request_id, std::string_view(key), std::string_view(value), primary_rank);
  send_msg(primary_rank, req);

  Message resp{};
  recv_msg(primary_rank, resp);

  std::string response =
      "status=" + std::to_string(static_cast<int>(resp.status)) +
      " request_id=" + std::to_string(resp.request_id) +
      " version=" + std::to_string(resp.version);
  if (resp.status == Status::OK && op_type == MsgType::GET_REQ) {
    response += " value=" + std::string(value_view(resp));
  }
  response += "\\n";

  if (verbose) {
    std::cout << "[Gateway] client_id=" << request.client_id
              << " op=" << op_name
              << " key=" << key
              << " primary_rank=" << primary_rank
              << " request_id=" << request_id
              << " status=" << static_cast<int>(resp.status) << "\n";
  }

  send_text(client_fd, response);
  return true;
}

void run_socket_gateway(int argc, char** argv, int world_size, bool verbose) {
  const std::string bind_address = parse_string_arg(argc, argv, "--bind-address", "0.0.0.0");
  const int gateway_port = validate_port(parse_int_arg(argc, argv, "--gateway-port", 7100), "--gateway-port");

  try {
    const int listener_fd = open_listener(bind_address, gateway_port);
    if (verbose) {
      std::cout << "[Gateway] Listening on " << bind_address << ":" << gateway_port
                << " (framed binary protocol)\n";
    }

    int next_request_id = 1;
    bool running = true;
    while (running) {
      sockaddr_in client_addr{};
      socklen_t client_len = sizeof(client_addr);
      const int client_fd = accept(listener_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
      if (client_fd < 0) {
        if (errno == EINTR) {
          continue;
        }
        throw std::runtime_error("accept() failed: " + std::string(std::strerror(errno)));
      }

      try {
        running = handle_socket_request(client_fd, world_size, next_request_id++, verbose);
      } catch (const std::exception& ex) {
        send_text(client_fd, std::string("status=ERROR error=") + ex.what() + "\n");
      }
      close(client_fd);
    }
    close(listener_fd);
  } catch (...) {
    throw;
  }
}

}  // namespace

int main(int argc, char** argv) {
  MPI_Init(&argc, &argv);

  int rank = 0;
  int world_size = 0;

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  if (world_size < 2) {
    if (rank == 0) {
      std::cerr << "Run with at least 2 processes.\n";
    }
    MPI_Finalize();
    return 1;
  }

  const bool quiet_mode = has_flag(argc, argv, "--quiet") || has_flag(argc, argv, "--non-verbose");
  const bool persistence_enabled = !has_flag(argc, argv, "--disable-persistence");
  const bool snapshot_enabled = resolve_component_persistence(
      argc, argv, persistence_enabled, "--enable-snapshot", "--disable-snapshot");
  const bool wal_enabled = resolve_component_persistence(
      argc, argv, persistence_enabled, "--enable-wal", "--disable-wal");
  const uint64_t snapshot_interval_seconds = parse_snapshot_interval_seconds(argc, argv);

  try {
    if (rank == 0) {
      run_socket_gateway(argc, argv, world_size, !quiet_mode);
    } else {
      run_node_server(rank, !quiet_mode, snapshot_enabled, wal_enabled, snapshot_interval_seconds);
    }
  } catch (const std::exception& ex) {
    std::cerr << "[Error] rank=" << rank << " message=" << ex.what() << "\n";
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  MPI_Finalize();
  return 0;
}
