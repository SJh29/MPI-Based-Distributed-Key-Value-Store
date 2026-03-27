#include <mpi.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
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

struct SocketOperationConfig {
  MsgType type;
  std::string name;
  int port;
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

std::string recv_line(int fd) {
  std::string line;
  char buffer[512];
  while (true) {
    const ssize_t n = recv(fd, buffer, sizeof(buffer), 0);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error("recv() failed: " + std::string(std::strerror(errno)));
    }

    if (n == 0) {
      break;
    }

    line.append(buffer, buffer + n);
    if (line.find('\n') != std::string::npos) {
      break;
    }

    if (line.size() > 2048) {
      throw std::runtime_error("request too large");
    }
  }

  const size_t newline = line.find('\n');
  if (newline != std::string::npos) {
    line.resize(newline);
  }
  if (!line.empty() && line.back() == '\r') {
    line.pop_back();
  }
  return line;
}

void send_text(int fd, const std::string& text) {
  size_t written = 0;
  while (written < text.size()) {
    const ssize_t n = send(fd, text.data() + written, text.size() - written, 0);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error("send() failed: " + std::string(std::strerror(errno)));
    }
    written += static_cast<size_t>(n);
  }
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
                           const SocketOperationConfig& operation,
                           int world_size,
                           int request_id,
                           bool verbose) {
  const std::string payload = recv_line(client_fd);

  std::string key;
  std::string value;

  if (operation.type == MsgType::PUT_REQ) {
    const size_t delimiter = payload.find('\t');
    if (delimiter == std::string::npos) {
      send_text(client_fd, "status=INVALID error=expected_key_tab_value\\n");
      return true;
    }
    key = payload.substr(0, delimiter);
    value = payload.substr(delimiter + 1);
  } else {
    key = payload;
  }

  if (key.empty()) {
    send_text(client_fd, "status=INVALID error=empty_key\\n");
    return true;
  }

  if (operation.type == MsgType::DEL_REQ && key == "__shutdown__") {
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
      make_operation_request(operation.type, request_id, std::string_view(key), std::string_view(value), primary_rank);
  send_msg(primary_rank, req);

  Message resp{};
  recv_msg(primary_rank, resp);

  std::string response =
      "status=" + std::to_string(static_cast<int>(resp.status)) +
      " request_id=" + std::to_string(resp.request_id) +
      " version=" + std::to_string(resp.version);
  if (resp.status == Status::OK && operation.type == MsgType::GET_REQ) {
    response += " value=" + std::string(value_view(resp));
  }
  response += "\\n";

  if (verbose) {
    std::cout << "[Gateway] op=" << operation.name
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

  std::vector<SocketOperationConfig> operations{
      {MsgType::PUT_REQ, "PUT", parse_int_arg(argc, argv, "--put-port", 7101)},
      {MsgType::GET_REQ, "GET", parse_int_arg(argc, argv, "--get-port", 7102)},
      {MsgType::DEL_REQ, "DEL", parse_int_arg(argc, argv, "--del-port", 7103)},
  };

  std::vector<int> listeners;
  listeners.reserve(operations.size());
  std::vector<pollfd> poll_fds;
  poll_fds.reserve(operations.size());

  try {
    for (const SocketOperationConfig& operation : operations) {
      const int listener_fd = open_listener(bind_address, operation.port);
      listeners.push_back(listener_fd);
      poll_fds.push_back(pollfd{listener_fd, POLLIN, 0});
      if (verbose) {
        std::cout << "[Gateway] Listening for " << operation.name
                  << " on " << bind_address << ":" << operation.port << "\n";
      }
    }

    int next_request_id = 1;
    bool running = true;
    while (running) {
      const int ready = poll(poll_fds.data(), poll_fds.size(), -1);
      if (ready < 0) {
        if (errno == EINTR) {
          continue;
        }
        throw std::runtime_error("poll() failed: " + std::string(std::strerror(errno)));
      }

      for (size_t i = 0; i < poll_fds.size(); ++i) {
        if ((poll_fds[i].revents & POLLIN) == 0) {
          continue;
        }

        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        const int client_fd = accept(poll_fds[i].fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
        if (client_fd < 0) {
          if (errno == EINTR) {
            continue;
          }
          throw std::runtime_error("accept() failed: " + std::string(std::strerror(errno)));
        }

        try {
          running = handle_socket_request(client_fd, operations[i], world_size, next_request_id++, verbose);
        } catch (const std::exception& ex) {
          send_text(client_fd, std::string("status=ERROR error=") + ex.what() + "\n");
        }
        close(client_fd);

        if (!running) {
          break;
        }
      }
    }
  } catch (...) {
    for (const int fd : listeners) {
      close(fd);
    }
    throw;
  }

  for (const int fd : listeners) {
    close(fd);
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
