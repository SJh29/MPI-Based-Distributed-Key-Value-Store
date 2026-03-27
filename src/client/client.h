#pragma once

#include <chrono>
#include <cstdint>
#include <mpi.h>
#include <string>
#include <string_view>
#include <vector>

#include "common/protocol.h"

enum class ClientCommandType {
  PUT,
  GET,
  DEL,
};

struct ClientCommand {
  ClientCommandType type{ClientCommandType::GET};
  std::string key;
  std::string value;
};

struct ClientResult {
  ClientCommand command;
  int target_rank{-1};
  int request_id{0};
  Status status{Status::ERROR};
  std::string value;
  uint64_t version{0};
  long long latency_us{0};
};

enum class ClientOperationMode {
  DIRECT_TO_PRIMARY,
  ROUTER_MEDIATED,
};

class KvClient {
 public:
  KvClient(int world_size,
           int client_rank = 0,
           int router_rank = 0,
           MPI_Comm comm = MPI_COMM_WORLD,
           ClientOperationMode mode = ClientOperationMode::DIRECT_TO_PRIMARY);

  ClientResult execute(const ClientCommand& command);
  std::vector<ClientResult> execute_many(const std::vector<ClientCommand>& commands);

  ClientResult put(std::string key, std::string value);
  ClientResult get(std::string key);
  ClientResult del(std::string key);

  void send_shutdown_to_nodes() const;

 private:
  Message build_request(const ClientCommand& command, int request_id, int target_rank) const;
  int resolve_target_rank(std::string_view key) const;

  int world_size_{0};
  int client_rank_{0};
  int router_rank_{0};
  MPI_Comm comm_{MPI_COMM_WORLD};
  ClientOperationMode mode_{ClientOperationMode::DIRECT_TO_PRIMARY};
  int next_request_id_{1};
};
