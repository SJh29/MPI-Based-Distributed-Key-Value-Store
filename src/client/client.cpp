#include "client/client.h"

#include <chrono>
#include <stdexcept>
#include <string_view>
#include <vector>

#include "common/mpi_comm.h"
#include "router/router.h"

using namespace mpi_comm;

KvClient::KvClient(int world_size,
                   int client_rank,
                   int router_rank,
                   MPI_Comm comm,
                   ClientOperationMode mode)
    : world_size_(world_size),
      client_rank_(client_rank),
      router_rank_(router_rank),
      comm_(comm),
      mode_(mode) {
  if (world_size_ < 2) {
    throw std::invalid_argument("world_size must be >= 2");
  }
  if (router_rank_ < 0 || router_rank_ >= world_size_) {
    throw std::invalid_argument("router_rank must be in [0, world_size)");
  }
}

int KvClient::resolve_target_rank(std::string_view key) const {
  if (mode_ == ClientOperationMode::ROUTER_MEDIATED) {
    return router_rank_;
  }

  const int target_rank = route_key_to_primary(key, world_size_);
  if (target_rank < 0) {
    throw std::runtime_error("failed to route key to primary");
  }
  return target_rank;
}

Message KvClient::build_request(const ClientCommand& command, int request_id, int target_rank) const {
  Message msg{};
  switch (command.type) {
    case ClientCommandType::PUT:
      msg = make_put_req(client_rank_, request_id, client_rank_, command.key, command.value);
      break;
    case ClientCommandType::GET:
      msg = make_get_req(client_rank_, request_id, client_rank_, command.key);
      break;
    case ClientCommandType::DEL:
      msg = make_del_req(client_rank_, request_id, client_rank_, command.key);
      break;
  }

  msg.primary_rank = target_rank;
  return msg;
}

ClientResult KvClient::execute(const ClientCommand& command) {
  const int target_rank = resolve_target_rank(std::string_view(command.key));
  const int request_id = next_request_id_++;
  const auto started_at = std::chrono::steady_clock::now();

  const Message req = build_request(command, request_id, target_rank);
  send_msg(target_rank, req, MSG_TAG, comm_);

  Message resp{};
  recv_msg(target_rank, resp, MSG_TAG, comm_);
  const auto ended_at = std::chrono::steady_clock::now();

  ClientResult result;
  result.command = command;
  result.target_rank = target_rank;
  result.request_id = request_id;
  result.status = resp.status;
  result.value = std::string(value_view(resp));
  result.version = resp.version;
  result.latency_us =
      std::chrono::duration_cast<std::chrono::microseconds>(ended_at - started_at).count();
  return result;
}

std::vector<ClientResult> KvClient::execute_many(const std::vector<ClientCommand>& commands) {
  std::vector<ClientResult> results;
  results.reserve(commands.size());
  for (const ClientCommand& command : commands) {
    results.push_back(execute(command));
  }
  return results;
}

ClientResult KvClient::put(std::string key, std::string value) {
  return execute(ClientCommand{ClientCommandType::PUT, std::move(key), std::move(value)});
}

ClientResult KvClient::get(std::string key) {
  return execute(ClientCommand{ClientCommandType::GET, std::move(key), ""});
}

ClientResult KvClient::del(std::string key) {
  return execute(ClientCommand{ClientCommandType::DEL, std::move(key), ""});
}

void KvClient::send_shutdown_to_nodes() const {
  if (mode_ == ClientOperationMode::ROUTER_MEDIATED) {
    send_msg(router_rank_, make_shutdown(), MSG_TAG, comm_);
    return;
  }

  for (int rank = 1; rank < world_size_; ++rank) {
    send_msg(rank, make_shutdown(), MSG_TAG, comm_);
  }
}
