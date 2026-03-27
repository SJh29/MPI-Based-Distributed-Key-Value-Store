#include "node/node_server.h"

#include <iostream>
#include <string>

#include <mpi.h>

#include "common/config.h"
#include "common/mpi_comm.h"
#include "common/protocol.h"
#include "node/replication.h"
#include "node/storage.h"

using namespace mpi_comm;

void run_node_server(int rank,
                     bool verbose,
                     bool snapshot_enabled,
                     bool wal_enabled,
                     uint64_t snapshot_interval_seconds) {
  int world_size = 0;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  const std::string base_path = "node_" + std::to_string(rank);
  const std::string snapshot_path = snapshot_enabled ? base_path + ".snap" : "";
  const std::string wal_path = wal_enabled ? base_path + ".snap.wal" : "";
  Storage storage(snapshot_path, wal_path, snapshot_interval_seconds);

  if ((snapshot_enabled || wal_enabled) && !storage.load_snapshot() && verbose) {
    std::cerr << "[Node " << rank << "] failed to load snapshot: " << snapshot_path << "\n";
  }

  if (verbose) {
    std::cout << "[Node " << rank << "] started.";
    std::cout << " snapshot=" << (snapshot_enabled ? snapshot_path : "disabled");
    std::cout << " wal=" << (wal_enabled ? wal_path : "disabled");
    std::cout << " snapshot_interval_sec=" << snapshot_interval_seconds;
    std::cout << " repl_factor=" << DEFAULT_REPLICATION_FACTOR;
    std::cout << "\n";
  }

  while (true) {
    Message msg{};
    recv_any(msg);

    if (msg.type == MsgType::SHUTDOWN) {
      if (verbose) {
        std::cout << "[Node " << rank << "] shutting down.\n";
      }
      break;
    }

    const std::string key(key_view(msg));

    switch (msg.type) {
      case MsgType::PUT_REQ: {
        const std::string value(value_view(msg));
        const uint64_t version = storage.put(key, value);
        if (snapshot_enabled && !storage.save_snapshot() && verbose) {
          std::cerr << "[Node " << rank << "] failed to save snapshot after PUT\n";
        }

        const int primary_rank = (msg.primary_rank > 0) ? msg.primary_rank : rank;
        if (rank == primary_rank) {
          Message repl_source = msg;
          repl_source.primary_rank = primary_rank;
          replicate_put(repl_source,
                        key,
                        value,
                        version,
                        world_size,
                        DEFAULT_REPLICATION_FACTOR,
                        verbose);
        }

        if (verbose) {
          std::cout << "[Node " << rank << "] PUT key=" << key << " value=" << value << "\n";
        }
        send_msg(msg.client_rank,
                 make_resp(msg.client_rank, msg.request_id, Status::OK, key, value, version));
        break;
      }
      case MsgType::GET_REQ: {
        VersionedValue existing;
        if (verbose) {
          std::cout << "[Node " << rank << "] GET key=" << key << "\n";
        }
        if (storage.get(key, existing)) {
          send_msg(msg.client_rank,
                   make_resp(msg.client_rank, msg.request_id, Status::OK, key,
                             existing.value, existing.version));
        } else {
          send_msg(msg.client_rank,
                   make_resp(msg.client_rank, msg.request_id, Status::NOT_FOUND, key, "", 0));
        }
        break;
      }
      case MsgType::DEL_REQ: {
        uint64_t deleted_version = 0;
        if (verbose) {
          std::cout << "[Node " << rank << "] DEL key=" << key << "\n";
        }
        const bool deleted = storage.del(key, &deleted_version);
        if (snapshot_enabled && deleted && !storage.save_snapshot() && verbose) {
          std::cerr << "[Node " << rank << "] failed to save snapshot after DEL\n";
        }

        const int primary_rank = (msg.primary_rank > 0) ? msg.primary_rank : rank;
        if (deleted && rank == primary_rank) {
          Message repl_source = msg;
          repl_source.primary_rank = primary_rank;
          replicate_del(repl_source,
                        key,
                        deleted_version,
                        world_size,
                        DEFAULT_REPLICATION_FACTOR,
                        verbose);
        }

        send_msg(msg.client_rank,
                 make_resp(msg.client_rank, msg.request_id,
                           deleted ? Status::OK : Status::NOT_FOUND,
                           key,
                           "",
                           deleted ? deleted_version : 0));
        break;
      }
      case MsgType::REPL_PUT: {
        const std::string value(value_view(msg));
        storage.put(key, value);
        if (snapshot_enabled && !storage.save_snapshot() && verbose) {
          std::cerr << "[Node " << rank << "] failed to save snapshot after REPL_PUT\n";
        }
        if (verbose) {
          std::cout << "[Node " << rank << "] REPL_PUT key=" << key
                    << " from primary=" << msg.primary_rank
                    << " replica_index=" << msg.replica_index << "\n";
        }
        break;
      }
      case MsgType::REPL_DEL: {
        uint64_t ignored = 0;
        storage.del(key, &ignored);
        if (snapshot_enabled && !storage.save_snapshot() && verbose) {
          std::cerr << "[Node " << rank << "] failed to save snapshot after REPL_DEL\n";
        }
        if (verbose) {
          std::cout << "[Node " << rank << "] REPL_DEL key=" << key
                    << " from primary=" << msg.primary_rank
                    << " replica_index=" << msg.replica_index << "\n";
        }
        break;
      }
      default:
        send_msg(msg.client_rank,
                 make_resp(msg.client_rank, msg.request_id, Status::INVALID, key, "", 0));
        break;
    }
  }
}
