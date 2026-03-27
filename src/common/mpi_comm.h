#pragma once

#include <mpi.h>
#include <stdexcept>
#include <string>
#include <vector>

#include "protocol.h"

// A small helper wrapper around MPI send/recv for our fixed-size Message.
// We send Messages as raw bytes (MPI_BYTE) to avoid MPI datatype/padding issues.
namespace mpi_comm {

  // Use a fixed tag for simplicity. You can expand to multiple tags later if needed.
  constexpr int MSG_TAG = 1001;

  // Throwing error helper
  void mpi_check(int err, const char* where);

  // Blocking send to a specific destination rank.
  void send_msg(int dst_rank, const Message& msg, int tag = MSG_TAG, MPI_Comm comm = MPI_COMM_WORLD);

  // Non-blocking send to a specific destination rank.
  MPI_Request isend_msg(int dst_rank, const Message& msg, int tag = MSG_TAG, MPI_Comm comm = MPI_COMM_WORLD);

  // Blocking receive from a specific source rank.
  void recv_msg(int src_rank, Message& out, int tag = MSG_TAG, MPI_Comm comm = MPI_COMM_WORLD);

  // Non-blocking receive from a specific source rank.
  MPI_Request irecv_msg(int src_rank, Message& out, int tag = MSG_TAG, MPI_Comm comm = MPI_COMM_WORLD);

  // Blocking receive from ANY source (and optional ANY tag).
  // Returns the MPI_Status so caller can see source/tag if needed.
  MPI_Status recv_any(Message& out, int tag = MSG_TAG, MPI_Comm comm = MPI_COMM_WORLD);

  // Non-blocking wait for any request in a request vector. Returns MPI_UNDEFINED if none are active.
  int wait_any(std::vector<MPI_Request>& requests, MPI_Status& out_status);

  // Non-blocking test for any completed request in a request vector.
  // Returns true when one completed and sets out_index/out_status.
  bool test_any(std::vector<MPI_Request>& requests, int& out_index, MPI_Status& out_status);

  // Non-blocking probe: checks if there is a message available.
  // If available, sets out_status and returns true; otherwise false.
  bool probe_any(MPI_Status& out_status, int tag = MSG_TAG, MPI_Comm comm = MPI_COMM_WORLD);

  // Convenience: probe and then recv (blocking) only if available.
  // Returns true if a message was received, false if none.
  bool try_recv_any(Message& out, MPI_Status& out_status, int tag = MSG_TAG, MPI_Comm comm = MPI_COMM_WORLD);
} // namespace mpi_comm
