#include "mpi_comm.h"

namespace mpi_comm {
void mpi_check(int err, const char* where) {
  if (err == MPI_SUCCESS) return;

  char err_str[MPI_MAX_ERROR_STRING];
  int len = 0;
  MPI_Error_string(err, err_str, &len);

  std::string msg = std::string("MPI error at ") + where + ": " +
                    std::string(err_str, (len > 0 ? len : 0));
  throw std::runtime_error(msg);
}

void send_msg(int dst_rank, const Message& msg, int tag, MPI_Comm comm) {
  const int err = MPI_Send((void*)&msg, MSG_BYTES, MPI_BYTE, dst_rank, tag, comm);
  mpi_check(err, "MPI_Send");
}

MPI_Request isend_msg(int dst_rank, const Message& msg, int tag, MPI_Comm comm) {
  MPI_Request request = MPI_REQUEST_NULL;
  const int err = MPI_Isend((void*)&msg, MSG_BYTES, MPI_BYTE, dst_rank, tag, comm, &request);
  mpi_check(err, "MPI_Isend");
  return request;
}

void recv_msg(int src_rank, Message& out, int tag, MPI_Comm comm) {
  MPI_Status status{};
  const int err = MPI_Recv((void*)&out, MSG_BYTES, MPI_BYTE, src_rank, tag, comm, &status);
  mpi_check(err, "MPI_Recv");
}

MPI_Request irecv_msg(int src_rank, Message& out, int tag, MPI_Comm comm) {
  MPI_Request request = MPI_REQUEST_NULL;
  const int err = MPI_Irecv((void*)&out, MSG_BYTES, MPI_BYTE, src_rank, tag, comm, &request);
  mpi_check(err, "MPI_Irecv");
  return request;
}

MPI_Status recv_any(Message& out, int tag, MPI_Comm comm) {
  MPI_Status status{};
  const int real_tag = (tag < 0) ? MPI_ANY_TAG : tag;

  const int err = MPI_Recv((void*)&out, MSG_BYTES, MPI_BYTE, MPI_ANY_SOURCE,
                           real_tag, comm, &status);
  mpi_check(err, "MPI_Recv(any)");
  return status;
}

int wait_any(std::vector<MPI_Request>& requests, MPI_Status& out_status) {
  int index = MPI_UNDEFINED;
  const int err = MPI_Waitany(static_cast<int>(requests.size()), requests.data(), &index, &out_status);
  mpi_check(err, "MPI_Waitany");
  return index;
}

bool test_any(std::vector<MPI_Request>& requests, int& out_index, MPI_Status& out_status) {
  int flag = 0;
  out_index = MPI_UNDEFINED;
  const int err = MPI_Testany(static_cast<int>(requests.size()), requests.data(), &out_index, &flag, &out_status);
  mpi_check(err, "MPI_Testany");
  return flag != 0 && out_index != MPI_UNDEFINED;
}

bool probe_any(MPI_Status& out_status, int tag, MPI_Comm comm) {
  int flag = 0;
  MPI_Status status{};
  const int real_tag = (tag < 0) ? MPI_ANY_TAG : tag;

  const int err = MPI_Iprobe(MPI_ANY_SOURCE, real_tag, comm, &flag, &status);
  mpi_check(err, "MPI_Iprobe");
  if (flag) out_status = status;
  return flag != 0;
}

bool try_recv_any(Message& out, MPI_Status& out_status, int tag, MPI_Comm comm) {
  MPI_Status st{};
  if (!probe_any(st, tag, comm)) return false;
  out_status = recv_any(out, tag, comm);
  return true;
}
}  // namespace mpi_comm
