# Distributed KV Store (MPI + Socket Gateway)

This project is a distributed key-value store built on MPI.

## Architecture

- **Rank 0** runs a production-facing TCP gateway with a **single listener** (default port `7100`) and a framed binary request protocol.
- **Ranks 1..N-1** run storage nodes with local persistence (snapshot + WAL) and replication.
- Keys are sharded by hash to a primary node and replicated to additional replicas.

## Build

```bash
make
```

## Run

```bash
mpirun --oversubscribe -np 4 ./kvstore
```

Useful flags:

- `--bind-address <ipv4>` (default: `0.0.0.0`)
- `--gateway-port <port>` (default: `7100`)
- `--disable-persistence`
- `--enable-snapshot` / `--disable-snapshot`
- `--enable-wal` / `--disable-wal`
- `--snapshot-interval-sec <seconds>`
- `--quiet`

## Socket Protocol

The gateway accepts one request per TCP connection on `--gateway-port`.

Each request is framed as:

```text
[4 bytes message length in network byte order][Message Body]
```

`Message Body` format:

```text
[client_id:u32][op:u8][key_len:u16][value_len:u16][key bytes][value bytes]
```

Where:

- `client_id`: arbitrary client-generated identifier (logged by gateway)
- `op`: `1=PUT`, `2=GET`, `3=DEL`
- `key_len`: key size in bytes
- `value_len`: value size in bytes (must be `0` for GET/DEL)
- `key bytes`: raw key bytes
- `value bytes`: raw value bytes (PUT only)

Special control key:

- `__shutdown__` on DEL (`op=3`) tells rank 0 to send MPI shutdown messages to all storage nodes.

### Response format (all operations)

```text
status=<status_code> request_id=<id> version=<version> [value=<value>]\n
```

Status codes follow `Status` enum in `src/common/protocol.h`:

- `0`: OK
- `1`: NOT_FOUND
- `2`: ERROR
- `3`: INVALID

## Example usage

```bash
# Python example client for PUT/GET/DEL over framed binary protocol
python3 - <<'PY'
import socket
import struct

HOST, PORT = "127.0.0.1", 7100

def send_req(client_id: int, op: int, key: bytes, value: bytes = b""):
    body = (
        struct.pack("!I", client_id)
        + struct.pack("!B", op)
        + struct.pack("!H", len(key))
        + struct.pack("!H", len(value))
        + key
        + value
    )
    frame = struct.pack("!I", len(body)) + body

    with socket.create_connection((HOST, PORT)) as s:
        s.sendall(frame)
        print(s.recv(4096).decode("utf-8"), end="")

# PUT key=hello value=world
send_req(client_id=1001, op=1, key=b"hello", value=b"world")
# GET key=hello
send_req(client_id=1002, op=2, key=b"hello")
# DEL key=hello
send_req(client_id=1003, op=3, key=b"hello")
# Graceful shutdown
send_req(client_id=9999, op=3, key=b"__shutdown__")
PY
```
