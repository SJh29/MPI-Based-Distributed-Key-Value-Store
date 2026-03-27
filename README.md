# Distributed KV Store (MPI + Socket Gateway)

This project is a distributed key-value store built on MPI.

## Architecture

- **Rank 0** runs a production-facing TCP gateway with a dedicated socket listener per operation:
  - `PUT` listener (default port `7101`)
  - `GET` listener (default port `7102`)
  - `DEL` listener (default port `7103`)
- **Ranks 1..N-1** run storage nodes with local persistence (snapshot + WAL) and replication.
- Keys are sharded by hash to a primary node and replicated to additional replicas.

## Build

```bash
make
```

## Run

```bash
mpirun -np 4 ./kvstore
```

Useful flags:

- `--bind-address <ipv4>` (default: `0.0.0.0`)
- `--put-port <port>` (default: `7101`)
- `--get-port <port>` (default: `7102`)
- `--del-port <port>` (default: `7103`)
- `--disable-persistence`
- `--enable-snapshot` / `--disable-snapshot`
- `--enable-wal` / `--disable-wal`
- `--snapshot-interval-sec <seconds>`
- `--quiet`

## Socket Protocol

Each operation uses its own TCP listener. Requests are single-line text payloads.

### PUT (`--put-port`)

Request format:

```text
<key>\t<value>\n
```

### GET (`--get-port`)

Request format:

```text
<key>\n
```

### DEL (`--del-port`)

Request format:

```text
<key>\n
```

Special control key:

- `__shutdown__` on DEL listener tells rank 0 to send MPI shutdown messages to all storage nodes.

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
# PUT
printf 'hello\tworld\n' | nc 127.0.0.1 7101

# GET
printf 'hello\n' | nc 127.0.0.1 7102

# DEL
printf 'hello\n' | nc 127.0.0.1 7103

# Graceful shutdown
printf '__shutdown__\n' | nc 127.0.0.1 7103
```
