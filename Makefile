CXX = mpic++
CXXFLAGS = -std=c++17 -O2 -Wall -Wextra -Isrc -DOMPI_SKIP_MPICXX -DMPICH_SKIP_MPICXX

COMMON_SRC = \
	src/common/mpi_comm.cpp \
	src/node/node_server.cpp \
	src/node/storage.cpp \
	src/node/replication.cpp \
	src/router/router.cpp

KVSTORE_SRC = src/main.cpp $(COMMON_SRC)

TARGET = kvstore

all: $(TARGET)

$(TARGET): $(KVSTORE_SRC)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(KVSTORE_SRC)

clean:
	rm -f $(TARGET)
	rm -f *.snap *.snap.wal

run:
	mpirun --oversubscribe -np 4 ./$(TARGET)
