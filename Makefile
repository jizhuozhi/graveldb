# GravelDB - Top-level Makefile
#
# Provides convenience targets wrapping CMake.
# Works on both macOS (dev) and Linux (CI / io_uring).
#
# Usage:
#   make              # build everything (debug)
#   make release      # build with Release config
#   make test         # run unit tests
#   make bench        # run all benchmarks
#   make bench-net    # run network benchmark only
#   make server       # start dev server (foreground)
#   make clean        # rm build dir
#   make linux        # build + test inside Docker (Ubuntu 22.04)
#   make linux-shell  # interactive shell in the Docker container

# ─── Config ───────────────────────────────────────────────────

BUILD_DIR     ?= build
CMAKE         ?= cmake
NPROC         := $(shell nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
BUILD_TYPE    ?= Debug

# Docker image for Linux verification
DOCKER_IMAGE  ?= ubuntu:22.04
DOCKER_NAME   ?= graveldb-linux-build

# ─── Phony targets ───────────────────────────────────────────

.PHONY: all release debug build test bench bench-net bench-latency \
        server clean format lint \
        linux linux-shell docker-build \
        help

all: build

help:
	@echo "GravelDB Build System"
	@echo ""
	@echo "Build:"
	@echo "  make              Build (debug, default)"
	@echo "  make release      Build (release, -O2)"
	@echo "  make clean        Remove build directory"
	@echo ""
	@echo "Test & Bench:"
	@echo "  make test         Run all unit tests (ctest)"
	@echo "  make bench        Run all benchmarks"
	@echo "  make bench-net    Run network (client/server) benchmark"
	@echo "  make bench-lat    Run latency benchmark"
	@echo ""
	@echo "Server:"
	@echo "  make server       Start graveldb-server (foreground, default config)"
	@echo "  make server PORT=9600 DIR=/tmp/data DIMS=64,128"
	@echo ""
	@echo "Linux Verification:"
	@echo "  make linux        Build + test in Docker (Ubuntu 22.04)"
	@echo "  make linux-bench  Build + run benchmarks in Docker"
	@echo "  make linux-shell  Interactive shell in Docker container"
	@echo ""

# ─── Build ────────────────────────────────────────────────────

$(BUILD_DIR)/Makefile:
	@mkdir -p $(BUILD_DIR)
	@cd $(BUILD_DIR) && $(CMAKE) .. -DCMAKE_BUILD_TYPE=$(BUILD_TYPE)

build: $(BUILD_DIR)/Makefile
	@$(CMAKE) --build $(BUILD_DIR) -j$(NPROC)

debug:
	@$(MAKE) build BUILD_TYPE=Debug

release:
	@$(MAKE) build BUILD_TYPE=Release

clean:
	@rm -rf $(BUILD_DIR)
	@echo "Cleaned."

# ─── Test ─────────────────────────────────────────────────────

test: build
	@cd $(BUILD_DIR) && ctest --output-on-failure -j$(NPROC)

# ─── Benchmarks ──────────────────────────────────────────────

bench: build
	@echo "═══ Multi-Dim Benchmark ═══"
	@$(BUILD_DIR)/bench-main
	@echo ""
	@echo "═══ Hash Index Benchmark ═══"
	@$(BUILD_DIR)/bench-hash-index
	@echo ""
	@echo "═══ Cache (TinyLFU) Benchmark ═══"
	@$(BUILD_DIR)/bench-cache
	@echo ""
	@echo "═══ Slab Allocator Benchmark ═══"
	@$(BUILD_DIR)/bench-slab-alloc
	@echo ""
	@echo "═══ Latency Benchmark ═══"
	@$(BUILD_DIR)/bench-latency
	@echo ""
	@echo "═══ Write Buffer Benchmark ═══"
	@$(BUILD_DIR)/bench-write-buffer
	@echo ""
	@echo "═══ Memory Scaling Benchmark ═══"
	@$(BUILD_DIR)/bench-memory-scaling
	@echo ""
	@echo "═══ Network Benchmark ═══"
	@$(BUILD_DIR)/bench-network

bench-net: build
	@$(BUILD_DIR)/bench-network

bench-lat: build
	@$(BUILD_DIR)/bench-latency

# ─── Server ──────────────────────────────────────────────────

PORT ?= 9527
DIR  ?= ./graveldb_data
DIMS ?= 32,64,128

server: build
	@$(BUILD_DIR)/graveldb-server -p $(PORT) -d $(DIR) -D $(DIMS)

# ─── Linux Verification (Docker) ─────────────────────────────
#
# Builds and tests inside a fresh Ubuntu container to verify:
#   - Linux-specific code paths (epoll, io_uring)
#   - No macOS-ism leaking into portable code
#   - Correct linking against liburing (if available)

define DOCKER_SCRIPT
set -e
apt-get update -qq
apt-get install -y -qq build-essential cmake liburing-dev > /dev/null 2>&1
echo "── Environment ──"
uname -a
gcc --version | head -1
echo ""
cd /workspace
rm -rf build && mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$$(nproc)
echo ""
echo "── Running tests ──"
ctest --output-on-failure -j$$(nproc)
endef
export DOCKER_SCRIPT

define DOCKER_BENCH_SCRIPT
set -e
apt-get update -qq
apt-get install -y -qq build-essential cmake liburing-dev > /dev/null 2>&1
cd /workspace
rm -rf build && mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$$(nproc)
echo ""
echo "── Running network benchmark ──"
./bench-network
echo ""
echo "── Running latency benchmark ──"
./bench-latency
endef
export DOCKER_BENCH_SCRIPT

linux:
	@echo "Building and testing in Docker ($(DOCKER_IMAGE))..."
	@docker run --rm \
		-v "$(CURDIR)":/workspace:ro \
		-w /workspace \
		--tmpfs /workspace/build:exec \
		$(DOCKER_IMAGE) \
		bash -c "cp -r /workspace /tmp/graveldb && cd /tmp/graveldb && $$DOCKER_SCRIPT"

linux-bench:
	@echo "Building and benchmarking in Docker ($(DOCKER_IMAGE))..."
	@docker run --rm \
		-v "$(CURDIR)":/workspace:ro \
		-w /workspace \
		--tmpfs /workspace/build:exec \
		$(DOCKER_IMAGE) \
		bash -c "cp -r /workspace /tmp/graveldb && cd /tmp/graveldb && $$DOCKER_BENCH_SCRIPT"

linux-shell:
	@docker run --rm -it \
		-v "$(CURDIR)":/workspace \
		-w /workspace \
		$(DOCKER_IMAGE) \
		bash
