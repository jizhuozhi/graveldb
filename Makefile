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

# ─── Phony targets ───────────────────────────────────────────

.PHONY: all release debug build test bench bench-net bench-latency \
        server clean format lint \
        linux linux-all linux-arm64 linux-amd64 linux-matrix \
        linux-bench linux-shell \
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
	@echo "Linux Verification (Docker):"
	@echo "  make linux            Default distro (Ubuntu 22.04, native arch)"
	@echo "  make linux-all        All distros (native arch)"
	@echo "  make linux-amd64      All distros, amd64 (QEMU on ARM hosts)"
	@echo "  make linux-arm64      All distros, arm64 (QEMU on x86 hosts)"
	@echo "  make linux-matrix     Full matrix: all distros × {amd64, arm64}"
	@echo "  make linux-bench      Benchmark in Docker (default distro)"
	@echo "  make linux-shell      Interactive shell in Docker"
	@echo ""
	@echo "  Distros: Ubuntu 22.04/24.04, Debian 11/12, Fedora 39/40,"
	@echo "           Amazon Linux 2023, Alpine 3.19/3.20"
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
	@echo "═══ Cache Benchmark ═══"
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
# Multi-distro, multi-arch build/test matrix.
# Tests:
#   - Linux-specific code paths (epoll, io_uring)
#   - No macOS-ism leaking into portable code
#   - Correct linking against liburing (when available)
#   - Fallback when liburing is NOT available (e.g. Alpine, older distros)
#   - Cross-architecture (arm64 via QEMU on x86 hosts, or native)
#
# Targets:
#   make linux              - default: Ubuntu 22.04 (native arch)
#   make linux-all          - all distros, native arch
#   make linux-arm64        - all distros, arm64 (cross via QEMU)
#   make linux-amd64        - all distros, amd64 (cross via QEMU on ARM hosts)
#   make linux-matrix       - full matrix: all distros × {amd64, arm64}
#   make linux-bench        - benchmark in Ubuntu 22.04
#   make linux-shell        - interactive shell
#   make linux-shell DOCKER_IMAGE=amazonlinux:2023

# Docker image for default single-distro targets
DOCKER_IMAGE  ?= ubuntu:22.04
DOCKER_NAME   ?= graveldb-linux-build

# ── Distro lists ──

DISTROS_APT   := ubuntu:22.04 ubuntu:24.04 debian:12 debian:11
DISTROS_DNF   := fedora:40 fedora:39 amazonlinux:2023
DISTROS_APK   := alpine:3.20 alpine:3.19

.PHONY: linux linux-all linux-arm64 linux-amd64 linux-matrix \
        linux-bench linux-shell _ensure-buildx

# Default: single distro, native arch
linux:
	@echo "Building and testing in Docker ($(DOCKER_IMAGE))..."
	@docker run --rm \
		-v "$(CURDIR)":/workspace:ro \
		$(DOCKER_IMAGE) \
		sh /workspace/scripts/docker-test.sh

linux-bench:
	@echo "Benchmarking in Docker ($(DOCKER_IMAGE))..."
	@docker run --rm \
		-v "$(CURDIR)":/workspace:ro \
		$(DOCKER_IMAGE) \
		sh /workspace/scripts/docker-bench.sh

linux-shell:
	@docker run --rm -it \
		-v "$(CURDIR)":/workspace \
		-w /workspace \
		$(DOCKER_IMAGE) \
		bash || sh

# ── All distros, native arch ──

linux-all: _ensure-buildx
	@echo "Testing all distros (native arch)..."
	@FAIL=0; \
	for img in $(DISTROS_APT) $(DISTROS_DNF) $(DISTROS_APK); do \
		echo ""; echo "═══ $$img ═══"; \
		docker run --rm \
			-v "$(CURDIR)":/workspace:ro \
			$$img \
			sh /workspace/scripts/docker-test.sh \
		|| FAIL=$$((FAIL+1)); \
	done; \
	echo ""; \
	if [ $$FAIL -ne 0 ]; then echo "✗ $$FAIL distro(s) FAILED"; exit 1; \
	else echo "✓ All distros passed"; fi

# ── Cross-architecture (arm64) ──

_ensure-buildx:
	@docker buildx inspect --builder default > /dev/null 2>&1 || true
	@docker run --rm --privileged multiarch/qemu-user-static --reset -p yes > /dev/null 2>&1 || true

linux-arm64: _ensure-buildx
	@echo "Testing all distros (linux/arm64 via QEMU)..."
	@FAIL=0; \
	for img in $(DISTROS_APT) $(DISTROS_DNF) $(DISTROS_APK); do \
		echo ""; echo "═══ $$img [arm64] ═══"; \
		docker run --rm --platform linux/arm64 \
			-v "$(CURDIR)":/workspace:ro \
			$$img \
			sh /workspace/scripts/docker-test.sh \
		|| FAIL=$$((FAIL+1)); \
	done; \
	echo ""; \
	if [ $$FAIL -ne 0 ]; then echo "✗ $$FAIL distro(s) FAILED [arm64]"; exit 1; \
	else echo "✓ All distros passed [arm64]"; fi

# ── Cross-architecture (amd64) — useful on ARM Mac ──

linux-amd64: _ensure-buildx
	@echo "Testing all distros (linux/amd64 via QEMU)..."
	@FAIL=0; \
	for img in $(DISTROS_APT) $(DISTROS_DNF) $(DISTROS_APK); do \
		echo ""; echo "═══ $$img [amd64] ═══"; \
		docker run --rm --platform linux/amd64 \
			-v "$(CURDIR)":/workspace:ro \
			$$img \
			sh /workspace/scripts/docker-test.sh \
		|| FAIL=$$((FAIL+1)); \
	done; \
	echo ""; \
	if [ $$FAIL -ne 0 ]; then echo "✗ $$FAIL distro(s) FAILED [amd64]"; exit 1; \
	else echo "✓ All distros passed [amd64]"; fi

# ── Full matrix: all distros × {amd64, arm64} ──

linux-matrix:
	@echo "Running full distro×arch matrix..."
	@$(MAKE) linux-amd64
	@$(MAKE) linux-arm64
	@echo ""
	@echo "✓ Full matrix complete (amd64 + arm64)"
