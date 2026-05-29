#!/bin/sh
# GravelDB - Docker build & test script
#
# Auto-detects the distro and installs appropriate build dependencies.
# Works across: Ubuntu, Debian, Fedora, Amazon Linux, Alpine.
#
# Usage (inside container):
#   sh /workspace/scripts/docker-test.sh
#   sh /workspace/scripts/docker-test.sh bench   # also run benchmarks

set -e

MODE="${1:-test}"  # "test" or "bench"

# ── Detect distro and install deps ──

install_deps() {
    if command -v apt-get > /dev/null 2>&1; then
        # Debian/Ubuntu
        apt-get update -qq
        apt-get install -y -qq build-essential cmake > /dev/null 2>&1
        apt-get install -y -qq liburing-dev > /dev/null 2>&1 || true
    elif command -v dnf > /dev/null 2>&1; then
        # Fedora / Amazon Linux 2023+
        dnf install -y -q gcc gcc-c++ make cmake > /dev/null 2>&1
        dnf install -y -q liburing-devel > /dev/null 2>&1 || true
    elif command -v yum > /dev/null 2>&1; then
        # Amazon Linux 2 / CentOS 7
        yum install -y -q gcc gcc-c++ make > /dev/null 2>&1
        # cmake3 on older distros
        if ! command -v cmake > /dev/null 2>&1; then
            yum install -y -q cmake3 > /dev/null 2>&1
            ln -sf /usr/bin/cmake3 /usr/local/bin/cmake 2>/dev/null || true
        else
            yum install -y -q cmake > /dev/null 2>&1 || true
        fi
        yum install -y -q liburing-devel > /dev/null 2>&1 || true
    elif command -v apk > /dev/null 2>&1; then
        # Alpine (musl libc, no io_uring typically)
        apk add --no-cache build-base cmake linux-headers > /dev/null 2>&1
    else
        echo "ERROR: Unknown package manager. Cannot install deps."
        exit 1
    fi
}

# ── Print environment info ──

print_env() {
    echo "── Environment ──"
    echo "OS: $(cat /etc/os-release 2>/dev/null | grep PRETTY_NAME | cut -d= -f2 | tr -d '"' || uname -s)"
    echo "Kernel: $(uname -r)"
    echo "Arch: $(uname -m)"
    if command -v gcc > /dev/null 2>&1; then
        echo "GCC: $(gcc --version | head -1)"
    elif command -v cc > /dev/null 2>&1; then
        echo "CC: $(cc --version | head -1)"
    fi
    echo ""
}

# ── Copy source to writable location ──

setup_workspace() {
    cp -r /workspace /tmp/graveldb
    cd /tmp/graveldb
    rm -rf build
    mkdir build
    cd build
}

# ── Build ──

do_build() {
    echo "── Building ──"
    cmake .. -DCMAKE_BUILD_TYPE=Release 2>&1 | grep -E "^--|Found|not found|STATUS" || true
    NPROC=$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)
    make -j"$NPROC" 2>&1 | tail -5
    echo ""
}

# ── Test ──

do_test() {
    echo "── Running tests ──"
    NPROC=$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)
    ctest --output-on-failure -j"$NPROC"
    echo ""
}

# ── Bench ──

do_bench() {
    echo "── Running network benchmark (small) ──"
    MD_SCALE=small RO_SCALE=small BENCH_SAMPLES=100 BENCH_MIXED_OPS=100 \
        ./bench-network
    echo ""
    echo "── Running latency benchmark ──"
    ./bench-latency
    echo ""
}

# ── Main ──

install_deps
print_env
setup_workspace
do_build
do_test

if [ "$MODE" = "bench" ]; then
    do_bench
fi

echo "✓ Done ($(cat /etc/os-release 2>/dev/null | grep PRETTY_NAME | cut -d= -f2 | tr -d '"' || uname -s) / $(uname -m))"
