/*
 * GravelDB - benchmark common helpers
 *
 * Header-only utilities so each bench is a single self-contained .c file.
 * Provides:
 *   - monotonic nanosecond timer
 *   - log-bucketed latency histogram (no third-party dep)
 *   - aligned text-table printing
 *   - tiny CLI parser
 *
 * Bench programs link only against graveldb_lib + pthread + m.
 * They are not compiled by default; enable with -DGRAVELDB_BUILD_BENCHMARKS=ON.
 */

#ifndef GRAVELDB_BENCH_COMMON_H_
#define GRAVELDB_BENCH_COMMON_H_

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdbool.h>
#include <time.h>
#include <math.h>

/* ---- timing ---- */

static inline uint64_t bench_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* ---- latency histogram ----
 *
 * Log-linear buckets: we cover [1ns, ~17 minutes] with ~1% relative error.
 * Each power-of-two range is split into BENCH_HIST_SUBBUCKETS sub-buckets.
 * Total buckets = 40 powers * 64 subs = 2560. ~20KB. Cheap.
 */

#define BENCH_HIST_POWERS       40
#define BENCH_HIST_SUBBUCKETS   64
#define BENCH_HIST_TOTAL        (BENCH_HIST_POWERS * BENCH_HIST_SUBBUCKETS)

typedef struct {
    uint64_t counts[BENCH_HIST_TOTAL];
    uint64_t total;
    uint64_t min_ns;
    uint64_t max_ns;
    uint64_t sum_ns;
} BenchHist;

static inline void bench_hist_init(BenchHist *h) {
    memset(h, 0, sizeof(*h));
    h->min_ns = UINT64_MAX;
}

static inline int bench_hist_bucket(uint64_t ns) {
    if (ns == 0) return 0;
    int p = 63 - __builtin_clzll(ns);  /* floor(log2(ns)) */
    if (p < 0) p = 0;
    if (p >= BENCH_HIST_POWERS) p = BENCH_HIST_POWERS - 1;
    uint64_t base = 1ull << p;
    uint64_t span = base; /* values in [base, 2*base) */
    int sub = (int)(((ns - base) * BENCH_HIST_SUBBUCKETS) / span);
    if (sub < 0) sub = 0;
    if (sub >= BENCH_HIST_SUBBUCKETS) sub = BENCH_HIST_SUBBUCKETS - 1;
    return p * BENCH_HIST_SUBBUCKETS + sub;
}

static inline void bench_hist_record(BenchHist *h, uint64_t ns) {
    h->counts[bench_hist_bucket(ns)]++;
    h->total++;
    h->sum_ns += ns;
    if (ns < h->min_ns) h->min_ns = ns;
    if (ns > h->max_ns) h->max_ns = ns;
}

static inline uint64_t bench_hist_value_at(int idx) {
    int p = idx / BENCH_HIST_SUBBUCKETS;
    int sub = idx % BENCH_HIST_SUBBUCKETS;
    uint64_t base = 1ull << p;
    return base + (base * (uint64_t)sub) / BENCH_HIST_SUBBUCKETS;
}

static inline uint64_t bench_hist_percentile(const BenchHist *h, double pct) {
    if (h->total == 0) return 0;
    uint64_t target = (uint64_t)((double)h->total * pct);
    if (target == 0) target = 1;
    uint64_t cum = 0;
    for (int i = 0; i < BENCH_HIST_TOTAL; i++) {
        cum += h->counts[i];
        if (cum >= target) return bench_hist_value_at(i);
    }
    return h->max_ns;
}

static inline double bench_hist_mean_ns(const BenchHist *h) {
    return h->total ? (double)h->sum_ns / (double)h->total : 0.0;
}

/* Merge src into dst. Used to fold per-thread histograms. */
static inline void bench_hist_merge(BenchHist *dst, const BenchHist *src) {
    for (int i = 0; i < BENCH_HIST_TOTAL; i++) dst->counts[i] += src->counts[i];
    dst->total += src->total;
    dst->sum_ns += src->sum_ns;
    if (src->min_ns < dst->min_ns) dst->min_ns = src->min_ns;
    if (src->max_ns > dst->max_ns) dst->max_ns = src->max_ns;
}

static inline void bench_print_latency_summary(const char *label, const BenchHist *h) {
    if (h->total == 0) {
        printf("  %-20s (no samples)\n", label);
        return;
    }
    printf("  %-20s n=%-10llu  mean=%8.2f us  min=%8.2f us  max=%8.2f us\n",
           label,
           (unsigned long long)h->total,
           bench_hist_mean_ns(h) / 1e3,
           (double)h->min_ns / 1e3,
           (double)h->max_ns / 1e3);
    printf("    p50=%7.2f  p90=%7.2f  p99=%7.2f  p99.9=%7.2f  p99.99=%7.2f us\n",
           (double)bench_hist_percentile(h, 0.50)   / 1e3,
           (double)bench_hist_percentile(h, 0.90)   / 1e3,
           (double)bench_hist_percentile(h, 0.99)   / 1e3,
           (double)bench_hist_percentile(h, 0.999)  / 1e3,
           (double)bench_hist_percentile(h, 0.9999) / 1e3);
}

/* ---- tiny CLI parser ---- */

static inline const char *bench_arg_str(int argc, char **argv,
                                        const char *flag, const char *def) {
    for (int i = 1; i + 1 < argc; i++) {
        if (strcmp(argv[i], flag) == 0) return argv[i + 1];
    }
    return def;
}

static inline long bench_arg_long(int argc, char **argv,
                                  const char *flag, long def) {
    const char *s = bench_arg_str(argc, argv, flag, NULL);
    return s ? strtol(s, NULL, 10) : def;
}

#endif /* GRAVELDB_BENCH_COMMON_H_ */
