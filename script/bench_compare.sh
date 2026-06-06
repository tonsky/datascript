#!/bin/bash
# Runs CLJ, CLJS, and CLJD benchmarks and prints a side-by-side comparison table.
# Usage: ./script/bench_compare.sh [bench-names...]
cd "`dirname $0`/.."

TMP=$(mktemp -d)
trap "rm -rf $TMP" EXIT

run_bench() {
  local platform="$1"
  shift
  local out="$TMP/$platform.txt"
  echo "=== $platform ===" >&2
  if ./script/bench_${platform}.sh "$@" >"$out" 2>&1; then
    cat "$out" >&2
  else
    echo "(failed)" >&2
    : >"$out"
  fi
  echo "" >&2
}

run_bench clj  "$@"
run_bench cljs "$@"
run_bench cljd "$@"

python3 - "$TMP/clj.txt" "$TMP/cljs.txt" "$TMP/cljd.txt" <<'PYTHON'
import sys, re

def parse(path):
    results = {}
    try:
        with open(path) as f:
            for line in f:
                m = re.match(r'^(\S+)\s+([\d.]+)\s+ms/op', line.strip())
                if m:
                    results[m.group(1)] = float(m.group(2))
    except OSError:
        pass
    return results

clj  = parse(sys.argv[1])
cljs = parse(sys.argv[2])
cljd = parse(sys.argv[3])

all_keys = sorted(set(clj) | set(cljs) | set(cljd))
if not all_keys:
    print("No benchmark results found.")
    sys.exit(0)

NW = max(max(len(k) for k in all_keys), 9)

def fmt_ms(v):
    if v is None:
        return "N/A"
    if v > 1:
        return f"{v:.1f}ms"
    if v > 0.01:
        return f"{v:.3f}ms"
    return f"{v:.7f}ms"

def fmt_ratio(a, b):
    if a is None or b is None:
        return "N/A"
    r = a / b
    return f"{r:.2f}x"

cols = [("CLJ", clj), ("CLJS", cljs), ("CLJD", cljd)]
present = [(name, data) for name, data in cols if data]

header_parts = [f"{'Benchmark':<{NW}}"]
for name, _ in present:
    header_parts.append(f"{name:>10}")
if len(present) > 1:
    for i in range(1, len(present)):
        label = f"{present[i][0]}/{present[0][0]}"
        header_parts.append(f"{label:>10}")

header = "  ".join(header_parts)
print(header)
print("-" * len(header))

for k in all_keys:
    row = [f"{k:<{NW}}"]
    vals = [data.get(k) for _, data in present]
    for v in vals:
        row.append(f"{fmt_ms(v):>10}")
    if len(present) > 1:
        base = vals[0]
        for v in vals[1:]:
            row.append(f"{fmt_ratio(v, base):>10}")
    print("  ".join(row))
PYTHON
