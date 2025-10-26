from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt


def parse_metrics(path: Path) -> Dict[Tuple[int, str], dict]:
    """Read CSV and keep the last record for each (n, format)."""
    latest: Dict[Tuple[int, str], dict] = {}
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                n = int(r["n"]) if "n" in r else 0
                fmt = r["format"].strip().lower()
                latest[(n, fmt)] = {
                    "n": n,
                    "format": fmt,
                    "bytes": int(r["bytes"]),
                    "write_ms": float(r["write_ms"]),
                    "read_ms": float(r["read_ms"]),
                    "rows": int(r.get("rows", 0) or 0),
                }
            except Exception:
                # Skip malformed rows
                continue
    return latest


def build_series(latest: Dict[Tuple[int, str], dict], fmt: str, field: str) -> Tuple[List[int], List[float]]:
    keys = sorted({n for (n, f) in latest.keys() if f == fmt})
    xs: List[int] = []
    ys: List[float] = []
    for n in keys:
        rec = latest.get((n, fmt))
        if rec is None:
            continue
        xs.append(n)
        val = rec[field]
        if field == "bytes":
            ys.append(val / (1024 * 1024))  # to MB
        else:
            ys.append(float(val))
    return xs, ys


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot Parquet vs ORC over N sweep")
    parser.add_argument("metrics_csv", type=Path)
    parser.add_argument("--out", type=Path, default=Path("./plot.png"))
    args = parser.parse_args()

    latest = parse_metrics(args.metrics_csv)
    if not latest:
        raise SystemExit("No data in metrics CSV")

    fig, ax = plt.subplots(1, 3, figsize=(14, 4.5))

    # Size (MB)
    x_pq, y_pq = build_series(latest, "parquet", "bytes")
    x_orc, y_orc = build_series(latest, "orc", "bytes")
    ax[0].plot(x_pq, y_pq, "o-", label="parquet")
    ax[0].plot(x_orc, y_orc, "s-", label="orc")
    ax[0].set_xscale("log")
    ax[0].set_title("Size vs N (MB)")
    ax[0].set_xlabel("N (log10)")
    ax[0].set_ylabel("MB")
    ax[0].legend()

    # Write time (ms)
    x_pq, y_pq = build_series(latest, "parquet", "write_ms")
    x_orc, y_orc = build_series(latest, "orc", "write_ms")
    ax[1].plot(x_pq, y_pq, "o-", label="parquet")
    ax[1].plot(x_orc, y_orc, "s-", label="orc")
    ax[1].set_xscale("log")
    ax[1].set_title("Write time vs N (ms)")
    ax[1].set_xlabel("N (log10)")
    ax[1].set_ylabel("ms")
    ax[1].legend()

    # Read time (ms)
    x_pq, y_pq = build_series(latest, "parquet", "read_ms")
    x_orc, y_orc = build_series(latest, "orc", "read_ms")
    ax[2].plot(x_pq, y_pq, "o-", label="parquet")
    ax[2].plot(x_orc, y_orc, "s-", label="orc")
    ax[2].set_xscale("log")
    ax[2].set_title("Read time vs N (ms)")
    ax[2].set_xlabel("N (log10)")
    ax[2].set_ylabel("ms")
    ax[2].legend()

    fig.tight_layout()
    fig.savefig(args.out, dpi=150)
    print(f"Saved plot to {args.out}")


if __name__ == "__main__":
    main()


