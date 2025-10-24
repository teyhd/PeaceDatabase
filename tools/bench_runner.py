#!/usr/bin/env python3
import argparse
import csv
import json
import os
import sys
import time
from typing import Dict, List, Tuple

import requests

# Headless backend для серверов без дисплея: ставим ДО pyplot
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import statistics as stats


def get_formats(base_url: str) -> List[str]:
    try:
        r = requests.get(f"{base_url}/v1/bench/formats", timeout=30)
        r.raise_for_status()
        return [str(x).lower() for x in r.json()]
    except Exception:
        return []


def run_bench(
    base_url: str,
    source_kind: str,
    path_or_url: str,
    scales: List[int],
    max_docs: int,
    warmup: int,
    iterations: int,
    random_seed: int = 123,
):
    payload = {
        "source": {"kind": source_kind},
        "scales": scales,
        "maxDocs": max_docs,
        "warmup": warmup,
        "iterations": iterations,
        "randomSeed": random_seed,
    }
    if source_kind == "file":
        payload["source"]["path"] = path_or_url
    elif source_kind == "url":
        payload["source"]["url"] = path_or_url

    r = requests.post(f"{base_url}/v1/bench/run", json=payload, timeout=600)
    r.raise_for_status()
    return r.json()


# ---------------- PLOTTING (наглядная версия) ----------------

def _aggregate_results(results):
    """
    Группируем по (format, n) и считаем mean/std для метрик.
    Возвращаем:
      scales_sorted: [int]
      formats_sorted: [str]
      agg: dict[format][n] -> {'serializeMs':{'mean','std'}, 'deserializeMs':..., 'avgBytesPerDoc':...}
    """
    buckets = {}
    ns = set()
    fmts = set()
    for r in results:
        fmt = str(r.get("format", "")).lower()
        n = int(r.get("n", 0))
        if not fmt:
            continue
        ns.add(n)
        fmts.add(fmt)
        d = buckets.setdefault(fmt, {}).setdefault(n, {"serializeMs": [], "deserializeMs": [], "avgBytesPerDoc": []})
        if "serializeMs" in r: d["serializeMs"].append(float(r["serializeMs"]))
        if "deserializeMs" in r: d["deserializeMs"].append(float(r["deserializeMs"]))
        if "avgBytesPerDoc" in r: d["avgBytesPerDoc"].append(float(r["avgBytesPerDoc"]))

    def mstd(vals):
        if not vals: return {"mean": 0.0, "std": 0.0}
        if len(vals) == 1: return {"mean": vals[0], "std": 0.0}
        return {"mean": stats.fmean(vals), "std": stats.pstdev(vals)}

    agg = {}
    for fmt, by_n in buckets.items():
        agg[fmt] = {}
        for n, met in by_n.items():
            agg[fmt][n] = {k: mstd(v) for k, v in met.items()}

    preferred = ["json", "binary", "protobuf"]
    fmts_detected = list(fmts)
    tail = [f for f in fmts_detected if f not in preferred]
    formats_sorted = [f for f in preferred if f in fmts_detected] + sorted(tail)
    scales_sorted = sorted(ns)
    return scales_sorted, formats_sorted, agg


def _annotate_hbars(ax, bars, values, speedup_vs=None):
    for bar, v in zip(bars, values):
        x = bar.get_width()
        y = bar.get_y() + bar.get_height() / 2
        label = f"{v:.4g} ms"
        if speedup_vs is not None:
            label += f"  (×{speedup_vs / v:.1f})" if v > 0 else ""
        ax.text(x * 1.02 if x > 0 else 0.02, y, label, va="center", ha="left", fontsize=9)


def _plot_single_n_times(out_dir, n, formats_sorted, agg, metric, title):
    vals, labels = [], []
    for fmt in formats_sorted:
        m = agg.get(fmt, {}).get(n)
        if not m: continue
        vals.append(m[metric]["mean"])
        labels.append(fmt)
    if not vals:
        return

    # База для speedup: json, если есть, иначе самое медленное
    base_idx = labels.index("json") if "json" in labels else max(range(len(vals)), key=lambda i: vals[i])
    base = vals[base_idx]

    fig, ax = plt.subplots(figsize=(9, 4.8))
    bars = ax.barh(labels, vals)
    ax.set_xscale("log")
    ax.set_xlabel("Time (ms) [log]")
    ax.set_title(f"{title} at N={n}")
    ax.grid(True, axis="x", ls=":", which="both")
    _annotate_hbars(ax, bars, vals, speedup_vs=base)
    plt.tight_layout()
    fname = "serialize_hbar.png" if metric == "serializeMs" else "deserialize_hbar.png"
    plt.savefig(os.path.join(out_dir, fname), dpi=150)
    plt.close(fig)


def _plot_multi_n_lines(out_dir, scales_sorted, formats_sorted, agg, metric, ylabel, fname):
    fig, ax = plt.subplots(figsize=(8.8, 5.2))
    for fmt in formats_sorted:
        xs, ys, yerr = [], [], []
        for n in scales_sorted:
            m = agg.get(fmt, {}).get(n)
            if not m: continue
            xs.append(n)
            ys.append(m[metric]["mean"])
            yerr.append(m[metric]["std"])
        if xs:
            ax.errorbar(xs, ys, yerr=yerr, marker="o", capsize=3, label=fmt)
    if scales_sorted and min(scales_sorted) > 0:
        ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("N (documents)" + (" [log]" if (scales_sorted and min(scales_sorted) > 0) else ""))
    ax.set_ylabel(ylabel + " [log]")
    ax.set_title(f"{metric} vs N (mean ± σ)")
    ax.grid(True, which="both", ls=":")
    ax.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, fname), dpi=150)
    plt.close(fig)


def _plot_bytes_bar(out_dir, n, formats_sorted, agg):
    vals, labels = [], []
    for fmt in formats_sorted:
        m = agg.get(fmt, {}).get(n)
        if not m: continue
        vals.append(m["avgBytesPerDoc"]["mean"])
        labels.append(fmt)
    if not vals:
        return

    fig, ax = plt.subplots(figsize=(7.8, 4.6))
    ax.bar(labels, vals)
    ax.set_ylabel("Avg bytes per doc")
    ax.set_title(f"Avg bytes per doc at N={n}")
    ax.grid(True, axis="y", ls=":")
    for i, v in enumerate(vals):
        ax.text(i, v * 1.01, f"{int(v):,}".replace(",", " "), ha="center", va="bottom", fontsize=9)
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "bytes_per_doc.png"), dpi=150)
    plt.close(fig)


def plot_results(data: dict, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    results = data.get("results") or data.get("Results")
    if not isinstance(results, list) or not results:
        raise ValueError("No results in response to plot.")

    scales_sorted, formats_sorted, agg = _aggregate_results(results)

    if len(scales_sorted) == 1:
        n = scales_sorted[0]
        _plot_single_n_times(out_dir, n, formats_sorted, agg, "serializeMs", "Serialize time")
        _plot_single_n_times(out_dir, n, formats_sorted, agg, "deserializeMs", "Deserialize time")
        _plot_bytes_bar(out_dir, n, formats_sorted, agg)
    else:
        _plot_multi_n_lines(out_dir, scales_sorted, formats_sorted, agg, "serializeMs",
                            "Serialize time (ms)", "serialize_vs_n.png")
        _plot_multi_n_lines(out_dir, scales_sorted, formats_sorted, agg, "deserializeMs",
                            "Deserialize time (ms)", "deserialize_vs_n.png")
        max_n = max(scales_sorted)
        _plot_bytes_bar(out_dir, max_n, formats_sorted, agg)


# ---------------- сохранение CSV ----------------

def save_csv(data: dict, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    results = data.get("results") or data.get("Results")
    if not isinstance(results, list) or not results:
        return
    csv_path = os.path.join(out_dir, "results.csv")
    fields = ["format", "n", "serializeMs", "deserializeMs", "totalBytes", "avgBytesPerDoc"]
    alt = {"Format": "format", "N": "n", "SerializeMs": "serializeMs", "DeserializeMs": "deserializeMs",
           "TotalBytes": "totalBytes", "AvgBytesPerDoc": "avgBytesPerDoc"}
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(fields)
        for r in results:
            row = []
            for k in fields:
                if k in r:
                    row.append(r[k])
                else:
                    pc = [kk for kk, vv in alt.items() if vv == k]
                    row.append(r.get(pc[0], "")) if pc else row.append("")
            w.writerow(row)


# ---------------- main ----------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default="http://localhost:5000")
    ap.add_argument("--source", choices=["file", "url", "synthetic"], default="file")
    ap.add_argument("--path-or-url", default="D:\\_ITMOStudy\\_Master\\OPVP\\PeaceDatabase\\tools\\AMZN.json")

    ap.add_argument("--max-docs", type=int, default=100000)
    ap.add_argument("--scales", default="1,10,100,1000,10000,1000000000")
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--iterations", type=int, default=1)
    ap.add_argument("--seed", type=int, default=123)
    ap.add_argument("--out", default="docs/benchmarks")
    args = ap.parse_args()

    try:
        scales = [int(x.strip()) for x in args.scales.split(",") if x.strip()]
        if not scales:
            raise ValueError("Empty --scales.")
    except Exception as e:
        print(f"Bad --scales: {e}", file=sys.stderr)
        sys.exit(2)

    fmts_from_api = get_formats(args.base_url)
    if fmts_from_api:
        print(f"Formats from API: {', '.join(fmts_from_api)}")

    t0 = time.time()
    data = run_bench(
        args.base_url,
        args.source,
        args.path_or_url,
        scales,
        args.max_docs,
        args.warmup,
        args.iterations,
        random_seed=args.seed,
    )
    dt = time.time() - t0

    os.makedirs(args.out, exist_ok=True)
    raw_path = os.path.join(args.out, "results.json")
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    save_csv(data, args.out)
    plot_results(data, args.out)

    print(f"Bench finished in {dt:.2f}s. Saved results and plots to {args.out}")
    print(f"Raw JSON: {raw_path}")
    print(f"CSV: {os.path.join(args.out, 'results.csv')}")
    print("Plots: serialize_hbar.png / deserialize_hbar.png (single N) "
          "или serialize_vs_n.png / deserialize_vs_n.png (multi N) + bytes_per_doc.png")


if __name__ == "__main__":
    main()


