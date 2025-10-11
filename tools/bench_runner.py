import argparse
import json
import os
import sys
import time
from typing import List

import requests
import matplotlib.pyplot as plt


def run_bench(base_url: str, source_kind: str, path_or_url: str, scales: List[int], max_docs: int):
    payload = {
        "source": {"kind": source_kind},
        "scales": scales,
        "maxDocs": max_docs,
        "warmup": 1,
        "iterations": 1,
    }
    if source_kind == "file":
        payload["source"]["path"] = path_or_url
    elif source_kind == "url":
        payload["source"]["url"] = path_or_url

    r = requests.post(f"{base_url}/v1/bench/run", json=payload, timeout=600)
    r.raise_for_status()
    return r.json()


def plot_results(data, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    results = data["results"]
    scales = sorted({r["n"] for r in results})
    formats = ["json", "binary", "protobuf"]

    def line_plot(metric: str, fname: str, ylabel: str):
        plt.figure(figsize=(8, 5))
        for fmt in formats:
            y = [next(r[metric] for r in results if r["format"] == fmt and r["n"] == n) for n in scales]
            plt.plot(scales, y, marker='o', label=fmt)
        plt.xscale('log')
        plt.xlabel('N (documents) [log]')
        plt.ylabel(ylabel)
        plt.title(f'{metric} vs N')
        plt.grid(True, which='both', ls=':')
        plt.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, fname), dpi=150)
        plt.close()

    # Serialize/Deserialize time
    line_plot('serializeMs', 'serialize_vs_n.png', 'Serialize time (ms)')
    line_plot('deserializeMs', 'deserialize_vs_n.png', 'Deserialize time (ms)')

    # Bytes per doc bar at max N
    max_n = max(scales)
    subset = [r for r in results if r["n"] == max_n]
    subset.sort(key=lambda r: formats.index(r["format"]))
    plt.figure(figsize=(8, 5))
    plt.bar([r["format"] for r in subset], [r["avgBytesPerDoc"] for r in subset])
    plt.ylabel('Avg bytes per doc')
    plt.title(f'Avg bytes per doc at N={max_n}')
    plt.grid(True, axis='y', ls=':')
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, 'bytes_per_doc.png'), dpi=150)
    plt.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--base-url', default='http://localhost:5000')
    ap.add_argument('--source', choices=['file', 'url', 'synthetic'], default='synthetic')
    ap.add_argument('--path-or-url', default='')
    ap.add_argument('--max-docs', type=int, default=100000)
    ap.add_argument('--scales', default='1,10,100,1000,10000,100000')
    ap.add_argument('--out', default='docs/benchmarks')
    args = ap.parse_args()

    scales = [int(x) for x in args.scales.split(',')]
    data = run_bench(args.base_url, args.source, args.path_or_url, scales, args.max_docs)

    # Save raw
    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, 'results.json'), 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    plot_results(data, args.out)
    print(f"Saved results and plots to {args.out}")


if __name__ == '__main__':
    main()


