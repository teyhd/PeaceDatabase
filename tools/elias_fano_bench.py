#!/usr/bin/env python3
"""
/v1/bench/compact
HashSet vs Elias-Fano для полнотекстового индекса.
"""

import argparse
import csv
import json
import os
import sys
from dataclasses import dataclass, asdict
from typing import List, Tuple

import requests

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


@dataclass
class MemoryResult:
    """Результат сравнения памяти"""
    num_docs: int
    tokens_per_doc: int
    hashset_kb: float
    elias_fano_kb: float
    compression_ratio: float
    total_postings: int
    bits_per_posting: float


@dataclass
class SpeedResult:
    """Результат сравнения скорости"""
    num_docs: int
    hashset_ms: float
    elias_fano_ms: float
    queries: int
    hashset_results: int
    elias_fano_results: int


def run_api_benchmark(base_url: str, scales: List[int], tokens_per_doc: int,
                      vocabulary_size: int, queries: int, seed: int) -> Tuple[List[MemoryResult], List[SpeedResult]]:
    """Вызывает API endpoint и возвращает результаты"""
    url = f"{base_url.rstrip('/')}/v1/bench/compact"
    
    payload = {
        "scales": scales,
        "tokensPerDoc": tokens_per_doc,
        "vocabularySize": vocabulary_size,
        "queries": queries,
        "randomSeed": seed
    }
    
    print(f"Calling API: POST {url}")
    print(f"Payload: {json.dumps(payload, indent=2)}")
    
    try:
        response = requests.post(url, json=payload, timeout=300)
        response.raise_for_status()
    except requests.exceptions.ConnectionError:
        print(f"\nERROR: Cannot connect to {base_url}")
        print("Make sure PeaceDatabase is running:")
        print("  cd PeaceDatabase && dotnet run")
        sys.exit(1)
    except requests.exceptions.HTTPError as e:
        print(f"\nERROR: API returned {e.response.status_code}")
        print(e.response.text)
        sys.exit(1)
    
    data = response.json()
    
    memory_results = [
        MemoryResult(
            num_docs=r["numDocs"],
            tokens_per_doc=r["tokensPerDoc"],
            hashset_kb=r["hashSetKb"],
            elias_fano_kb=r["eliasFanoKb"],
            compression_ratio=r["compressionRatio"],
            total_postings=r["totalPostings"],
            bits_per_posting=r["bitsPerPosting"],
        )
        for r in data["memory"]
    ]
    
    speed_results = [
        SpeedResult(
            num_docs=r["numDocs"],
            hashset_ms=r["hashSetMs"],
            elias_fano_ms=r["eliasFanoMs"],
            queries=r["queries"],
            hashset_results=r["hashSetResults"],
            elias_fano_results=r["eliasFanoResults"],
        )
        for r in data["speed"]
    ]
    
    return memory_results, speed_results


def save_results(memory_results: List[MemoryResult], speed_results: List[SpeedResult], 
                 out_dir: str) -> Tuple[str, str, str]:
    """Сохраняет результаты в CSV и JSON"""
    os.makedirs(out_dir, exist_ok=True)
    
    # Memory CSV
    memory_csv = os.path.join(out_dir, "elias_fano_memory.csv")
    with open(memory_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["num_docs", "tokens_per_doc", "hashset_kb", "elias_fano_kb", 
                    "compression_ratio", "total_postings", "bits_per_posting"])
        for r in memory_results:
            w.writerow([r.num_docs, r.tokens_per_doc, f"{r.hashset_kb:.2f}", 
                       f"{r.elias_fano_kb:.2f}", f"{r.compression_ratio:.2f}",
                       r.total_postings, f"{r.bits_per_posting:.2f}"])
    
    # Speed CSV
    speed_csv = os.path.join(out_dir, "elias_fano_speed.csv")
    with open(speed_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["num_docs", "hashset_ms", "elias_fano_ms", "queries"])
        for r in speed_results:
            w.writerow([r.num_docs, f"{r.hashset_ms:.2f}", f"{r.elias_fano_ms:.2f}", r.queries])
    
    # Combined JSON
    results_json = os.path.join(out_dir, "elias_fano_results.json")
    with open(results_json, "w", encoding="utf-8") as f:
        json.dump({
            "memory": [asdict(r) for r in memory_results],
            "speed": [asdict(r) for r in speed_results],
        }, f, indent=2)
    
    return memory_csv, speed_csv, results_json


def plot_memory_comparison(results: List[MemoryResult], out_path: str) -> None:
    """Строит график сравнения памяти"""
    if not results:
        print("No memory results to plot")
        return
    
    results = sorted(results, key=lambda r: r.num_docs)
    
    labels = [f"{r.num_docs:,}\ndocs" for r in results]
    hashset_kb = [r.hashset_kb for r in results]
    ef_kb = [r.elias_fano_kb for r in results]
    
    x = np.arange(len(labels))
    width = 0.35
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    ax.bar(x - width/2, hashset_kb, width, label='HashSet<string>', color='#e74c3c')
    ax.bar(x + width/2, ef_kb, width, label='Elias-Fano', color='#27ae60')
    
    ax.set_ylabel('Memory (KB)', fontsize=12)
    ax.set_xlabel('Dataset Size', fontsize=12)
    ax.set_title('Memory Comparison: HashSet vs Elias-Fano\n(Full-Text Index Posting Lists)', fontsize=14)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend(fontsize=11)
    ax.set_yscale('log')
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    
    for i, r in enumerate(results):
        ax.annotate(f'{r.compression_ratio:.0f}x', 
                    xy=(i, max(hashset_kb[i], ef_kb[i])),
                    xytext=(0, 10), textcoords='offset points',
                    ha='center', fontsize=10, fontweight='bold', color='#2c3e50')
    
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Saved memory comparison plot to {out_path}")


def plot_compression_ratio(results: List[MemoryResult], out_path: str) -> None:
    """Строит график коэффициента компрессии"""
    if not results:
        print("No memory results to plot")
        return
    
    results = sorted(results, key=lambda r: r.total_postings)
    
    postings = [r.total_postings for r in results]
    ratios = [r.compression_ratio for r in results]
    bits = [r.bits_per_posting for r in results]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    
    # Compression ratio
    ax1.bar(range(len(postings)), ratios, color='#3498db')
    ax1.set_xticks(range(len(postings)))
    ax1.set_xticklabels([f"{p//1000}K" if p >= 1000 else str(p) for p in postings])
    ax1.set_xlabel('Total Postings')
    ax1.set_ylabel('Compression Ratio (x)')
    ax1.set_title('Compression Ratio vs Dataset Size')
    ax1.grid(axis='y', alpha=0.3, linestyle='--')
    
    for i, v in enumerate(ratios):
        ax1.text(i, v + max(ratios) * 0.02, f'{v:.0f}x', ha='center', fontsize=10, fontweight='bold')
    
    # Bits per posting
    ax2.bar(range(len(postings)), bits, color='#9b59b6')
    ax2.axhline(y=3, color='#e74c3c', linestyle='--', label='Theoretical min (~3 bits)')
    ax2.set_xticks(range(len(postings)))
    ax2.set_xticklabels([f"{p//1000}K" if p >= 1000 else str(p) for p in postings])
    ax2.set_xlabel('Total Postings')
    ax2.set_ylabel('Bits per Posting')
    ax2.set_title('Space Efficiency: Bits per Posting')
    ax2.legend()
    ax2.grid(axis='y', alpha=0.3, linestyle='--')
    
    for i, v in enumerate(bits):
        ax2.text(i, v + max(bits) * 0.02, f'{v:.1f}', ha='center', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Saved compression ratio plot to {out_path}")


def plot_speed_comparison(results: List[SpeedResult], out_path: str) -> None:
    """Строит график сравнения скорости"""
    if not results:
        print("No speed results to plot")
        return
    
    results = sorted(results, key=lambda r: r.num_docs)
    
    labels = [f"{r.num_docs:,}" for r in results]
    hashset_ms = [r.hashset_ms for r in results]
    ef_ms = [r.elias_fano_ms for r in results]
    
    x = np.arange(len(labels))
    width = 0.35
    
    fig, ax = plt.subplots(figsize=(8, 5))
    
    bars1 = ax.bar(x - width/2, hashset_ms, width, label='HashSet<string>', color='#e74c3c')
    bars2 = ax.bar(x + width/2, ef_ms, width, label='Elias-Fano', color='#27ae60')
    
    ax.set_ylabel('Time (ms)', fontsize=12)
    ax.set_xlabel('Number of Documents', fontsize=12)
    ax.set_title(f'Search Speed: HashSet vs Elias-Fano\n({results[0].queries} queries)', fontsize=14)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend(fontsize=11)
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    
    for bar in bars1:
        height = bar.get_height()
        ax.annotate(f'{height:.1f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3), textcoords="offset points",
                    ha='center', va='bottom', fontsize=9)
    for bar in bars2:
        height = bar.get_height()
        ax.annotate(f'{height:.1f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3), textcoords="offset points",
                    ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Saved speed comparison plot to {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Elias-Fano Benchmark - calls API and builds comparison plots"
    )
    parser.add_argument(
        "--url", "-u",
        default="http://localhost:5000",
        help="PeaceDatabase API base URL (default: http://localhost:5000)"
    )
    parser.add_argument(
        "--scales", "-s",
        default="100,1000,10000,10000",
        help="Comma-separated list of document counts (default: 100,1000,10000,10000)"
    )
    parser.add_argument(
        "--tokens", "-t",
        type=int, default=100,
        help="Tokens per document (default: 100)"
    )
    parser.add_argument(
        "--vocabulary", "-v",
        type=int, default=1000,
        help="Vocabulary size (default: 1000)"
    )
    parser.add_argument(
        "--queries", "-q",
        type=int, default=100,
        help="Number of search queries (default: 100)"
    )
    parser.add_argument(
        "--seed",
        type=int, default=42,
        help="Random seed (default: 42)"
    )
    parser.add_argument(
        "--out", "-o",
        default="docs/benchmarks",
        help="Output directory for results and plots"
    )
    
    args = parser.parse_args()
    
    scales = [int(x.strip()) for x in args.scales.split(",")]
    
    # Вызываем API
    memory_results, speed_results = run_api_benchmark(
        base_url=args.url,
        scales=scales,
        tokens_per_doc=args.tokens,
        vocabulary_size=args.vocabulary,
        queries=args.queries,
        seed=args.seed
    )
    
    print(f"\nReceived {len(memory_results)} memory results, {len(speed_results)} speed results")
    
    # Выводим таблицу результатов
    if memory_results:
        print("\n=== Memory Comparison Results ===")
        print(f"{'Docs':>10} {'Tok/Doc':>8} {'HashSet':>12} {'Elias-Fano':>12} {'Compress':>10} {'Bits/Post':>10}")
        print("-" * 65)
        for r in sorted(memory_results, key=lambda x: x.num_docs):
            print(f"{r.num_docs:>10,} {r.tokens_per_doc:>8} {r.hashset_kb:>10.1f} KB "
                  f"{r.elias_fano_kb:>10.1f} KB {r.compression_ratio:>9.1f}x {r.bits_per_posting:>10.2f}")
    
    if speed_results:
        print("\n=== Speed Comparison Results ===")
        print(f"{'Docs':>10} {'HashSet':>12} {'Elias-Fano':>12} {'Queries':>10}")
        print("-" * 50)
        for r in sorted(speed_results, key=lambda x: x.num_docs):
            print(f"{r.num_docs:>10,} {r.hashset_ms:>10.1f} ms {r.elias_fano_ms:>10.1f} ms {r.queries:>10}")
    
    # Сохраняем результаты
    memory_csv, speed_csv, results_json = save_results(memory_results, speed_results, args.out)
    print(f"\nSaved results to:")
    print(f"  - {memory_csv}")
    print(f"  - {speed_csv}")
    print(f"  - {results_json}")
    
    # Строим графики
    plot_memory_comparison(memory_results, os.path.join(args.out, "elias_fano_memory.png"))
    plot_compression_ratio(memory_results, os.path.join(args.out, "elias_fano_compression.png"))
    plot_speed_comparison(speed_results, os.path.join(args.out, "elias_fano_speed.png"))
    
    print("\nDone!")


if __name__ == "__main__":
    main()
