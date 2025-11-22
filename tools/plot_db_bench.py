#!/usr/bin/env python3
import argparse
import csv
import os
from typing import List, Tuple

import matplotlib.pyplot as plt


def load_results(csv_path: str) -> Tuple[List[str], List[float], List[float]]:
    engines: List[str] = []
    insert_ms: List[float] = []
    read_ms: List[float] = []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            engines.append(row["engine"])
            insert_ms.append(float(row["insertMs"]))
            read_ms.append(float(row["readMs"]))

    return engines, insert_ms, read_ms


def plot_db_bench(csv_path: str, out_path: str) -> None:
    engines, insert_ms, read_ms = load_results(csv_path)
    if not engines:
        raise ValueError(f"No data found in {csv_path}")

    # Переводим миллисекунды в секунды для удобства чтения
    insert_s = [v / 1000.0 for v in insert_ms]
    read_s = [v / 1000.0 for v in read_ms]

    x = range(len(engines))
    width = 0.35

    fig, ax = plt.subplots(figsize=(8, 4.5))

    ax.bar([i - width / 2 for i in x], insert_s, width, label="insert (s)")
    ax.bar([i + width / 2 for i in x], read_s, width, label="read (s)")

    ax.set_xticks(list(x))
    ax.set_xticklabels(engines)
    ax.set_ylabel("Time, seconds")
    ax.set_title("DB benchmark: insert/read total time per engine")
    ax.grid(axis="y", linestyle=":", alpha=0.5)
    ax.legend()

    # Подписи над столбцами
    for i, v in enumerate(insert_s):
        ax.text(i - width / 2, v * 1.01, f"{v:.1f}", ha="center", va="bottom", fontsize=8)
    for i, v in enumerate(read_s):
        ax.text(i + width / 2, v * 1.01, f"{v:.1f}", ha="center", va="bottom", fontsize=8)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close(fig)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--csv",
        default="../docs/db_bench.csv",
        help="Path to db_bench.csv file",
    )
    ap.add_argument(
        "--out",
        default="../docs/db_bench.png",
        help="Path to output PNG image",
    )
    args = ap.parse_args()

    plot_db_bench(args.csv, args.out)
    print(f"Saved plot to {args.out}")


if __name__ == "__main__":
    main()


