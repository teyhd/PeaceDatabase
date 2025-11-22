#!/usr/bin/env python3
import argparse
import csv
import dataclasses
import json
import random
import string
import time
from typing import Any, Dict, List, Tuple

import requests
from pymongo import MongoClient


def _rand_string(prefix: str, n: int = 8) -> str:
    return prefix + "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


def generate_documents(n: int, seed: int = 123) -> List[Dict[str, Any]]:
    """
    Генерирует синтетические документы, структурой близкие к BenchController.GenerateSynthetic.
    """
    rnd = random.Random(seed)
    docs: List[Dict[str, Any]] = []
    for i in range(n):
        doc_id = f"bench-{i:06d}"
        data = {
            "name": f"user_{i}",
            "age": 18 + (i % 60),
            "score": rnd.random() * 100.0,
            "active": (i % 3) == 0,
            "tags": ["bench", "synthetic", "even" if (i % 2) == 0 else "odd"],
            "nested": {
                "x": rnd.randint(0, 1000),
                "y": rnd.random(),
                "z": {"k": "v"},
            },
        }
        docs.append(
            {
                "id": doc_id,
                "data": data,
            }
        )
    return docs


@dataclasses.dataclass
class EngineResult:
    engine: str
    n: int
    insert_ms: float
    read_ms: float


class PeaceDbClient:
    def __init__(self, base_url: str, db_name: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.db = db_name

    def create_db(self) -> None:
        r = requests.put(f"{self.base_url}/v1/db/{self.db}", timeout=30)
        r.raise_for_status()

    def bulk_insert(self, docs: List[Dict[str, Any]]) -> None:
        url = f"{self.base_url}/v1/db/{self.db}/docs"
        for d in docs:
            payload = {
                "id": d["id"],
                "data": d["data"],
                "tags": ["bench"],
                "content": None,
            }
            r = requests.post(url, json=payload, timeout=30)
            r.raise_for_status()

    def bulk_read(self, ids: List[str]) -> None:
        for doc_id in ids:
            url = f"{self.base_url}/v1/db/{self.db}/docs/{doc_id}"
            r = requests.get(url, timeout=30)
            r.raise_for_status()


class MongoClientWrapper:
    def __init__(self, uri: str, db_name: str, collection: str) -> None:
        self._client = MongoClient(uri)
        self._db = self._client[db_name]
        self._col = self._db[collection]

    def prepare(self) -> None:
        self._col.drop()
        self._col.create_index("id", unique=True)

    def bulk_insert(self, docs: List[Dict[str, Any]]) -> None:
        to_insert = [
            {
                "_id": d["id"],
                "id": d["id"],
                "data": d["data"],
            }
            for d in docs
        ]
        if to_insert:
            self._col.insert_many(to_insert, ordered=False)

    def bulk_read(self, ids: List[str]) -> None:
        for doc_id in ids:
            _ = self._col.find_one({"id": doc_id})


class CouchDbClient:
    def __init__(self, base_url: str, db_name: str, user: str, password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.db = db_name
        self.auth = (user, password)

    def create_db(self) -> None:
        r = requests.put(f"{self.base_url}/{self.db}", auth=self.auth, timeout=30)
        if r.status_code not in (201, 202, 412):
            r.raise_for_status()

    def bulk_insert(self, docs: List[Dict[str, Any]]) -> None:
        bulk = {
            "docs": [
                {
                    "_id": d["id"],
                    "id": d["id"],
                    "data": d["data"],
                }
                for d in docs
            ]
        }
        url = f"{self.base_url}/{self.db}/_bulk_docs"
        r = requests.post(url, auth=self.auth, json=bulk, timeout=120)
        r.raise_for_status()

    def bulk_read(self, ids: List[str]) -> None:
        for doc_id in ids:
            url = f"{self.base_url}/{self.db}/{doc_id}"
            r = requests.get(url, auth=self.auth, timeout=30)
            if r.status_code not in (200, 404):
                r.raise_for_status()


def run_engine_bench(
    engine: str,
    docs: List[Dict[str, Any]],
    peacedb_client: PeaceDbClient,
    mongo_client: MongoClientWrapper,
    couch_client: CouchDbClient,
) -> EngineResult:
    ids = [d["id"] for d in docs]

    if engine == "peacedb":
        peacedb_client.create_db()
        t0 = time.perf_counter()
        peacedb_client.bulk_insert(docs)
        t1 = time.perf_counter()
        peacedb_client.bulk_read(ids)
        t2 = time.perf_counter()
        return EngineResult(engine="peacedb", n=len(docs), insert_ms=(t1 - t0) * 1000.0, read_ms=(t2 - t1) * 1000.0)

    if engine == "mongo":
        mongo_client.prepare()
        t0 = time.perf_counter()
        mongo_client.bulk_insert(docs)
        t1 = time.perf_counter()
        mongo_client.bulk_read(ids)
        t2 = time.perf_counter()
        return EngineResult(engine="mongo", n=len(docs), insert_ms=(t1 - t0) * 1000.0, read_ms=(t2 - t1) * 1000.0)

    if engine == "couchdb":
        couch_client.create_db()
        t0 = time.perf_counter()
        couch_client.bulk_insert(docs)
        t1 = time.perf_counter()
        couch_client.bulk_read(ids)
        t2 = time.perf_counter()
        return EngineResult(engine="couchdb", n=len(docs), insert_ms=(t1 - t0) * 1000.0, read_ms=(t2 - t1) * 1000.0)

    raise ValueError(f"Unknown engine: {engine}")


def save_results_csv(path: str, rows: List[EngineResult]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["engine", "n", "insertMs", "readMs"])
        for r in rows:
            w.writerow([r.engine, r.n, f"{r.insert_ms:.3f}", f"{r.read_ms:.3f}"])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=100000, help="Number of documents to insert/read per engine")
    ap.add_argument("--seed", type=int, default=123, help="Random seed for data generation")

    ap.add_argument("--peacedb-url", default="http://localhost:8080", help="Base URL of PeaceDatabase Web API")
    ap.add_argument("--peacedb-db", default="benchdb", help="Database name in PeaceDatabase")

    ap.add_argument("--mongo-uri", default="mongodb://localhost:27017", help="MongoDB connection URI")
    ap.add_argument("--mongo-db", default="benchdb", help="MongoDB database name")
    ap.add_argument("--mongo-collection", default="docs", help="MongoDB collection name")

    ap.add_argument("--couchdb-url", default="http://localhost:5984", help="CouchDB base URL")
    ap.add_argument("--couchdb-db", default="benchdb", help="CouchDB database name")
    ap.add_argument("--couchdb-user", default="admin", help="CouchDB admin user")
    ap.add_argument("--couchdb-password", default="admin", help="CouchDB admin password")

    ap.add_argument("--out", default="docs/db_bench.csv", help="Path to CSV file with results")
    args = ap.parse_args()

    docs = generate_documents(args.n, seed=args.seed)

    peacedb_client = PeaceDbClient(args.peacedb_url, args.peacedb_db)
    mongo_client = MongoClientWrapper(args.mongo_uri, args.mongo_db, args.mongo_collection)
    couch_client = CouchDbClient(args.couchdb_url, args.couchdb_db, args.couchdb_user, args.couchdb_password)

    engines = ["peacedb", "mongo", "couchdb"]
    all_results: List[EngineResult] = []

    for eng in engines:
        print(f"Running benchmark for {eng} with N={args.n}...")
        res = run_engine_bench(eng, docs, peacedb_client, mongo_client, couch_client)
        print(
            f"{eng}: insert={res.insert_ms:.1f} ms, read={res.read_ms:.1f} ms "
            f"(insert/doc={res.insert_ms / max(1, res.n):.4f} ms, read/doc={res.read_ms / max(1, res.n):.4f} ms)"
        )
        all_results.append(res)

    save_results_csv(args.out, all_results)
    print(f"Saved results to {args.out}")


if __name__ == "__main__":
    main()


