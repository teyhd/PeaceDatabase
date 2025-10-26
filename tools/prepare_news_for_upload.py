import argparse
import hashlib
import json
import os
import sys
import time
from typing import Iterable, Iterator, Optional

try:
    import requests  # type: ignore
except Exception:
    requests = None  # lazy error until --upload is requested


def iter_records(path: str) -> Iterator[dict]:
    """
    Yield JSON objects from a dataset file that may be:
      - JSON Lines (one JSON object per line)
      - JSON array ([{...}, {...}, ...])
      - Irregular concatenated objects (best-effort line-by-line parsing)
    """
    # Fast path: try to load whole file as JSON (array or single object)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    yield item
            return
        if isinstance(data, dict):
            # Single object dataset (rare) – treat as one record
            yield data
            return
    except Exception:
        # Fall back to line-by-line parsing for JSONL or irregular files
        pass

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # Some files may include a header like {"id":..., "rev":..., "deleted":..., "data": { ...
            # We only parse standalone JSON objects per line.
            if not (line.startswith("{") and line.endswith("}")):
                # Best-effort: attempt to find a JSON object substring
                start = line.find("{")
                end = line.rfind("}")
                if start >= 0 and end > start:
                    line = line[start : end + 1]
                else:
                    continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    yield obj
            except json.JSONDecodeError:
                # Skip malformed lines
                continue


def to_document(item: dict) -> dict:
    """Map a raw news item into PeaceDB Document JSON shape."""
    # Build content from headline + short_description when available
    headline = item.get("headline") or item.get("title") or ""
    short = item.get("short_description") or item.get("summary") or ""
    parts = [p for p in [headline, short] if p]
    content: Optional[str] = " ".join(parts) if parts else None

    category = item.get("category")
    tags = [str(category)] if category else None

    # Let server autogenerate id; still produce a stable candidate for id if needed
    # candidate_id = stable_id(item)

    doc = {
        # "id": candidate_id,  # optional – comment out to let server generate
        "data": item,
        "tags": tags,
        "content": content,
    }
    # Remove None fields to keep payload compact
    return {k: v for k, v in doc.items() if v is not None}


def stable_id(item: dict) -> str:
    """Create a stable id from link or headline+date to avoid duplicates (optional)."""
    basis = (
        str(item.get("link"))
        or (str(item.get("headline", "")) + "|" + str(item.get("date", "")))
        or json.dumps(item, sort_keys=True)
    )
    h = hashlib.sha1(basis.encode("utf-8")).hexdigest()
    return f"news-{h[:16]}"


def write_ndjson(records: Iterable[dict], out_path: str, limit: Optional[int]) -> int:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    count = 0
    with open(out_path, "w", encoding="utf-8") as out:
        for raw in records:
            doc = to_document(raw)
            out.write(json.dumps(doc, ensure_ascii=False) + "\n")
            count += 1
            if limit and count >= limit:
                break
    return count


def ensure_db(base_url: str, db: str) -> None:
    # HEAD triggers idempotent create per server code
    url = f"{base_url.rstrip('/')}/v1/db/{db}"
    try:
        requests.head(url, timeout=10)
    except Exception:
        pass


def upload_stream(base_url: str, db: str, ndjson_path: str, start: int, rate_limit: float) -> int:
    url = f"{base_url.rstrip('/')}/v1/db/{db}/docs"
    sent = 0
    with open(ndjson_path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i < start:
                continue
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            try:
                r = requests.post(url, json=payload, timeout=30)
                if r.status_code not in (200, 201):
                    # Print minimal diagnostics and continue
                    sys.stderr.write(f"HTTP {r.status_code}: {r.text[:200]}\n")
                sent += 1
            except Exception as ex:
                sys.stderr.write(f"ERR: {ex}\n")
            if rate_limit > 0:
                time.sleep(rate_limit)
    return sent


def main() -> None:
    p = argparse.ArgumentParser(description="Prepare News_Category_Dataset_v3 for PeaceDB upload")
    p.add_argument("--input", default=os.path.join("tools", "News_Category_Dataset_v3.json"))
    p.add_argument("--out", default=os.path.join("tools", "out", "news_documents.ndjson"))
    p.add_argument("--limit", type=int, default=None, help="Max records to prepare")
    p.add_argument("--upload", action="store_true", help="Upload to API after preparing")
    p.add_argument("--api", default="http://localhost:5000", help="API base URL")
    p.add_argument("--db", default="news", help="Database name")
    p.add_argument("--start", type=int, default=0, help="Start index within NDJSON for upload")
    p.add_argument("--rate", type=float, default=0.0, help="Seconds to sleep between uploads")
    args = p.parse_args()

    recs = iter_records(args.input)
    total = write_ndjson(recs, args.out, args.limit)
    print(f"Prepared {total} documents -> {args.out}")

    if args.upload:
        if requests is None:
            print("The 'requests' package is required for --upload. Install via: pip install requests", file=sys.stderr)
            sys.exit(2)
        ensure_db(args.api, args.db)
        sent = upload_stream(args.api, args.db, args.out, args.start, args.rate)
        print(f"Uploaded {sent} documents to {args.api.rstrip('/')}/v1/db/{args.db}/docs")


if __name__ == "__main__":
    main()


