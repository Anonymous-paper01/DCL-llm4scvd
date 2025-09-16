#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Collect verified Solidity source codes from Etherscan with leakage-safe splits.

Rules:
  - Early split: contracts created between 2022-04-03 00:00:00 UTC and 2025-04-29 23:59:59 UTC,
    verified on Etherscan, and having at least one transaction to the contract address -> sc2-qwen3/
  - Late split:  contracts created >= 2025-04-30 00:00:00 UTC,
    verified on Etherscan, and having at least one transaction -> sc2-20250430/

Notes:
  - Uses BigQuery to find creation addresses + timestamps and filter to "has >=1 tx".
  - Etherscan getsourcecode is called only to confirm verification & pull sources.
  - Only VERIFIED contracts are written to disk (unverified are skipped without creating dirs).
  - Concurrency is bounded; retries only on transient errors.
"""

import os
import re
import sys
import time
import json
import pathlib
import argparse
import threading
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone

import orjson
import requests
from tenacity import retry, wait_exponential_jitter, stop_after_attempt, retry_if_exception_type
from google.cloud import bigquery
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

# -------------------------
# Config (dates in UTC)
# -------------------------

EARLY_START_UTC = "2022-04-03 00:00:00+00"  # strictly AFTER April 2, 2022
EARLY_END_EXCLUSIVE_UTC = "2025-04-30 00:00:00+00"  # early set < this timestamp
LATE_START_UTC = "2025-04-30 00:00:00+00"  # late set >= this timestamp

OUTDIR_EARLY = "sc2-qwen3"
OUTDIR_LATE = "sc2-20250430"
LISTS_DIR = "address_lists"

# ⚠️ Prefer ENV over hardcoding; keeping your value for continuity.
ETHERSCAN_API_KEY = 'J69I8VHNEW1C14UDC85KQN3V6QVW7K86P5'
ETHERSCAN_API_URL = "https://api.etherscan.io/api"

ETHERSCAN_MAX_WORKERS = 4
ETHERSCAN_QPS_BUDGET = 4.5
ETHERSCAN_RATELIMIT_BUCKET = 10
SESSION = requests.Session()

# Non-transient Etherscan messages (do NOT retry on these)
NONRETRY_SUBSTRS = (
    "source code not verified",
    "contract source code not verified",
    "invalid address format",
    "contract does not exist",
)

# -------------------------
# Utilities
# -------------------------

def ensure_dir(p: str | pathlib.Path) -> None:
    pathlib.Path(p).mkdir(parents=True, exist_ok=True)

def write_json(path: pathlib.Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(orjson.dumps(obj, option=orjson.OPT_INDENT_2))

def read_json(path: pathlib.Path) -> Any:
    return orjson.loads(path.read_bytes())

def sanitize_address(addr: str) -> str:
    return addr.lower().strip()

def short(addr: str) -> str:
    return addr[:6] + "..." + addr[-4:]

# -------------------------
# Rate limiter
# -------------------------

class RateLimiter:
    def __init__(self, rate_per_sec: float, bucket: int):
        self.rate = rate_per_sec
        self.bucket = bucket
        self.tokens = bucket
        self.lock = threading.Lock()
        self.last = time.perf_counter()

    def acquire(self):
        while True:
            with self.lock:
                now = time.perf_counter()
                elapsed = now - self.last
                self.last = now
                self.tokens = min(self.bucket, self.tokens + elapsed * self.rate)
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
            time.sleep(0.01)

ratelimiter = RateLimiter(ETHERSCAN_QPS_BUDGET, ETHERSCAN_RATELIMIT_BUCKET)

# -------------------------
# Etherscan
# -------------------------

class EtherscanError(Exception):
    pass

@retry(
    wait=wait_exponential_jitter(initial=0.8, max=6.0),
    stop=stop_after_attempt(6),
    retry=retry_if_exception_type(EtherscanError)
)
def etherscan_getsourcecode(address: str, api_key: str) -> Dict[str, Any]:
    ratelimiter.acquire()
    params = {
        "module": "contract",
        "action": "getsourcecode",
        "address": address,
        "apikey": api_key
    }
    try:
        resp = SESSION.get(ETHERSCAN_API_URL, params=params, timeout=30)
    except requests.RequestException as e:
        raise EtherscanError(f"Network error: {e}") from e

    if resp.status_code != 200:
        raise EtherscanError(f"HTTP {resp.status_code}: {resp.text}")

    try:
        data = resp.json()
    except Exception as e:
        raise EtherscanError(f"Bad JSON: {e}; body={resp.text[:300]}") from e

    status = str(data.get("status", "0"))
    if status != "1":
        msg = (data.get("message") or "").lower()
        res_text = str(data.get("result") or "").lower()
        if any(s in msg or s in res_text for s in NONRETRY_SUBSTRS):
            # Non-verified / invalid → return as-is (caller will skip saving)
            return data
        # Likely transient (rate limit / backend hiccup) → retry
        raise EtherscanError(
            f"Etherscan transient/not-ok: status={data.get('status')} message={data.get('message')} result={data.get('result')}"
        )
    return data

def extract_sources_from_result(result_item: Dict[str, Any]) -> Dict[str, str]:
    """
    Normalize Etherscan SourceCode into a {relative_path: source} dict.
    """
    src_raw = result_item.get("SourceCode", "")
    if not src_raw:
        return {}

    cleaned = src_raw.strip()
    if cleaned.startswith("{{") and cleaned.endswith("}}"):
        cleaned = cleaned[1:-1].strip()

    sources_map: Dict[str, str] = {}
    try:
        obj = json.loads(cleaned)
        if isinstance(obj, dict):
            if "sources" in obj and isinstance(obj["sources"], dict):
                for path, node in obj["sources"].items():
                    if isinstance(node, dict) and "content" in node:
                        sources_map[path] = node["content"]
                    elif isinstance(node, str):
                        sources_map[path] = node
            else:
                if "content" in obj and isinstance(obj["content"], str):
                    sources_map["Contract.sol"] = obj["content"]
                elif "language" in obj and "sources" in obj:
                    for path, node in obj["sources"].items():
                        c = node.get("content") if isinstance(node, dict) else None
                        if c:
                            sources_map[path] = c
    except Exception:
        if cleaned:
            sources_map["Contract.sol"] = cleaned

    return {k: v for k, v in sources_map.items() if isinstance(v, str) and v.strip()}

def smart_write_sources(base_dir: pathlib.Path, address: str, result_item: Dict[str, Any]) -> None:
    """
    For VERIFIED contracts only:
      - write etherscan.json and extracted sources under base_dir/{address}/
    """
    addr_dir = base_dir / sanitize_address(address)
    if (addr_dir / "etherscan.json").exists():
        return

    write_json(addr_dir / "etherscan.json", result_item)

    sources = extract_sources_from_result(result_item)
    if sources:
        for rel_path, text in sources.items():
            safe_rel = re.sub(r"^[./]+", "", rel_path).strip() or "Contract.sol"
            dst = addr_dir / "src" / safe_rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            if not dst.suffix:
                dst = dst.with_suffix(".sol")
            dst.write_text(text, encoding="utf-8")

def fetch_and_store_one(address: str, outdir: pathlib.Path, api_key: str) -> Dict[str, Any]:
    """
    Fetch and persist ONLY if the contract is VERIFIED (has SourceCode).
    """
    addr = sanitize_address(address)
    addr_dir = outdir / addr
    if (addr_dir / "etherscan.json").exists():
        return {"address": addr, "status": "skip_exists"}

    try:
        data = etherscan_getsourcecode(addr, api_key=api_key)
    except EtherscanError as e:
        # network/transient errors after retries; skip persisting
        return {"address": addr, "status": "error", "error": str(e)}

    result_list = data.get("result", [])
    if not isinstance(result_list, list) or len(result_list) == 0:
        return {"address": addr, "status": "empty"}

    # Pick the first entry that actually contains SourceCode
    best = None
    for item in result_list:
        if item.get("SourceCode"):
            best = item
            break

    if best is None:
        return {"address": addr, "status": "unverified"}

    smart_write_sources(outdir, addr, best)
    return {"address": addr, "status": "ok"}

# -------------------------
# BigQuery queries
# -------------------------
# Contract creation within window AND having >=1 transaction to the contract address.
# We compute the creation timestamp as MIN(block_timestamp) per address to dedupe.

SQL_ADDRESSES_EARLY = f"""
WITH created AS (
  SELECT
    receipt_contract_address AS contract_address,
    MIN(block_timestamp) AS creation_ts
  FROM `bigquery-public-data.crypto_ethereum.transactions`
  WHERE receipt_contract_address IS NOT NULL
    AND block_timestamp >= TIMESTAMP('{EARLY_START_UTC}')
    AND block_timestamp <  TIMESTAMP('{EARLY_END_EXCLUSIVE_UTC}')
  GROUP BY contract_address
)
SELECT
  c.contract_address,
  c.creation_ts AS block_timestamp
FROM created c
WHERE EXISTS (
  SELECT 1
  FROM `bigquery-public-data.crypto_ethereum.transactions` t
  WHERE t.to_address = c.contract_address
  LIMIT 1
)
"""

SQL_ADDRESSES_LATE = f"""
WITH created AS (
  SELECT
    receipt_contract_address AS contract_address,
    MIN(block_timestamp) AS creation_ts
  FROM `bigquery-public-data.crypto_ethereum.transactions`
  WHERE receipt_contract_address IS NOT NULL
    AND block_timestamp >= TIMESTAMP('{LATE_START_UTC}')
  GROUP BY contract_address
)
SELECT
  c.contract_address,
  c.creation_ts AS block_timestamp
FROM created c
WHERE EXISTS (
  SELECT 1
  FROM `bigquery-public-data.crypto_ethereum.transactions` t
  WHERE t.to_address = c.contract_address
  LIMIT 1
)
"""

def query_addresses(save_path: pathlib.Path, sql: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    client = bigquery.Client()
    job = client.query(sql)
    rows = job.result(page_size=100000)

    results = []
    for i, row in enumerate(rows):
        if limit and i >= limit:
            break
        results.append({
            "address": row["contract_address"],
            "block_timestamp": row["block_timestamp"].isoformat()
        })

    write_json(save_path, results)
    return results

# -------------------------
# Orchestration
# -------------------------

def already_done(outdir: pathlib.Path) -> set:
    if not outdir.exists():
        return set()
    done = set()
    for child in outdir.iterdir():
        if child.is_dir() and (child / "etherscan.json").exists():
            done.add(child.name.lower())
    return done

def run_phase(addresses: List[Dict[str, Any]], outdir: pathlib.Path, api_key: str, max_workers: int) -> Dict[str, int]:
    ensure_dir(outdir)
    done_dirs = already_done(outdir)

    uniq: List[str] = []
    seen = set()
    for item in addresses:
        addr = sanitize_address(item["address"])
        if addr and addr not in seen and addr not in done_dirs:
            seen.add(addr)
            uniq.append(addr)

    counters = {"ok": 0, "unverified": 0, "empty": 0, "error": 0, "skip_exists": len(done_dirs)}
    total = len(uniq)
    if total == 0:
        return counters

    max_pending = max(16, max_workers * 8)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        it = iter(uniq)
        pending = set()

        # Prime
        for _ in range(min(max_pending, total)):
            try:
                addr = next(it)
            except StopIteration:
                break
            pending.add(ex.submit(fetch_and_store_one, addr, outdir, api_key))

        with tqdm(total=total, desc=f"Saving -> {outdir}") as pbar:
            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for fut in done:
                    try:
                        res = fut.result()
                    except Exception:
                        res = {"status": "error"}
                    counters[res["status"]] = counters.get(res["status"], 0) + 1
                    pbar.update(1)
                    try:
                        addr = next(it)
                        pending.add(ex.submit(fetch_and_store_one, addr, outdir, api_key))
                    except StopIteration:
                        pass

    return counters

def main():
    parser = argparse.ArgumentParser(description="Collect verified Etherscan sources with leakage-safe splits.")
    parser.add_argument("--limit_early", type=int, default=None, help="Limit for early window (debug).")
    parser.add_argument("--limit_late", type=int, default=None, help="Limit for late window (debug).")
    parser.add_argument("--max_workers", type=int, default=ETHERSCAN_MAX_WORKERS, help="Parallel Etherscan workers.")
    parser.add_argument("--skip_bq", action="store_true", help="Reuse cached address lists in address_lists/.")
    parser.add_argument("--etherscan_api_key", type=str, default=ETHERSCAN_API_KEY, help="Etherscan API key.")
    args = parser.parse_args()

    if not args.etherscan_api_key:
        print("ERROR: Missing Etherscan API key. Set --etherscan_api_key or env.", file=sys.stderr)
        sys.exit(2)

    ensure_dir(LISTS_DIR)
    ensure_dir(OUTDIR_EARLY)
    ensure_dir(OUTDIR_LATE)

    path_early = pathlib.Path(LISTS_DIR) / "addresses_20220403_to_20250429.json"
    path_late  = pathlib.Path(LISTS_DIR) / "addresses_on_or_after_20250430.json"

    # Step 1: Query or load cached lists
    if not args.skip_bq or not path_early.exists() or not path_late.exists():
        print("Querying BigQuery for contract addresses (early & late windows)…")
        addrs_early = query_addresses(path_early, SQL_ADDRESSES_EARLY, args.limit_early)
        addrs_late  = query_addresses(path_late,  SQL_ADDRESSES_LATE,  args.limit_late)
    else:
        print("Loading cached address lists…")
        addrs_early = read_json(path_early)
        addrs_late  = read_json(path_late)

    # Step 2: Early window → sc2-qwen3 (verified + >=1 tx)
    print(f"Phase A (early): fetching {len(addrs_early):,} addresses into {OUTDIR_EARLY}/ …")
    stats_a = run_phase(addrs_early, pathlib.Path(OUTDIR_EARLY), args.etherscan_api_key, args.max_workers)
    print("Phase A stats:", stats_a)

    # Step 3: Late window → sc2-20250430 (verified + >=1 tx)
    print(f"Phase B (late):  fetching {len(addrs_late):,} addresses into {OUTDIR_LATE}/ …")
    stats_b = run_phase(addrs_late, pathlib.Path(OUTDIR_LATE), args.etherscan_api_key, args.max_workers)
    print("Phase B stats:", stats_b)

    print("\nDone.\n"
          f"  Early (2022-04-03 .. 2025-04-29) saved to: {OUTDIR_EARLY}/\n"
          f"  Late  (>= 2025-04-30)                saved to: {OUTDIR_LATE}/\n"
          "Notes:\n"
          "  - Only VERIFIED contracts are persisted (unverified are skipped).\n"
          "  - Each contract dir contains etherscan.json and src/*.sol when available.\n")

if __name__ == "__main__":
    main()
