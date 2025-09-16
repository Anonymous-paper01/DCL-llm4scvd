import argparse
import json
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional

import requests

ETHERSCAN_API_KEY = 'J69I8VHNEW1C14UDC85KQN3V6QVW7K86P5'
SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def sanitize_filename(name: str) -> str:
    name = name.strip().replace(" ", "_")
    name = SAFE_NAME_RE.sub("_", name)
    name = name.strip("._")
    return name or "contract"


def ensure_unique(path: str) -> str:
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(path)
    idx = 1
    while True:
        candidate = f"{base}__{idx}{ext}"
        if not os.path.exists(candidate):
            return candidate
        idx += 1


def concat_sources_from_metadata(source_code_field: str) -> Optional[str]:
    # Etherscan can return SourceCode as:
    # - Flat solidity string
    # - JSON metadata (often wrapped with extra braces)
    text = source_code_field
    if not text:
        return None

    # Try to parse as JSON metadata
    raw = text.strip()
    # Remove possible leading/trailing quotes
    if (raw.startswith("\"") and raw.endswith("\"")) or (raw.startswith("'") and raw.endswith("'")):
        raw = raw[1:-1]
    # Etherscan sometimes wraps with extra braces {{...}}
    if raw.startswith("{{") and raw.endswith("}}"):
        raw = raw[1:-1]
    try:
        meta = json.loads(raw)
        # Solidity standard JSON: {"sources": {"path": {"content": "..."}}}
        if isinstance(meta, dict):
            sources = meta.get("sources")
            if isinstance(sources, dict) and sources:
                parts: List[str] = []
                for path, entry in sources.items():
                    content = None
                    if isinstance(entry, dict):
                        content = entry.get("content") or entry.get("source")
                    if not content:
                        continue
                    parts.append(f"// File: {path}\n\n{content}\n")
                if parts:
                    return "\n\n".join(parts)
        # Fallback: if JSON but no recognized structure, return raw text
    except Exception:
        # Not JSON, treat as flat source
        pass
    return text


def fetch_source(session: requests.Session, api_key: str, address: str, network: str = "mainnet") -> Optional[Dict[str, Any]]:
    url = "https://api.etherscan.io/api"
    params = {
        "module": "contract",
        "action": "getsourcecode",
        "address": address,
        "apikey": api_key,
    }
    # Basic retry with backoff
    backoff = 1.0
    for _ in range(5):
        try:
            r = session.get(url, params=params, timeout=30)
            if r.status_code == 200:
                data = r.json()
                if str(data.get("status")) == "1" and data.get("result"):
                    rec = data["result"][0]
                    return rec
                elif str(data.get("status")) == "0" and data.get("message") == "NOTOK":
                    # rate limit or not verified
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 10)
                else:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 10)
            elif r.status_code in (429, 500, 502, 503):
                time.sleep(backoff)
                backoff = min(backoff * 2, 10)
            else:
                time.sleep(backoff)
                backoff = min(backoff * 2, 10)
        except Exception:
            time.sleep(backoff)
            backoff = min(backoff * 2, 10)
    return None


def load_addresses(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    addrs: List[str] = []
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict) and "address" in item:
                addrs.append(str(item["address"]).strip())
            elif isinstance(item, str):
                addrs.append(item.strip())
    return addrs


def _done_list_path(out_dir: str) -> str:
    return os.path.join(out_dir, "_downloaded_addresses.txt")


def _load_done_set(out_dir: str) -> set:
    done: set = set()
    path = _done_list_path(out_dir)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    addr = line.strip().lower()
                    if addr:
                        done.add(addr)
        except Exception:
            pass
    return done


def _append_done(out_dir: str, address: str) -> None:
    try:
        with open(_done_list_path(out_dir), "a", encoding="utf-8") as f:
            f.write(address.lower() + "\n")
    except Exception:
        pass


def download_all(before_list: str, after_list: str, out_before: str, out_after: str, api_key: str, rps: float = 5.0, resume: bool = True) -> None:
    os.makedirs(out_before, exist_ok=True)
    os.makedirs(out_after, exist_ok=True)

    session = requests.Session()
    session.headers.update({"User-Agent": "contract-downloader/1.0"})

    def process(addrs: List[str], out_dir: str) -> int:
        written = 0
        done_set = _load_done_set(out_dir) if resume else set()
        min_sleep = 1.0 / max(rps, 0.1)
        for idx, addr in enumerate(addrs):
            addr = addr.lower()
            if not addr or not addr.startswith("0x"):
                continue
            if addr in done_set:
                continue
            rec = fetch_source(session, api_key, addr)
            # throttle to requested rps
            time.sleep(min_sleep)
            if not rec:
                continue
            name = rec.get("ContractName") or rec.get("contractName") or "contract"
            name = sanitize_filename(os.path.splitext(os.path.basename(str(name)))[0])
            combined = concat_sources_from_metadata(rec.get("SourceCode", ""))
            if not combined:
                continue
            filename = f"{name}.sol"
            out_path = ensure_unique(os.path.join(out_dir, filename))
            try:
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(f"// Address: {addr}\n")
                    f.write(combined)
                written += 1
                if resume:
                    done_set.add(addr)
                    _append_done(out_dir, addr)
            except Exception:
                continue
        return written

    before_addrs = load_addresses(before_list)
    after_addrs = load_addresses(after_list)

    print(f"Before list: {len(before_addrs)} addresses → {out_before}")
    wrote_before = process(before_addrs, out_before)
    print(f"Wrote {wrote_before} files to {out_before}")

    print(f"After list: {len(after_addrs)} addresses → {out_after}")
    wrote_after = process(after_addrs, out_after)
    print(f"Wrote {wrote_after} files to {out_after}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Download verified contract sources from Etherscan for two address lists")
    parser.add_argument("--before", default="/Users/lichang/projects/llm4sc-longtailed/llm4sclongtailed/address_lists/addresses_20220403_to_20250429.json")
    parser.add_argument("--after", default="/Users/lichang/projects/llm4sc-longtailed/llm4sclongtailed/address_lists/addresses_on_or_after_20250430.json")
    parser.add_argument("--out_before", default=os.path.expanduser("~/Desktop/Verfied-dataset/trainingllm"))
    parser.add_argument("--out_after", default="/Users/lichang/projects/llm4sc-longtailed/llm4sclongtailed/dataset/sc2-20250430")
    parser.add_argument("--api_key", default=ETHERSCAN_API_KEY)
    parser.add_argument("--rps", type=float, default=5.0, help="Requests per second throttle")
    parser.add_argument("--no_resume", action="store_true", help="Disable resume/skip of already-downloaded addresses")
    args = parser.parse_args()


    download_all(args.before, args.after, args.out_before, args.out_after, args.api_key, rps=args.rps, resume=not args.no_resume)


if __name__ == "__main__":
    main()


