import json
import os
import random
import re
import time
from pathlib import Path
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

import monitor

BASE_URL = "https://www.hmtwatches.in/"
SEARCH_KEYWORDS = ["kohinoor"]  # Add "pilot", "janata", etc. anytime
STATE_FILE = Path("kohinoor_state.json")
CARD_OOS_WORDS = ("out of stock", "coming soon", "sold out", "notify me")


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {"_baseline_done": False, "items": {}}
    return json.loads(STATE_FILE.read_text(encoding="utf-8"))


def save_state(state: dict) -> None:
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp.replace(STATE_FILE)


def scan_keyword(client: httpx.Client, keyword: str) -> list[dict]:
    url = f"{BASE_URL}search_products?keys={keyword}&_t={int(time.time())}"
    resp = client.get(url, headers=monitor.HEADERS, timeout=15.0)
    if resp.status_code != 200:
        print(f"[{keyword}] Search HTTP {resp.status_code} — skipping")
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    seen_urls: set[str] = set()
    watches: list[dict] = []

    for link in soup.select('a[href*="product-"]'):
        href = link.get("href", "").strip()
        full_url = urljoin(BASE_URL, href).split("?")[0]
        if full_url in seen_urls or keyword.lower() not in full_url.lower():
            continue
        seen_urls.add(full_url)

        card = link.parent.parent or link.parent
        card_text = card.get_text(" | ", strip=True)
        card_lower = card_text.lower()

        # Extract slug as unique ID and first text segment as watch name
        slug_match = re.search(r"(product-\d+-[a-z0-9-]+)", full_url, re.I)
        watch_id = slug_match.group(1).lower() if slug_match else full_url
        name = card_text.split(" | ")[0] if card_text else watch_id
        card_looks_oos = any(word in card_lower for word in CARD_OOS_WORDS)

        watches.append({
            "id": watch_id,
            "name": name,
            "url": full_url,
            "card_looks_oos": card_looks_oos,
            "purchase_root_selector": "div._product-detail-content",
            "price_selector": ".price, .product-price, ._p-price-box",
        })

    return watches


def run_collection_cycle() -> None:
    state = load_state()
    items = state.setdefault("items", {})
    baseline_done = state.get("_baseline_done", False)
    now = time.time()

    with httpx.Client(http2=True, follow_redirects=True) as client:
        for kw in SEARCH_KEYWORDS:
            watches = scan_keyword(client, kw)
            print(f"[{kw}] Scanned {len(watches)} watches on page 1")

            for w in watches:
                entry = items.get(w["id"], {"status": "unknown", "last_alerted_at": 0})
                prev_status = entry["status"]

                if w["card_looks_oos"]:
                    entry["status"] = "out_of_stock"
                    entry["last_checked_at"] = now
                    items[w["id"]] = entry
                    continue

                # Card does NOT say Out Of Stock / Coming Soon -> verify on product page!
                result = monitor.check_stock(client, w)
                print(f"  VERIFY {result.name} | {prev_status} -> {result.status} | {result.price}")

                if result.status == "unknown":
                    continue

                cooldown_ok = (now - entry.get("last_alerted_at", 0)) >= monitor.COOLDOWN_SECONDS
                should_alert = (
                    baseline_done
                    and result.status == "in_stock"
                    and prev_status != "in_stock"
                    and cooldown_ok
                )

                if should_alert:
                    monitor.send_telegram(result)
                    entry["last_alerted_at"] = now
                    print(f"  RESTOCK ALERT SENT: {result.name}")

                entry["status"] = result.status
                entry["last_checked_at"] = now
                items[w["id"]] = entry
                time.sleep(random.uniform(1.0, 2.0))

    state["_baseline_done"] = True
    save_state(state)


if __name__ == "__main__":
    monitor.load_env_file()
    max_runtime = int(os.getenv("MAX_RUNTIME_SECONDS", "0"))
    deadline = time.time() + max_runtime if max_runtime > 0 else None

    while True:
        run_collection_cycle()
        if os.getenv("LOOP") != "1":
            break
        base = int(os.getenv("POLL_SECONDS", "35"))
        sleep_s = max(20.0, base + random.uniform(-0.2 * base, 0.2 * base))
        if deadline and (time.time() + sleep_s) >= deadline:
            break
        time.sleep(sleep_s)