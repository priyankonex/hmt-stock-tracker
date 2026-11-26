import asyncio
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
MAX_CONCURRENCY = 4


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {"_baseline_done": False, "items": {}}
    return json.loads(STATE_FILE.read_text(encoding="utf-8"))


def save_state(state: dict) -> None:
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp.replace(STATE_FILE)


async def discover_urls(client: httpx.AsyncClient, keyword: str) -> list[dict]:
    url = f"{BASE_URL}search_products?keys={keyword}&_t={int(time.time())}"
    try:
        resp = await client.get(url, headers=monitor.HEADERS, timeout=25.0)
        if resp.status_code != 200:
            print(f"[{keyword}] Search HTTP {resp.status_code} — skipping discovery")
            return []
    except httpx.HTTPError as exc:
        print(f"[{keyword}] Search network timeout/error ({exc}) — will retry next cycle")
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    seen: set[str] = set()
    watches: list[dict] = []

    for link in soup.select('a[href*="product-"]'):
        full_url = urljoin(BASE_URL, link.get("href", "").strip()).split("?")[0]
        if full_url in seen or keyword.lower() not in full_url.lower():
            continue
        seen.add(full_url)

        slug_match = re.search(r"(product-\d+-[a-z0-9-]+)", full_url, re.I)
        watch_id = slug_match.group(1).lower() if slug_match else full_url
        # Clean human-readable fallback name from URL slug
        pretty_name = re.sub(r"^product-\d+-", "", watch_id).replace("-", " ").title()

        watches.append({
            "id": watch_id,
            "name": pretty_name,
            "url": full_url,
            "purchase_root_selector": "div._product-detail-content",
            "price_selector": ".price, .product-price, ._p-price-box",
        })

    return watches


async def check_one_async(
    client: httpx.AsyncClient, sem: asyncio.Semaphore, product: dict
) -> monitor.CheckResult:
    async with sem:
        try:
            sep = "&" if "?" in product["url"] else "?"
            resp = await client.get(
                f"{product['url']}{sep}_t={int(time.time())}",
                headers=monitor.HEADERS,
                timeout=25.0,
            )
            if resp.status_code == 404:
                return monitor.CheckResult(product["id"], product["name"], product["url"], "removed")
            if resp.status_code != 200:
                return monitor.CheckResult(product["id"], product["name"], product["url"], "unknown")

            first = monitor.parse_html(resp.text, product)

            # Fast 1.5s confirmation re-check before alerting
            if first.status == "in_stock":
                await asyncio.sleep(1.5)
                resp2 = await client.get(
                    f"{product['url']}{sep}_t={int(time.time())}",
                    headers=monitor.HEADERS,
                    timeout=25.0,
                )
                if resp2.status_code == 200:
                    second = monitor.parse_html(resp2.text, product)
                    if second.status != "in_stock":
                        return second
            return first
        except httpx.HTTPError as exc:
            print(f"  [{product['id']}] Timeout/error: {exc}")
            return monitor.CheckResult(product["id"], product["name"], product["url"], "unknown")


async def run_collection_cycle_async() -> None:
    state = load_state()
    items = state.setdefault("items", {})
    baseline_done = state.get("_baseline_done", False)
    now = time.time()
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    async with httpx.AsyncClient(http2=True, follow_redirects=True) as client:
        for kw in SEARCH_KEYWORDS:
            watches = await discover_urls(client, kw)
            if not watches:
                continue

            results = await asyncio.gather(*(check_one_async(client, sem, w) for w in watches))
            in_stock_count = sum(1 for r in results if r.status == "in_stock")
            print(f"[{kw}] Checked {len(results)} product pages directly ({in_stock_count} in stock)")

            for result in results:
                entry = items.get(result.product_id, {"status": "unknown", "last_alerted_at": 0})
                prev_status = entry["status"]

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
                    print(f"  RESTOCK ALERT SENT: {result.name} ({result.price})")

                entry["status"] = result.status
                entry["last_checked_at"] = now
                items[result.product_id] = entry

    state["_baseline_done"] = True
    save_state(state)


def run_collection_cycle() -> None:
    asyncio.run(run_collection_cycle_async())


def get_poll_interval() -> float:
    """15s during HMT's peak IST drop windows, 45s off-peak."""
    now_ist = monitor.datetime.now(monitor.IST)
    minutes = now_ist.hour * 60 + now_ist.minute
    in_morning_peak = (8 * 60 + 45) <= minutes <= (12 * 60 + 30)   # 8:45 AM - 12:30 PM IST
    in_evening_peak = (17 * 60 + 45) <= minutes <= (19 * 60 + 0)   # 5:45 PM - 7:00 PM IST

    if in_morning_peak or in_evening_peak:
        base = int(os.getenv("PEAK_POLL_SECONDS", "15"))
    else:
        base = int(os.getenv("POLL_SECONDS", "45"))
    return max(12.0, base + random.uniform(-0.15 * base, 0.15 * base))


if __name__ == "__main__":
    monitor.load_env_file()
    max_runtime = int(os.getenv("MAX_RUNTIME_SECONDS", "0"))
    deadline = time.time() + max_runtime if max_runtime > 0 else None

    while True:
        run_collection_cycle()
        if os.getenv("LOOP") != "1":
            break
        sleep_s = get_poll_interval()
        if deadline and (time.time() + sleep_s) >= deadline:
            break
        time.sleep(sleep_s)