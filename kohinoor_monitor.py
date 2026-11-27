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
STORE_KOHINOOR_PAGES = [
    ("store-quartz-b1", "HMT Kohinoor Quartz B1 [hmtwatches.store]", "https://www.hmtwatches.store/product/0035cf80-48d5-4cf3-a02f-1f36b01071a5", "₹2899"),
    ("store-quartz-lb", "HMT Kohinoor Quartz B Light Blue Sunray [hmtwatches.store]", "https://www.hmtwatches.store/product/9420ec5f-9703-4fcd-8495-1f051c4c6d56", "₹2899"),
    ("store-quartz-sunray", "HMT Kohinoor Quartz Sunray [hmtwatches.store]", "https://www.hmtwatches.store/product/3e3d31b3-236d-4611-bb9d-fffb268df514", "₹2599"),
]
SEARCH_KEYWORDS = ["kohinoor"]
STATE_FILE = Path("kohinoor_state.json")
MAX_CONCURRENCY = 1            # 1 at a time + stagger prevents HTTP 429 bursts
PER_PAGE_STAGGER_SECONDS = 3.0
DISCOVERY_TTL_SECONDS = 1800   # Refresh hmtwatches.in URL list every 30 minutes
ERROR_ALERT_THRESHOLD = 10
RATE_LIMIT_COOLDOWN = 300      # 5-minute pause on hmtwatches.in if 429 occurs

_DISCOVERY_CACHE: dict[str, tuple[float, list[dict]]] = {}
_HMT_IN_BACKOFF_UNTIL = 0.0


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {"_baseline_done": False, "items": {}}
    return json.loads(STATE_FILE.read_text(encoding="utf-8"))


def save_state(state: dict) -> None:
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp.replace(STATE_FILE)


def send_telegram_notice(text: str) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    try:
        httpx.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=15.0,
        )
    except httpx.HTTPError as exc:
        print(f"Notice send failed: {exc}")


async def discover_urls(client: httpx.AsyncClient, keyword: str) -> list[dict]:
    seen: set[str] = set()
    watches: list[dict] = []

    def add_watch(full_url: str, fallback_name: str = "") -> None:
        clean_url = full_url.split("?")[0].strip()
        if (
            not clean_url
            or clean_url in seen
            or keyword.lower() not in clean_url.lower()
            or "automatic" in clean_url.lower()
        ):
            return
        seen.add(clean_url)
        slug_match = re.search(r"(product-\d+-[a-z0-9-]+)", clean_url, re.I)
        watch_id = slug_match.group(1).lower() if slug_match else clean_url
        pretty_name = fallback_name or re.sub(r"^product-\d+-", "", watch_id).replace("-", " ").title()
        watches.append({
            "id": watch_id,
            "name": pretty_name,
            "url": clean_url,
            "purchase_root_selector": "div._product-detail-content",
            "price_selector": ".price, .product-price, ._p-price-box",
        })

    if Path("products.json").exists():
        for item in json.loads(Path("products.json").read_text(encoding="utf-8")):
            if item.get("enabled", True):
                add_watch(item["url"], item.get("name", ""))

    # Fetch sitemap + search gently (sequential) so discovery never triggers 429
    for u in (
        f"{BASE_URL}sitemap.xml",
        f"{BASE_URL}search_products?keys={keyword}&_t={int(time.time())}",
    ):
        try:
            resp = await client.get(u, headers=monitor.HEADERS, timeout=25.0)
            if resp.status_code == 200:
                for slug in re.findall(r"https?://[^\"'<\s]*product-\d+-[a-z0-9-]+", resp.text, re.I):
                    add_watch(slug)
                for rel in re.findall(r"(?:href=[\"\']|/)?(product-\d+-[a-z0-9-]+)", resp.text, re.I):
                    add_watch(urljoin(BASE_URL, rel.lstrip("/")))
        except httpx.HTTPError:
            continue
        await asyncio.sleep(2.0)

    return watches


async def get_cached_urls(client: httpx.AsyncClient, keyword: str) -> list[dict]:
    cached = _DISCOVERY_CACHE.get(keyword)
    now = time.time()
    if cached and (now - cached[0]) < DISCOVERY_TTL_SECONDS:
        return cached[1]
    watches = await discover_urls(client, keyword)
    # Keep previous fuller cache if discovery was temporarily rate-limited
    if cached and len(watches) < len(cached[1]):
        return cached[1]
    if watches:
        _DISCOVERY_CACHE[keyword] = (now, watches)
    return watches


async def check_one_async(
    client: httpx.AsyncClient, sem: asyncio.Semaphore, product: dict
) -> monitor.CheckResult:
    global _HMT_IN_BACKOFF_UNTIL
    async with sem:
        try:
            sep = "&" if "?" in product["url"] else "?"
            resp = await client.get(
                f"{product['url']}{sep}_t={int(time.time())}",
                headers=monitor.HEADERS,
                timeout=25.0,
            )
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", RATE_LIMIT_COOLDOWN))
                wait_s = max(retry_after, RATE_LIMIT_COOLDOWN)
                _HMT_IN_BACKOFF_UNTIL = time.time() + wait_s
                print(f"  [hmtwatches.in] HTTP 429 — pausing hmtwatches.in for {wait_s}s")
                return monitor.CheckResult(product["id"], product["name"], product["url"], "unknown")
            if resp.status_code == 404:
                return monitor.CheckResult(product["id"], product["name"], product["url"], "removed")
            if resp.status_code != 200:
                return monitor.CheckResult(product["id"], product["name"], product["url"], "unknown")

            first = monitor.parse_html(resp.text, product)

            if first.status == "in_stock":
                await asyncio.sleep(2.0)
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


async def check_hmt_store(client: httpx.AsyncClient) -> list[monitor.CheckResult]:
    """Checks the 3 direct Kohinoor Quartz product pages on hmtwatches.store."""
    results: list[monitor.CheckResult] = []
    for pid, name, url, fallback_price in STORE_KOHINOOR_PAGES:
        try:
            resp = await client.get(f"{url}?_t={int(time.time())}", headers=monitor.HEADERS, timeout=25.0)
            if resp.status_code != 200:
                print(f"[hmtwatches.store] {pid} HTTP {resp.status_code}")
                continue
            tag = BeautifulSoup(resp.text, "lxml").select_one("#__NEXT_DATA__")
            if not tag:
                continue
            raw_json = tag.get_text()
            # Any variant on this Kohinoor Quartz page having inStock:true means a dial is live!
            in_stock = bool(re.search(r'"inStock"\s*:\s*true', raw_json))
            results.append(
                monitor.CheckResult(
                    product_id=pid,
                    name=name,
                    url=url,
                    status="in_stock" if in_stock else "out_of_stock",
                    price=fallback_price,
                )
            )
        except httpx.HTTPError as exc:
            print(f"[hmtwatches.store] {pid} error: {exc}")
        await asyncio.sleep(1.0)

    in_stock_count = sum(1 for r in results if r.status == "in_stock")
    print(f"[hmtwatches.store] Checked {len(results)} Kohinoor Quartz pages ({in_stock_count} in stock)")
    return results


def maybe_send_daily_heartbeat(state: dict, watch_count: int) -> None:
    now_ist = monitor.datetime.now(monitor.IST)
    today = now_ist.strftime("%Y-%m-%d")
    minutes = now_ist.hour * 60 + now_ist.minute
    if (8 * 60 + 45) <= minutes <= (9 * 60) and state.get("_last_heartbeat_date") != today:
        send_telegram_notice(
            f"HMT Tracker alive — watching {watch_count} Kohinoor Quartz pages across hmtwatches.in & hmtwatches.store."
        )
        state["_last_heartbeat_date"] = today


async def run_collection_cycle_async() -> bool:
    state = load_state()
    items = state.setdefault("items", {})
    baseline_done = state.get("_baseline_done", False)
    now = time.time()
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    all_results: list[monitor.CheckResult] = []

    async with httpx.AsyncClient(http2=True, follow_redirects=True) as client:
        # 1. Check hmtwatches.in gently (1 by 1 with 3s stagger, stops immediately on 429)
        if time.time() >= _HMT_IN_BACKOFF_UNTIL:
            for kw in SEARCH_KEYWORDS:
                watches = await get_cached_urls(client, kw)
                if not watches:
                    continue
                site_results: list[monitor.CheckResult] = []
                for w in watches:
                    if time.time() < _HMT_IN_BACKOFF_UNTIL:
                        break  # Circuit breaker tripped -> stop immediately
                    res = await check_one_async(client, sem, w)
                    site_results.append(res)
                    await asyncio.sleep(PER_PAGE_STAGGER_SECONDS)
                in_stock_count = sum(1 for r in site_results if r.status == "in_stock")
                print(f"[hmtwatches.in:{kw}] Checked {len(site_results)} Quartz pages ({in_stock_count} in stock)")
                all_results.extend(site_results)
        else:
            rem = int(_HMT_IN_BACKOFF_UNTIL - time.time())
            print(f"[hmtwatches.in] Cooling down from 429 ({rem}s left) — checking hmtwatches.store only")

        # 2. Check hmtwatches.store (Amazon SmartBiz — never rate-limits)
        store_results = await check_hmt_store(client)
        all_results.extend(store_results)

    valid_results = [r for r in all_results if r.status != "unknown"]
    for result in valid_results:
        entry = items.get(result.product_id, {"status": "unknown", "last_alerted_at": 0})
        prev_status = entry["status"]

        cooldown_ok = (now - entry.get("last_alerted_at", 0)) >= monitor.COOLDOWN_SECONDS
        should_alert = (
            baseline_done
            and result.status == "in_stock"
            and prev_status != "in_stock"
            and cooldown_ok
        )

        if should_alert:
            try:
                monitor.send_telegram(result)
                entry["last_alerted_at"] = now
                print(f"  RESTOCK ALERT SENT: {result.name} ({result.price})")
            except Exception as exc:
                print(f"  Telegram failed for {result.name}: {exc} (will retry next cycle)")
                continue

        entry["status"] = result.status
        entry["last_checked_at"] = now
        items[result.product_id] = entry

    if not valid_results:
        errors = state.get("_consecutive_errors", 0) + 1
        state["_consecutive_errors"] = errors
        if errors == ERROR_ALERT_THRESHOLD:
            send_telegram_notice(
                "HMT Tracker warning: 10 consecutive check cycles failed across both stores."
            )
    else:
        state["_consecutive_errors"] = 0
        state["_baseline_done"] = True
        maybe_send_daily_heartbeat(state, len(valid_results))

    save_state(state)
    return len(valid_results) > 0


def run_collection_cycle() -> bool:
    return asyncio.run(run_collection_cycle_async())


def get_poll_interval() -> float:
    """Gentle pause between staggered cycles (total cycle ~45-60s peak, ~90s off-peak)."""
    now_ist = monitor.datetime.now(monitor.IST)
    minutes = now_ist.hour * 60 + now_ist.minute
    in_morning_peak = (8 * 60 + 45) <= minutes <= (12 * 60 + 30)
    in_evening_peak = (17 * 60 + 45) <= minutes <= (19 * 60 + 0)

    if in_morning_peak or in_evening_peak:
        base = int(os.getenv("PEAK_POLL_SECONDS", "25"))
    else:
        base = int(os.getenv("POLL_SECONDS", "60"))
    return max(20.0, base + random.uniform(-0.15 * base, 0.15 * base))


if __name__ == "__main__":
    monitor.load_env_file()
    max_runtime = int(os.getenv("MAX_RUNTIME_SECONDS", "0"))
    deadline = time.time() + max_runtime if max_runtime > 0 else None

    while True:
        healthy = run_collection_cycle()
        if os.getenv("LOOP") != "1":
            break
        sleep_s = get_poll_interval() if healthy else 180.0
        if deadline and (time.time() + sleep_s) >= deadline:
            break
        time.sleep(sleep_s)