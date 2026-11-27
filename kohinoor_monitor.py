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
STORE_KOHINOOR_URL = "https://www.hmtwatches.store/collection/c757ecc9-31e2-4cf7-bcb1-9b29f5c57c41/Kohinoor"
SEARCH_KEYWORDS = ["kohinoor"]  # Add "pilot", "janata", etc. anytime
STATE_FILE = Path("kohinoor_state.json")
MAX_CONCURRENCY = 2
DISCOVERY_TTL_SECONDS = 600  # Refresh hmtwatches.in URL list every 10 minutes
ERROR_ALERT_THRESHOLD = 10   # Warn after 10 consecutive failed cycles

_DISCOVERY_CACHE: dict[str, tuple[float, list[dict]]] = {}


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {"_baseline_done": False, "items": {}}
    return json.loads(STATE_FILE.read_text(encoding="utf-8"))


def save_state(state: dict) -> None:
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp.replace(STATE_FILE)


def send_telegram_notice(text: str) -> None:
    """Sends a lightweight status/heartbeat notice without touching restock logic."""
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

    sources = [
        f"{BASE_URL}search_products?keys={keyword}&_t={int(time.time())}",
        f"{BASE_URL}all_product?type=shop_collection&id=51&_t={int(time.time())}",
        f"{BASE_URL}sitemap.xml",
    ]
    responses = await asyncio.gather(
        *(client.get(u, headers=monitor.HEADERS, timeout=25.0) for u in sources),
        return_exceptions=True,
    )

    for resp in responses:
        if isinstance(resp, Exception) or resp.status_code != 200:
            continue
        for slug in re.findall(r"https?://[^\"'<\s]*product-\d+-[a-z0-9-]+", resp.text, re.I):
            add_watch(slug)
        for rel in re.findall(r"(?:href=[\"\']|/)?(product-\d+-[a-z0-9-]+)", resp.text, re.I):
            add_watch(urljoin(BASE_URL, rel.lstrip("/")))

    return watches


async def get_cached_urls(client: httpx.AsyncClient, keyword: str) -> list[dict]:
    cached = _DISCOVERY_CACHE.get(keyword)
    now = time.time()
    if cached and (now - cached[0]) < DISCOVERY_TTL_SECONDS:
        return cached[1]
    watches = await discover_urls(client, keyword)
    if watches:
        _DISCOVERY_CACHE[keyword] = (now, watches)
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


async def check_hmt_store(client: httpx.AsyncClient) -> list[monitor.CheckResult]:
    """Checks all Kohinoor Quartz watches on hmtwatches.store in 1 single request."""
    try:
        resp = await client.get(
            f"{STORE_KOHINOOR_URL}?_t={int(time.time())}",
            headers=monitor.HEADERS,
            timeout=25.0,
        )
        if resp.status_code != 200:
            print(f"[hmtwatches.store] HTTP {resp.status_code} — skipping")
            return []
        tag = BeautifulSoup(resp.text, "lxml").select_one("#__NEXT_DATA__")
        if not tag:
            return []
        data = json.loads(tag.get_text())
    except (httpx.HTTPError, json.JSONDecodeError) as exc:
        print(f"[hmtwatches.store] Error: {exc}")
        return []

    seen_skus: set[str] = set()
    results: list[monitor.CheckResult] = []

    def walk(node) -> None:
        if isinstance(node, dict):
            sku = node.get("sku")
            name = node.get("name")
            opts = node.get("buyingOptions")
            if isinstance(sku, str) and isinstance(name, str) and isinstance(opts, dict):
                low = name.lower()
                if "kohinoor" in low and "automatic" not in low and sku not in seen_skus:
                    seen_skus.add(sku)
                    avail = (opts.get("singlePurchase") or {}).get("availability") or {}
                    in_stock = bool(avail.get("inStock") and avail.get("isBuyable"))
                    price_val = node.get("sellingPrice") or node.get("mrp") or "N/A"
                    price = f"₹{price_val}" if price_val != "N/A" else "N/A"
                    results.append(
                        monitor.CheckResult(
                            product_id=f"store-{sku}",
                            name=f"{name} [hmtwatches.store]",
                            url=f"https://www.hmtwatches.store/product/{sku}",
                            status="in_stock" if in_stock else "out_of_stock",
                            price=price,
                        )
                    )
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(data)
    in_stock_count = sum(1 for r in results if r.status == "in_stock")
    print(f"[hmtwatches.store] Checked {len(results)} Kohinoor Quartz listings ({in_stock_count} in stock)")
    return results


def maybe_send_daily_heartbeat(state: dict, watch_count: int) -> None:
    """Sends one daily 'alive' ping between 8:45 AM and 9:00 AM IST."""
    now_ist = monitor.datetime.now(monitor.IST)
    today = now_ist.strftime("%Y-%m-%d")
    minutes = now_ist.hour * 60 + now_ist.minute
    if (8 * 60 + 45) <= minutes <= (9 * 60) and state.get("_last_heartbeat_date") != today:
        peak_s = os.getenv("PEAK_POLL_SECONDS", "35")
        send_telegram_notice(
            f"HMT Tracker alive — watching {watch_count} Kohinoor Quartz listings across hmtwatches.in & hmtwatches.store ({peak_s}s peak mode)."
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
        # 1. Check hmtwatches.in Kohinoor Quartz pages
        for kw in SEARCH_KEYWORDS:
            watches = await get_cached_urls(client, kw)
            if not watches:
                continue
            results = await asyncio.gather(*(check_one_async(client, sem, w) for w in watches))
            in_stock_count = sum(1 for r in results if r.status == "in_stock")
            print(f"[hmtwatches.in:{kw}] Checked {len(results)} Quartz pages ({in_stock_count} in stock)")
            all_results.extend(results)

        # 2. Check hmtwatches.store Kohinoor Quartz listings (1 single request)
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
                "HMT Tracker warning: 10 consecutive check cycles failed (possible HMT outage or IP throttle)."
            )
    else:
        state["_consecutive_errors"] = 0
        state["_baseline_done"] = True
        maybe_send_daily_heartbeat(state, len(all_results))

    save_state(state)
    return len(valid_results) > 0


def run_collection_cycle() -> bool:
    return asyncio.run(run_collection_cycle_async())


def get_poll_interval() -> float:
    """35s during HMT's peak IST drop windows, 60s off-peak."""
    now_ist = monitor.datetime.now(monitor.IST)
    minutes = now_ist.hour * 60 + now_ist.minute
    in_morning_peak = (8 * 60 + 45) <= minutes <= (12 * 60 + 30)   # 8:45 AM - 12:30 PM IST
    in_evening_peak = (17 * 60 + 45) <= minutes <= (19 * 60 + 0)   # 5:45 PM - 7:00 PM IST

    if in_morning_peak or in_evening_peak:
        base = int(os.getenv("PEAK_POLL_SECONDS", "35"))
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