import json
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

import httpx
from bs4 import BeautifulSoup

Status = Literal["in_stock", "out_of_stock", "removed", "unknown"]

PRODUCTS_FILE = Path("products.json")
STATE_FILE = Path("state.json")
COOLDOWN_SECONDS = 900  # 15 minutes between repeat alerts per watch
IST = timezone(timedelta(hours=5, minutes=30))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-IN,en;q=0.9",
}

CHALLENGE_WORDS = ("captcha", "verify you are human", "access denied", "cloudflare")
NEGATIVE_PHRASES = ("out of stock", "sold out", "currently unavailable", "coming soon", "notify me")


@dataclass
class CheckResult:
    product_id: str
    name: str
    url: str
    status: Status
    price: str = "N/A"


def load_env_file() -> None:
    env_path = Path(".env")
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError("state.json is corrupted. Fix or delete it before running.") from exc


def save_state(state: dict) -> None:
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp.replace(STATE_FILE)


def extract_jsonld_availability(soup: BeautifulSoup) -> set[str]:
    found: set[str] = set()

    def walk(node) -> None:
        if isinstance(node, dict):
            avail = node.get("availability")
            if isinstance(avail, str):
                found.add(avail.lower())
            for val in node.values():
                walk(val)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    for tag in soup.select('script[type="application/ld+json"]'):
        raw = tag.string or tag.get_text()
        try:
            walk(json.loads(raw))
        except (json.JSONDecodeError, TypeError):
            continue
    return found


def parse_html(html_text: str, product: dict) -> CheckResult:
    soup = BeautifulSoup(html_text, "lxml")
    full_text = soup.get_text(" ", strip=True).lower()

    if any(word in full_text for word in CHALLENGE_WORDS):
        return CheckResult(product["id"], product["name"], product["url"], "unknown")

    price_el = soup.select_one(product.get("price_selector", ".price, .product-price"))
    price = price_el.get_text(" ", strip=True) if price_el else "N/A"

    availability = extract_jsonld_availability(soup)
    positive_json = any(v.endswith(("/instock", "#instock")) for v in availability)
    negative_json = any(v.endswith(("/outofstock", "#outofstock")) for v in availability)

    root = soup.select_one(product.get("purchase_root_selector", "main"))
    if root is None:
        if positive_json and not negative_json:
            return CheckResult(product["id"], product["name"], product["url"], "in_stock", price)
        if negative_json and not positive_json:
            return CheckResult(product["id"], product["name"], product["url"], "out_of_stock", price)
        return CheckResult(product["id"], product["name"], product["url"], "unknown", price)

    root_text = root.get_text(" ", strip=True).lower()
    negative_text = any(phrase in root_text for phrase in NEGATIVE_PHRASES)
    has_notify_btn = root.select_one("#notifyMe") is not None

    positive_button = False
    for btn in root.select("button, a, input[type=submit]"):
        label = f"{btn.get_text(' ', strip=True)} {btn.get('value', '')}".lower()
        is_disabled = (
            btn.has_attr("disabled")
            or btn.get("aria-disabled") == "true"
            or "disabled" in (btn.get("class") or [])
        )
        if re.search(r"add\s+to\s+cart|buy\s+now", label) and not is_disabled:
            positive_button = True
            break

    positive = positive_json or positive_button
    negative = negative_json or negative_text or has_notify_btn

    if positive and negative:
        status: Status = "unknown"
    elif positive:
        status = "in_stock"
    elif negative:
        status = "out_of_stock"
    else:
        status = "unknown"

    return CheckResult(product["id"], product["name"], product["url"], status, price)


def fetch_page(client: httpx.Client, url: str) -> httpx.Response:
    sep = "&" if "?" in url else "?"
    return client.get(f"{url}{sep}_t={int(time.time())}", headers=HEADERS, timeout=15.0)


def check_stock(client: httpx.Client, product: dict) -> CheckResult:
    try:
        resp = fetch_page(client, product["url"])
        if resp.status_code == 404:
            return CheckResult(product["id"], product["name"], product["url"], "removed")
        if resp.status_code in (403, 429) or resp.status_code != 200:
            print(f"[{product['id']}] HTTP {resp.status_code} — skipping")
            return CheckResult(product["id"], product["name"], product["url"], "unknown")

        first = parse_html(resp.text, product)

        if first.status == "in_stock":
            time.sleep(2.0 + random.uniform(0.2, 0.7))
            resp2 = fetch_page(client, product["url"])
            if resp2.status_code == 200:
                second = parse_html(resp2.text, product)
                if second.status != "in_stock":
                    return second
        return first
    except httpx.HTTPError as exc:
        print(f"[{product['id']}] Network error: {exc}")
        return CheckResult(product["id"], product["name"], product["url"], "unknown")


def send_telegram(result: CheckResult) -> None:
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    now_ist = datetime.now(IST).strftime("%d %b %Y, %I:%M:%S %p IST")

    message = (
        f"HMT RESTOCK DETECTED\n\n"
        f"Watch: {result.name}\n"
        f"Price: {result.price}\n"
        f"Detected: {now_ist}\n\n"
        f"Link: {result.url}"
    )

    resp = httpx.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": message},
        timeout=15.0,
    )
    resp.raise_for_status()
    if not resp.json().get("ok"):
        raise RuntimeError(f"Telegram API rejected message: {resp.text}")


def run_once() -> None:
    products = json.loads(PRODUCTS_FILE.read_text(encoding="utf-8"))
    state = load_state()
    now = time.time()

    with httpx.Client(http2=True, follow_redirects=True) as client:
        for product in products:
            if not product.get("enabled", True):
                continue

            result = check_stock(client, product)
            entry = state.get(result.product_id, {"status": "unknown", "last_alerted_at": 0})
            prev_status = entry["status"]
            cooldown_ok = (now - entry.get("last_alerted_at", 0)) >= COOLDOWN_SECONDS

            print(f"{result.name} | {prev_status} -> {result.status} | {result.price}")

            if result.status == "unknown":
                time.sleep(random.uniform(1.5, 3.0))
                continue

            if prev_status == "out_of_stock" and result.status == "in_stock" and cooldown_ok:
                send_telegram(result)
                entry["last_alerted_at"] = now
                print(f"RESTOCK ALERT SENT: {result.product_id}")

            entry["status"] = result.status
            entry["last_checked_at"] = now
            state[result.product_id] = entry

            save_state(state)
            time.sleep(random.uniform(1.5, 3.0))


if __name__ == "__main__":
    load_env_file()
    while True:
        run_once()
        if os.getenv("LOOP") != "1":
            break
        base = int(os.getenv("POLL_SECONDS", "35"))
        jitter = random.uniform(-0.2 * base, 0.2 * base)
        time.sleep(max(20.0, base + jitter))