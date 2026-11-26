import asyncio
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import kohinoor_monitor
import monitor

PRODUCT = {
    "id": "product-1171-hmt-kohinoor-quartz-b1-dark-blue-sunray",
    "name": "HMT Kohinoor Quartz B1 Dark Blue Sunray",
    "url": "https://www.hmtwatches.in/product-1171-hmt-kohinoor-quartz-b1-dark-blue-sunray",
    "purchase_root_selector": "div._product-detail-content",
    "price_selector": ".price, .product-price, ._p-price-box",
}

OOS_HTML = """
<div class="_product-detail-content">
  <span class="_p-price-box">MRP 2899</span>
  <div>Out Of Stock</div>
  <div class="action">
    <a id="notifyMe" class="add-to-cart btn btn-default" type="button">
      <span class="fa fa-shopping-cart">NOTIFY ME</span>
    </a>
  </div>
</div>
"""

IN_STOCK_HTML = """
<div class="_product-detail-content">
  <span class="_p-price-box">MRP 2899</span>
  <div class="action">
    <a id class="add-to-cart btn btn-default update_cart_product" type="button">
      <span class="fa fa-shopping-cart">add to cart</span>
    </a>
  </div>
</div>
"""

CONTRADICTORY_HTML = """
<div class="_product-detail-content">
  <div>Out Of Stock</div>
  <div class="action">
    <a id="notifyMe" class="add-to-cart" type="button">NOTIFY ME</a>
    <a class="update_cart_product" type="button">add to cart</a>
  </div>
</div>
"""

CAPTCHA_HTML = "<html><body><h1>Verify you are human - Cloudflare</h1></body></html>"


class TestHMTStockTracker(unittest.TestCase):
    def test_notify_me_with_add_to_cart_class_is_oos(self):
        res = monitor.parse_html(OOS_HTML, PRODUCT)
        self.assertEqual(res.status, "out_of_stock")
        self.assertEqual(res.price, "MRP 2899")

    def test_update_cart_product_is_in_stock(self):
        res = monitor.parse_html(IN_STOCK_HTML, PRODUCT)
        self.assertEqual(res.status, "in_stock")
        self.assertEqual(res.price, "MRP 2899")

    def test_contradictory_signals_return_unknown(self):
        res = monitor.parse_html(CONTRADICTORY_HTML, PRODUCT)
        self.assertEqual(res.status, "unknown")

    def test_captcha_page_returns_unknown(self):
        res = monitor.parse_html(CAPTCHA_HTML, PRODUCT)
        self.assertEqual(res.status, "unknown")

    def test_alert_deduplication_cooldown_and_retry(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.json"
            state_path.write_text(json.dumps({
                "_baseline_done": True,
                "items": {
                    PRODUCT["id"]: {"status": "out_of_stock", "last_alerted_at": 0}
                },
            }))

            fake_in_stock = monitor.CheckResult(
                PRODUCT["id"], PRODUCT["name"], PRODUCT["url"], "in_stock", "MRP 2899"
            )

            async def fake_discover(_client, _kw):
                return [PRODUCT]

            async def fake_check(_client, _sem, _prod):
                return fake_in_stock

            with patch.object(kohinoor_monitor, "STATE_FILE", state_path), \
                 patch.object(kohinoor_monitor, "discover_urls", side_effect=fake_discover), \
                 patch.object(kohinoor_monitor, "check_one_async", side_effect=fake_check):

                # 1. Failed Telegram call must NOT mark watch as in_stock (so it retries)
                with patch.object(monitor, "send_telegram", side_effect=RuntimeError("net down")):
                    asyncio.run(kohinoor_monitor.run_collection_cycle_async())
                saved = json.loads(state_path.read_text())
                self.assertEqual(saved["items"][PRODUCT["id"]]["status"], "out_of_stock")

                # 2. Next cycle succeeds -> fires exactly 1 alert and marks in_stock
                with patch.object(monitor, "send_telegram") as mock_send:
                    asyncio.run(kohinoor_monitor.run_collection_cycle_async())
                    self.assertEqual(mock_send.call_count, 1)

                # 3. Immediate repeat while still in_stock -> 0 duplicate alerts
                with patch.object(monitor, "send_telegram") as mock_send:
                    asyncio.run(kohinoor_monitor.run_collection_cycle_async())
                    self.assertEqual(mock_send.call_count, 0)

                # 4. Flapping OOS -> in_stock inside 15-min cooldown -> 0 spam alerts
                saved = json.loads(state_path.read_text())
                saved["items"][PRODUCT["id"]]["status"] = "out_of_stock"
                saved["items"][PRODUCT["id"]]["last_alerted_at"] = time.time()
                state_path.write_text(json.dumps(saved))

                with patch.object(monitor, "send_telegram") as mock_send:
                    asyncio.run(kohinoor_monitor.run_collection_cycle_async())
                    self.assertEqual(mock_send.call_count, 0)


if __name__ == "__main__":
    unittest.main()