"""
test_swatch_scraper.py — Quick integration test
================================================
Grabs the first 5 product URLs from the Swatch Finder AJAX endpoint,
scrapes each one (clicking the Features tab), and prints a summary table.

Usage:
    pip install playwright beautifulsoup4
    python -m playwright install chromium
    python test_swatch_scraper.py
"""

import asyncio
import json
import logging
import random
import sys

from bs4 import BeautifulSoup
from patchright.async_api import async_playwright, TimeoutError as PWTimeout

# ── Import helpers from the main scraper ─────────────────────────────────────
from melchior.melchior import (
    AJAX_URL,
    BASE_URL,
    FINDER_URL,
    PAGE_TIMEOUT,
    STEALTH_HEADERS,
    WAIT_UNTIL,
    click_features_tab,
    extract_code_from_url,
    new_stealth_page,
    parse_product_page,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

TEST_COUNT = 5   # number of products to scrape


# ─────────────────────────────────────────────────────────────────────────────
# Collect a small batch of URLs
# ─────────────────────────────────────────────────────────────────────────────

async def get_test_urls(page, n: int = TEST_COUNT) -> list[str]:
    """Return the first n product URLs from the Finder AJAX endpoint."""
    log.info(f"Loading finder to establish session cookies…")
    await page.goto(FINDER_URL, wait_until=WAIT_UNTIL, timeout=PAGE_TIMEOUT)
    try:
        await page.wait_for_selector("[data-analytics]", timeout=20_000)
    except PWTimeout:
        log.warning("Timed out waiting for product tiles — trying AJAX anyway.")
    await asyncio.sleep(2)

    ajax = AJAX_URL.format(start=0, sz=n)
    log.info(f"Fetching AJAX: {ajax}")

    response = await page.evaluate(
        """async (url) => {
            const r = await fetch(url, {credentials: 'include'});
            return r.ok ? await r.text() : null;
        }""",
        ajax,
    )

    if not response:
        log.error("AJAX returned nothing — site may be blocking headless requests.")
        return []

    soup = BeautifulSoup(response, "html.parser")

    # Try both link selectors the main scraper uses
    links = soup.select("a.b-product_tile-image_link[href]")
    if not links:
        links = soup.select("[data-analytics][href]")

    urls = []
    for a in links[:n]:
        href = a["href"]
        if not href.startswith("http"):
            href = BASE_URL + href
        urls.append(href)

    log.info(f"Collected {len(urls)} test URLs")
    return urls


# ─────────────────────────────────────────────────────────────────────────────
# Scrape one product
# ─────────────────────────────────────────────────────────────────────────────

async def scrape_one(page, url: str) -> dict:
    code = extract_code_from_url(url)
    log.info(f"  → {code}  {url}")

    await page.goto(url, wait_until=WAIT_UNTIL, timeout=PAGE_TIMEOUT)

    # Wait for the product title
    for sel in ["h1", "[class*='pdp']", "main"]:
        try:
            await page.wait_for_selector(sel, timeout=12_000)
            break
        except PWTimeout:
            continue

    await asyncio.sleep(random.uniform(0.8, 1.4))

    # ── DEBUG: report every button visible on the page ────────────────────────
    buttons = await page.query_selector_all("button")
    log.info(f"    [DEBUG] {len(buttons)} buttons found on page:")
    for btn in buttons:
        label = await btn.get_attribute("aria-label") or ""
        test_id = await btn.get_attribute("data-test-id") or ""
        text = (await btn.inner_text()).strip()[:40]
        visible = await btn.is_visible()
        log.info(f"      aria-label={label!r:20}  data-test-id={test_id!r:25}  text={text!r:30}  visible={visible}")

    # ── DEBUG: check specifically for features tab (find the rendered instance) ─
    for sel in ['[data-test-id="features-tab"]', '[aria-label="Features"]']:
        result = await page.evaluate(f"""() => {{
            const els = document.querySelectorAll('{sel}');
            let found = 0, rendered = 0;
            for (const el of els) {{
                found++;
                const r = el.getBoundingClientRect();
                if (r.width > 0 && r.height > 0) rendered++;
            }}
            return {{found, rendered}};
        }}""")
        log.info(f"    [DEBUG] {sel!r} → {result['found']} in DOM, {result['rendered']} with non-zero size")

    # ── Screenshot before click ───────────────────────────────────────────────
    await page.screenshot(path=f"debug_{code}_before.png")
    log.info(f"    [DEBUG] Screenshot saved: debug_{code}_before.png")

    # Click the Features tab
    clicked = await click_features_tab(page)
    log.info(f"    Features tab clicked: {clicked}")

    # ── Screenshot after click ────────────────────────────────────────────────
    await page.screenshot(path=f"debug_{code}_after.png")
    log.info(f"    [DEBUG] Screenshot saved: debug_{code}_after.png")

    html = await page.content()
    data = parse_product_page(html, url)
    return data


# ─────────────────────────────────────────────────────────────────────────────
# Main test runner
# ─────────────────────────────────────────────────────────────────────────────

def print_table(rows: list[dict]) -> None:
    if not rows:
        print("No data.")
        return

    col_widths = {
        "Code":             12,
        "Nom":              28,
        "Année de sortie":  6,
        "Emballage spécial": 20,
        "Image": 60,
    }
    headers = ["Code", "Nom", "Année de sortie", "Emballage spécial", "Image"]
    header_line = " | ".join(h.ljust(col_widths[h]) for h in headers)
    sep = "-+-".join("-" * col_widths[h] for h in headers)

    print("\n" + header_line)
    print(sep)
    for row in rows:
        line = " | ".join(
            str(row.get(h, "")).ljust(col_widths[h])[:col_widths[h]]
            for h in headers
        )
        print(line)
    print()

    # Summary
    found_year   = sum(1 for r in rows if r.get("Année de sortie"))
    found_pkg    = sum(1 for r in rows if r.get("Emballage spécial"))
    found_name   = sum(1 for r in rows if r.get("Nom"))
    found_image  = sum(1 for r in rows if r.get("Image"))
    n = len(rows)
    print(f"Results for {n} products:")
    print(f"  Name found        : {found_name}/{n}")
    print(f"  Image found       : {found_image}/{n}")
    print(f"  Year found        : {found_year}/{n}  {'✓' if found_year else '✗ (may not be on page)'}")
    print(f"  Packaging found   : {found_pkg}/{n}  {'✓' if found_pkg == n else ('⚠ partial' if found_pkg else '✗')}")


async def main():
    log.info("╔══════════════════════════════════════╗")
    log.info("║     Swatch Scraper — Quick Test      ║")
    log.info(f"║     Target: {TEST_COUNT} products{' ' * (20 - len(str(TEST_COUNT)))}║")
    log.info("╚══════════════════════════════════════╝")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        # Stealth patches applied per-page via new_stealth_page()

        page = await new_stealth_page(context)
        await page.set_extra_http_headers(STEALTH_HEADERS)

        # Step 1: get URLs
        urls = await get_test_urls(page, TEST_COUNT)
        if not urls:
            log.error("Could not collect any URLs. Aborting test.")
            await context.close()
            await browser.close()
            sys.exit(1)

        # Step 2: scrape each
        results = []
        for i, url in enumerate(urls, 1):
            log.info(f"\n[{i}/{len(urls)}] Scraping…")
            try:
                data = await scrape_one(page, url)
                results.append(data)
                log.info(
                    f"    Name     : {data['Nom'] or '(empty)'}\n"
                    f"    Year     : {data['Année de sortie'] or '—'}\n"
                    f"    Packaging: {data['Emballage spécial'] or '—'}"
                )
            except Exception as e:
                log.error(f"  Failed: {e}")
                results.append({"Code": extract_code_from_url(url), "Nom": "ERROR"})

            if i < len(urls):
                await asyncio.sleep(random.uniform(2.0, 3.5))

        await context.close()
        await browser.close()

    # Step 3: print results
    print_table(results)


if __name__ == "__main__":
    asyncio.run(main())