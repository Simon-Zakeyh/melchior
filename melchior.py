"""
swatch_scraper.py — Swatch full catalog scraper
================================================
Requirements:
    pip install playwright beautifulsoup4
    python -m playwright install chromium

Usage:
    python swatch_scraper.py

Output:
    swatch_catalog.csv  — main data file (written row by row, crash-safe)
    swatch_urls.txt     — all product URLs discovered in Phase 1
    swatch_errors.log   — products that failed after all retries

Resume:
    Re-run the script at any time; it will skip already-processed products.

Columns:
    Image, Nom, Code, Année de sortie, Emballage spécial
"""

import asyncio
import csv
import json
import logging
import os
import random
import re
import sys
import time
from pathlib import Path

from bs4 import BeautifulSoup
from patchright.async_api import async_playwright, TimeoutError as PWTimeout

# ─── CONFIG ──────────────────────────────────────────────────────────────────
BASE_URL        = "https://www.swatch.com"
FINDER_URL      = "https://www.swatch.com/en-en/swatch-finder/"
AJAX_URL        = (
    "https://www.swatch.com/on/demandware.store/Sites-swarp-INT-Site/en/"
    "Search-ShowAjax?cgid=swatch-finder&srule=newest-swatch&start={start}&sz={sz}"
)
PAGE_SIZE       = 24          # products per finder page
TOTAL_PRODUCTS  = 8400        # upper bound; scraper stops when no more found

DELAY_MIN       = 2.5         # seconds between product page requests (min)
DELAY_MAX       = 4.5         # seconds between product page requests (max)
PAGE_DELAY      = 3.0         # seconds between finder pages
MAX_RETRIES     = 3           # retries per product page on failure
PAGE_TIMEOUT    = 60_000      # ms – Playwright navigation timeout

# Swatch fires background requests indefinitely, so "networkidle" never fires.
# Use "load" everywhere and then wait for a known CSS selector instead.
WAIT_UNTIL      = "load"

OUTPUT_CSV      = "swatch_catalog.csv"
URLS_FILE       = "swatch_urls.txt"
ERRORS_LOG      = "swatch_errors.log"
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("swatch_scraper.log"),
    ],
)
log = logging.getLogger(__name__)

CSV_COLUMNS = ["Image", "Nom", "Code", "Année de sortie", "Emballage spécial"]

STEALTH_HEADERS = {
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "Upgrade-Insecure-Requests": "1",
}


async def new_stealth_page(context):
    """Create a new page with realistic headers and a human-like mouse nudge.
    Stealth patching is handled automatically by patchright at the browser level."""
    page = await context.new_page()
    await page.set_extra_http_headers(STEALTH_HEADERS)
    await page.mouse.move(random.randint(200, 800), random.randint(100, 500))
    return page


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def load_done_codes() -> set:
    """Return set of product codes already written to the CSV."""
    done = set()
    if Path(OUTPUT_CSV).exists():
        with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = row.get("Code", "").strip()
                if code:
                    done.add(code)
    return done


def append_row(row: dict) -> None:
    """Append one row to the CSV, creating the file with header if needed."""
    write_header = not Path(OUTPUT_CSV).exists()
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def log_error(url: str, reason: str) -> None:
    with open(ERRORS_LOG, "a", encoding="utf-8") as f:
        f.write(f"{url}\t{reason}\n")


def random_delay(min_s: float = DELAY_MIN, max_s: float = DELAY_MAX) -> None:
    time.sleep(random.uniform(min_s, max_s))


def extract_code_from_url(url: str) -> str:
    """Extract product code like SS08P113 from URL."""
    match = re.search(r"/([A-Z0-9]+)\.html", url)
    return match.group(1) if match else ""


def image_url_from_code(code: str) -> str:
    """Construct the standard product image URL from its code."""
    return f"https://static.swatch.com/images/product/{code}/li3/{code}_li3_ec001.jpg"


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 1 — Collect all product URLs
# ═══════════════════════════════════════════════════════════════════════════════

async def collect_urls_via_ajax(session_page) -> list:
    """
    Try the Demandware AJAX endpoint to collect product URLs page by page.
    Returns list of absolute URLs, or empty list if the endpoint is blocked.
    """
    urls = []
    start = 0

    while True:
        ajax = AJAX_URL.format(start=start, sz=PAGE_SIZE)
        log.info(f"  [AJAX] Fetching finder offset {start}…")

        try:
            response = await session_page.evaluate(
                """async (url) => {
                    const r = await fetch(url, {credentials: 'include'});
                    return r.ok ? await r.text() : null;
                }""",
                ajax,
            )
        except Exception as e:
            log.warning(f"  AJAX fetch error: {e}")
            return []

        if not response:
            log.info("  AJAX endpoint returned empty — switching to browser navigation.")
            return []

        soup = BeautifulSoup(response, "html.parser")
        links = soup.select("a.b-product_tile-image_link[href]")
        if not links:
            links = soup.select("[data-analytics][href]")

        if not links:
            log.info(f"  No more products at offset {start}, stopping.")
            break

        for a in links:
            href = a["href"]
            if not href.startswith("http"):
                href = BASE_URL + href
            if href not in urls:
                urls.append(href)

        log.info(f"  Collected {len(urls)} URLs so far…")
        start += PAGE_SIZE

        if len(links) < PAGE_SIZE:
            break  # last page

        await asyncio.sleep(PAGE_DELAY)

    return urls


async def collect_urls_via_browser(browser) -> list:
    """
    Navigate the finder page by page using a real browser, extracting
    product links from data-analytics attributes on product cards.
    """
    urls = []
    seen = set()
    page = await new_stealth_page(browser)

    log.info("  Loading finder page in browser…")
    await page.goto(FINDER_URL, wait_until=WAIT_UNTIL, timeout=PAGE_TIMEOUT)
    try:
        await page.wait_for_selector("[data-analytics]", timeout=20_000)
    except PWTimeout:
        log.warning("  Timed out waiting for product tiles — continuing anyway.")
    await asyncio.sleep(2)

    # Accept cookies if a banner appears
    for btn_text in ["Accept", "Accept All", "Accept Cookies", "I agree"]:
        try:
            btn = page.get_by_text(btn_text, exact=False)
            if await btn.is_visible(timeout=2000):
                await btn.click()
                await asyncio.sleep(1)
                break
        except Exception:
            pass

    page_num = 0
    while True:
        page_num += 1
        log.info(f"  Scraping finder page {page_num}…")

        # Extract all product card analytics data
        cards = await page.query_selector_all("[data-analytics]")
        new_this_page = 0
        for card in cards:
            href = await card.get_attribute("href")
            analytics_raw = await card.get_attribute("data-analytics")
            if not href or not analytics_raw:
                continue
            if "product" not in analytics_raw.lower():
                continue
            if not href.startswith("http"):
                href = BASE_URL + href
            if href not in seen:
                seen.add(href)
                urls.append(href)
                new_this_page += 1

        log.info(f"  Page {page_num}: +{new_this_page} new URLs (total {len(urls)})")

        # Try clicking "next page"
        next_btn = None
        for selector in [
            "[aria-label='Next']",
            ".b-pagination-next",
            "a[rel='next']",
            "[class*='pagination'][class*='next']",
            "[data-test-id='pagination-next']",
        ]:
            try:
                el = await page.query_selector(selector)
                if el and await el.is_visible():
                    next_btn = el
                    break
            except Exception:
                pass

        if not next_btn:
            log.info("  No next button found — finder fully paginated.")
            break

        await next_btn.click()
        try:
            await page.wait_for_load_state("load", timeout=PAGE_TIMEOUT)
            await page.wait_for_selector("[data-analytics]", timeout=15_000)
        except PWTimeout:
            log.warning("  Timeout after next-page click — continuing.")
        await asyncio.sleep(PAGE_DELAY)

    await page.close()
    return urls


async def phase1_get_urls(browser) -> list:
    """Return list of all product page URLs, loading from file if already done."""
    if Path(URLS_FILE).exists():
        with open(URLS_FILE) as f:
            urls = [l.strip() for l in f if l.strip()]
        log.info(f"Phase 1 skipped — loaded {len(urls)} URLs from {URLS_FILE}")
        return urls

    log.info("=== PHASE 1: Collecting product URLs ===")

    # Try AJAX first (faster, no rendering overhead)
    probe_page = await new_stealth_page(browser)
    await probe_page.goto(FINDER_URL, wait_until=WAIT_UNTIL, timeout=PAGE_TIMEOUT)
    try:
        await probe_page.wait_for_selector("[data-analytics]", timeout=20_000)
    except PWTimeout:
        log.warning("  Timed out waiting for product tiles on probe page.")
    await asyncio.sleep(2)

    urls = await collect_urls_via_ajax(probe_page)
    await probe_page.close()

    if not urls:
        # Fallback: real browser navigation
        log.info("Falling back to browser-based pagination…")
        urls = await collect_urls_via_browser(browser)

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            unique.append(u)
    urls = unique

    with open(URLS_FILE, "w") as f:
        f.write("\n".join(urls))

    log.info(f"Phase 1 complete: {len(urls)} product URLs saved to {URLS_FILE}")
    return urls


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2 — Scrape each product page
# ═══════════════════════════════════════════════════════════════════════════════

def parse_features_html(html: str) -> tuple[str, str]:
    """
    Parse the page HTML (after Features tab has been clicked) and return
    (packaging_type, year_of_release).  Both may be empty strings.

    The Features tab reveals a list of <li> elements shaped like:
        <li class="flex">
          <span ...>Label&nbsp;:</span>
          <span ...>Value</span>
        </li>
    We iterate every such pair and match on the label text.
    """
    soup = BeautifulSoup(html, "html.parser")
    packaging = ""
    year = ""

    # ── Strategy 1: li > span pairs (Features tab structure) ─────────────────
    for li in soup.select("li.flex, li"):
        spans = li.find_all("span", recursive=False)
        if len(spans) < 2:
            # Try one level deeper (sometimes there's a wrapper div)
            spans = li.find_all("span")
        if len(spans) < 2:
            continue

        label = spans[0].get_text(strip=True).rstrip(":").rstrip("\u00a0:").strip().lower()
        value = spans[1].get_text(strip=True)

        if not packaging and re.search(r"packag", label):
            packaging = value

        if not year and re.search(r"year|ann[eé]e|release", label):
            # Keep only the 4-digit year portion in case there's extra text
            m = re.search(r"\b(19|20)\d{2}\b", value)
            if m:
                year = m.group(0)

    # ── Strategy 2: JSON-LD structured data (often contains releaseDate) ──────
    if not year:
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                # May be a list or a single object
                items = data if isinstance(data, list) else [data]
                for item in items:
                    # Flatten nested @graph if present
                    if "@graph" in item:
                        items.extend(item["@graph"])
                    for key in ("releaseDate", "datePublished", "productionDate", "offers"):
                        val = item.get(key, "")
                        if isinstance(val, str):
                            m = re.search(r"\b(19|20)\d{2}\b", val)
                            if m:
                                year = m.group(0)
                                break
                    if year:
                        break
            except (json.JSONDecodeError, AttributeError):
                continue

    # ── Strategy 3: scan window.__STATE__ / inline JSON blobs ─────────────────
    if not year:
        for script in soup.find_all("script"):
            text = script.string or ""
            # Look for year-like fields near release/launch keywords
            m = re.search(
                r'"(?:releaseYear|yearOfRelease|launchYear|year)["\s]*:\s*"?(\d{4})"?',
                text, re.IGNORECASE
            )
            if m:
                year = m.group(1)
                break

    return packaging, year


def parse_product_page(html: str, url: str) -> dict:
    """
    Parse the rendered HTML of a product page (after Features tab click)
    and return a data dict.
    """
    soup = BeautifulSoup(html, "html.parser")

    # ── Code ──────────────────────────────────────────────────────────────────
    code = extract_code_from_url(url)

    # ── Name ──────────────────────────────────────────────────────────────────
    name = ""
    for sel in ["h1.b-product_details-title", "h1[itemprop='name']", "h1", ".b-pdp_name"]:
        tag = soup.select_one(sel)
        if tag:
            name = tag.get_text(strip=True)
            break

    # ── Image ─────────────────────────────────────────────────────────────────
    image = ""
    for sel in [
        ".b-pdp_gallery img",
        "[class*='gallery'] img",
        "[class*='pdp'] img",
        "img[alt*='Gallery']",
        f"img[src*='{code}']",
    ]:
        tag = soup.select_one(sel)
        if tag:
            image = tag.get("src") or tag.get("data-src") or ""
            if image and not image.startswith("http"):
                image = "https:" + image if image.startswith("//") else BASE_URL + image
            if image:
                break

    # Fallback: construct from code pattern
    if not image and code:
        image = image_url_from_code(code)

    # ── Features tab: packaging + year ────────────────────────────────────────
    packaging, year = parse_features_html(html)

    return {
        "Image":             image,
        "Nom":               name,
        "Code":              code,
        "Année de sortie":   year,
        "Emballage spécial": packaging,
    }


async def click_features_tab(page) -> bool:
    """
    Click the Features tab and wait for its content to render.
    Uses a single JS evaluate call to find and click the rendered (non-zero-size)
    instance of the tab, bypassing Playwright's is_visible() which can mis-report
    when there are duplicate elements (e.g. desktop + mobile nav copies).
    """
    FEATURE_CONTENT_SEL = "li.flex span"

    # First: if content is already rendered (tab pre-selected), nothing to do
    try:
        await page.wait_for_selector(FEATURE_CONTENT_SEL, timeout=2_000)
        log.debug("  Features content already visible — tab was pre-selected.")
        return True
    except PWTimeout:
        pass

    # Find and click the Features tab entirely in JS, using getBoundingClientRect
    # to identify the visible instance (non-zero dimensions) among duplicates.
    clicked = await page.evaluate("""() => {
        const selectors = [
            '[data-test-id="features-tab"]',
            '[aria-label="Features"]',
        ];
        for (const sel of selectors) {
            const els = document.querySelectorAll(sel);
            for (const el of els) {
                const r = el.getBoundingClientRect();
                if (r.width > 0 && r.height > 0) {
                    el.scrollIntoView({block: 'center'});
                    el.click();
                    return true;
                }
            }
        }
        return false;
    }""")

    if not clicked:
        log.warning("  Features tab: no rendered instance found in DOM.")
        return False

    # Wait for the feature list items to appear
    try:
        await page.wait_for_selector(FEATURE_CONTENT_SEL, timeout=8_000)
        await asyncio.sleep(0.5)
        return True
    except PWTimeout:
        log.warning("  Features tab clicked but content did not appear within 8s.")
        return False


async def scrape_product(page, url: str, attempt: int = 1) -> dict | None:
    """Navigate to a product page, click Features tab, and extract data."""
    try:
        await page.goto(url, wait_until=WAIT_UNTIL, timeout=PAGE_TIMEOUT)

        # Wait for the product title to confirm the page loaded
        for sel in ["h1", "[class*='pdp']", "[class*='product']", "main"]:
            try:
                await page.wait_for_selector(sel, timeout=12_000)
                break
            except PWTimeout:
                continue

        # Small human-like pause to let initial JS settle
        await asyncio.sleep(random.uniform(0.8, 1.5))

        # Click the Features tab to reveal packaging type (and possibly year)
        clicked = await click_features_tab(page)
        if not clicked:
            log.debug(f"  Features tab not found on {url} — continuing with base HTML.")

        html = await page.content()
        data = parse_product_page(html, url)
        return data

    except PWTimeout:
        log.warning(f"  Timeout on attempt {attempt}: {url}")
        return None
    except Exception as e:
        log.warning(f"  Error on attempt {attempt}: {url} — {e}")
        return None


async def phase2_scrape_products(browser, urls: list, done_codes: set) -> None:
    """Visit each product URL, extract data, append to CSV."""
    log.info(f"=== PHASE 2: Scraping {len(urls)} product pages ===")
    log.info(f"  Already done: {len(done_codes)} products (skipping)")

    page = await new_stealth_page(browser)

    total     = len(urls)
    processed = 0
    skipped   = 0
    errors    = 0

    for i, url in enumerate(urls, 1):
        code = extract_code_from_url(url)
        if code in done_codes:
            skipped += 1
            continue

        log.info(f"[{i}/{total}] Scraping {code} …")

        data = None
        for attempt in range(1, MAX_RETRIES + 1):
            data = await scrape_product(page, url, attempt)
            if data:
                break
            if attempt < MAX_RETRIES:
                wait = DELAY_MAX * attempt
                log.info(f"  Retry {attempt}/{MAX_RETRIES - 1} in {wait:.0f}s…")
                await asyncio.sleep(wait)

        if data:
            append_row(data)
            done_codes.add(code)
            processed += 1
            log.info(
                f"  ✓ {data['Nom']!r}  |  Year: {data['Année de sortie'] or '—'}"
                f"  |  Packaging: {data['Emballage spécial'] or '—'}"
            )
        else:
            log_error(url, "Failed after all retries")
            errors += 1
            log.error(f"  ✗ {url} — logged to {ERRORS_LOG}")
            # Append a partial row so we don't re-attempt on resume
            append_row({
                "Image": image_url_from_code(code),
                "Nom": "",
                "Code": code,
                "Année de sortie": "",
                "Emballage spécial": "ERROR",
            })
            done_codes.add(code)

        # Rate-limit delay (skip on last item)
        if i < total:
            await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    await page.close()
    log.info(
        f"\nPhase 2 complete — processed: {processed}, skipped: {skipped}, errors: {errors}"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

async def main():
    log.info("╔══════════════════════════════════════╗")
    log.info("║       Swatch Catalog Scraper         ║")
    log.info("╚══════════════════════════════════════╝")

    done_codes = load_done_codes()
    log.info(f"Resuming — {len(done_codes)} products already in CSV.")

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
        # Note: stealth patches are applied per-page in new_stealth_page()

        try:
            urls = await phase1_get_urls(context)

            if not urls:
                log.error("No product URLs collected. Aborting.")
                return

            await phase2_scrape_products(context, urls, done_codes)

        finally:
            await context.close()
            await browser.close()

    log.info(f"All done! Results saved to {OUTPUT_CSV}")


if __name__ == "__main__":
    asyncio.run(main())