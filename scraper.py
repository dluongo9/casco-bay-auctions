"""
Casco Bay Auctions scraper
Collects all sold lots with prices across all past auctions.

Sources:
  - Archive:      https://cascobayauctions.com/archive/
  - Catalog:      https://cascobayauctions.com/auction/{id}-{slug}/page/{n}/
  - Lot detail:   https://cascobayauctions.com/auction/{id}-{slug}/lot-{n}-{slug}/
  - Prices API:   https://cascobayauctions.bidspirit.com/api/thirdparty/v1/items/getItemsInfo.api

Output: auctions.csv
"""

import csv
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
})

BIDSPIRIT_API = (
    "https://cascobayauctions.bidspirit.com"
    "/api/thirdparty/v1/items/getItemsInfo.api"
)
ARCHIVE_URL = "https://cascobayauctions.com/archive/"
BASE_URL = "https://cascobayauctions.com"
SLEEP = 0.3  # seconds between sequential requests
WORKERS = 5  # concurrent workers for lot detail pages


def get_archive():
    """Return list of dicts: auction_id, name, url, date_str."""
    resp = SESSION.get(ARCHIVE_URL, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    auctions = []
    seen = set()

    for card in soup.select(".bp-lot.bp-lot_auction"):
        link = card.select_one("a[href*='/auction/']")
        if not link:
            continue
        href = link["href"]
        if not href.startswith("http"):
            href = BASE_URL + href

        m = re.search(r"/auction/(\d+)-", href)
        if not m:
            continue
        auction_id = m.group(1)
        if auction_id in seen:
            continue
        seen.add(auction_id)

        title_el = card.select_one(".bp-lot__title, .bp-title, h2, h3")
        raw_text = title_el.get_text("\n", strip=True) if title_el else ""
        lines = [l.strip() for l in raw_text.split("\n") if l.strip()]
        name = lines[0] if lines else href.split("/")[-2].replace("-", " ").title()

        date_match = re.search(
            r"[A-Z][a-z]+day,\s+([A-Za-z]+\s+\d+,\s+\d{4})", raw_text
        )
        date_str = date_match.group(1) if date_match else ""
        if date_str:
            try:
                date_str = datetime.strptime(date_str, "%B %d, %Y").strftime("%Y-%m-%d")
            except ValueError:
                pass

        auctions.append({
            "auction_id": auction_id,
            "name": name,
            "url": href,
            "date": date_str,
        })

    return auctions


def get_catalog_page(url, page=1):
    """Fetch one catalog page. Returns (soup, total_pages)."""
    if page > 1:
        page_url = url.rstrip("/") + f"/page/{page}/"
    else:
        page_url = url
    resp = SESSION.get(page_url, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    total_pages = 1
    for a in soup.select("a[href*='/page/']"):
        m = re.search(r"/page/(\d+)/", a["href"])
        if m:
            total_pages = max(total_pages, int(m.group(1)))

    return soup, total_pages


def parse_lots_from_page(soup):
    """Extract lot metadata + detail URLs from a catalog HTML page."""
    lots = []
    for el in soup.select("[data-lot-index]"):
        lot_index = el.get("data-lot-index", "").strip()
        lot_id = el.get("data-lot-id", "").strip()

        title_el = el.select_one(".bp-lot__title")
        title = ""
        if title_el:
            raw = title_el.get_text(strip=True)
            title = re.sub(r"^Lot\s+\d+\s*:\s*", "", raw, flags=re.I).strip()

        # Get lot detail page URL
        detail_link = el.select_one("a[href*='/lot-']")
        detail_url = ""
        if detail_link:
            detail_url = detail_link["href"]
            if not detail_url.startswith("http"):
                detail_url = BASE_URL + detail_url

        lots.append({
            "lot_index": lot_index,
            "lot_id": lot_id,
            "title": title,
            "detail_url": detail_url,
            "estimate_low": None,
            "estimate_high": None,
        })
    return lots


def fetch_lot_estimates(detail_url):
    """Fetch a lot detail page and extract estimate + start price from HTML."""
    try:
        resp = SESSION.get(detail_url, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        est_low = est_high = None
        est_el = soup.select_one(".bp-control__estimate")
        if est_el:
            nums = re.findall(r"[\d,]+", est_el.get_text().replace(",", ""))
            nums = [int(n) for n in nums]
            est_low = nums[0] if len(nums) >= 1 else None
            est_high = nums[1] if len(nums) >= 2 else None

        return {"estimate_low": est_low, "estimate_high": est_high}
    except Exception:
        return {"estimate_low": None, "estimate_high": None}


def fetch_estimates_batch(lots):
    """Fetch estimates for a list of lots concurrently."""
    results = {}  # lot_index -> {estimate_low, estimate_high}
    to_fetch = [(lot["lot_index"], lot["detail_url"]) for lot in lots if lot["detail_url"]]

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        future_to_idx = {}
        for lot_index, url in to_fetch:
            future = executor.submit(fetch_lot_estimates, url)
            future_to_idx[future] = lot_index

        done = 0
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            results[idx] = future.result()
            done += 1
            if done % 50 == 0:
                print(f"      estimates: {done}/{len(to_fetch)}")

    return results


def get_prices(auction_id, lot_indexes, batch_size=50):
    """Query BidSpirit API. Returns dict {lot_index_str: price_info}."""
    results = {}
    for i in range(0, len(lot_indexes), batch_size):
        batch = lot_indexes[i : i + batch_size]
        params = {
            "auctionId": auction_id,
            "lotIndexes": ",".join(str(x) for x in batch),
        }
        try:
            resp = SESSION.get(BIDSPIRIT_API, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            items_info = data.get("response", {}).get("itemsInfo", {})
            for idx, info in items_info.items():
                sold_bid = info.get("soldBid")
                results[str(idx)] = {
                    "sold_price": sold_bid["price"] if sold_bid else None,
                    "sold_time": sold_bid["soldTime"] if sold_bid else None,
                    "start_price": info.get("startPrice"),
                }
        except Exception as e:
            print(f"    API error auction={auction_id} batch {batch[0]}-{batch[-1]}: {e}")
        time.sleep(SLEEP)
    return results


def scrape_auction(auction):
    """Scrape all lots from one auction dict. Returns list of row dicts."""
    auction_id = auction["auction_id"]
    name = auction["name"]
    url = auction["url"]
    date = auction["date"]
    print(f"  [{auction_id}] {name} ({date})")

    all_lots = []
    try:
        soup, total_pages = get_catalog_page(url, page=1)
        all_lots.extend(parse_lots_from_page(soup))
        print(f"    page 1/{total_pages} → {len(all_lots)} lots")

        for page in range(2, total_pages + 1):
            time.sleep(SLEEP)
            try:
                soup2, _ = get_catalog_page(url, page=page)
                new_lots = parse_lots_from_page(soup2)
                all_lots.extend(new_lots)
                print(f"    page {page}/{total_pages} → {len(all_lots)} lots")
            except Exception as e:
                print(f"    page {page} error: {e}")
    except Exception as e:
        print(f"    Failed to load auction: {e}")
        return []

    if not all_lots:
        return []

    # Deduplicate by lot_index
    deduped = {}
    for lot in all_lots:
        deduped[lot["lot_index"]] = lot
    all_lots = list(deduped.values())

    # Fetch estimates from detail pages concurrently
    print(f"    Fetching estimates for {len(all_lots)} lots...")
    estimates = fetch_estimates_batch(all_lots)
    for lot in all_lots:
        est = estimates.get(lot["lot_index"], {})
        lot["estimate_low"] = est.get("estimate_low")
        lot["estimate_high"] = est.get("estimate_high")

    # Fetch prices from BidSpirit API
    print(f"    Fetching prices for {len(all_lots)} lots...")
    lot_indexes = [lot["lot_index"] for lot in all_lots]
    prices = get_prices(auction_id, lot_indexes)

    rows = []
    for lot in all_lots:
        idx = lot["lot_index"]
        p = prices.get(idx, {})
        sold_price = p.get("sold_price")
        start_price = p.get("start_price")
        sold_ts = p.get("sold_time")
        sold_date = ""
        if sold_ts:
            try:
                sold_date = datetime.fromtimestamp(
                    sold_ts / 1000, tz=timezone.utc
                ).strftime("%Y-%m-%d")
            except Exception:
                pass
        auction_date = date or sold_date

        rows.append({
            "auction_id": auction_id,
            "auction_name": name,
            "auction_date": auction_date,
            "auction_url": url,
            "lot_number": idx,
            "lot_url": lot["detail_url"],
            "lot_title": lot["title"],
            "start_price": start_price if start_price is not None else "",
            "estimate_low": lot["estimate_low"] if lot["estimate_low"] is not None else "",
            "estimate_high": lot["estimate_high"] if lot["estimate_high"] is not None else "",
            "sold_price": sold_price if sold_price is not None else "",
            "sold_date": sold_date,
            "unsold": "yes" if sold_price is None else "no",
        })

    sold = sum(1 for r in rows if r["unsold"] == "no")
    print(f"    → {len(rows)} lots, {sold} sold, {len(rows)-sold} unsold")
    return rows


def main():
    output_file = "auctions.csv"
    fieldnames = [
        "auction_id", "auction_name", "auction_date", "auction_url",
        "lot_number", "lot_url", "lot_title",
        "start_price", "estimate_low", "estimate_high",
        "sold_price", "sold_date", "unsold",
    ]

    print("Fetching archive...")
    auctions = get_archive()
    print(f"Found {len(auctions)} auctions\n")

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i, auction in enumerate(auctions):
            print(f"[{i+1}/{len(auctions)}] ", end="")
            rows = scrape_auction(auction)
            if rows:
                writer.writerows(rows)
                f.flush()
            time.sleep(SLEEP)

    print(f"\nDone! Results saved to {output_file}")


if __name__ == "__main__":
    main()
