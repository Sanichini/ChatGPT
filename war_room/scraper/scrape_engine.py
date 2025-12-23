"""
scrape_engine.py
------------------

This module implements a simple web scraping engine for collecting
real‑time pricing and specification data for smartphones from
major e‑commerce marketplaces operating in Thailand (Shopee, Lazada
and JD Central). It is designed to be run as a CLI tool or
imported as a library by other scripts (e.g. for scheduled
scraping jobs).

The scraper uses the `requests` library to fetch HTML pages and
`BeautifulSoup` to parse product names, prices, specifications
and approximate sales volumes. Each marketplace has its own
HTML structure, so dedicated functions are provided for each
site. Error handling is included so that the scraper will
continue running even if one site fails or returns unexpected
markup.

Collected results can be saved to a CSV file and an SQLite
database. The database schema is defined in `create_tables()`.
Anomaly detection functions look for sudden price drops or
flash‑sale events by comparing the current price against a
rolling window of historical prices stored in the database.

Note: This implementation uses static selectors based on the
public site structure at the time of writing (late 2025).
E‑commerce sites frequently update their markup and may employ
anti‑scraping techniques. In production, consider using headless
browser automation (e.g. Selenium) with rotating user agents
and proxies. Respect each site's robots.txt policies and
terms of service.
"""

from __future__ import annotations

import csv
import datetime as dt
import json
import logging
import sqlite3
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


@dataclass
class Product:
    """Simple container for scraped smartphone data."""
    marketplace: str
    name: str
    price: float
    currency: str
    specs: Dict[str, str]
    sales: Optional[int] = None
    rating: Optional[float] = None
    timestamp: dt.datetime = field(default_factory=dt.datetime.utcnow)

    def as_tuple(self) -> tuple:
        return (
            self.timestamp.isoformat(),
            self.marketplace,
            self.name,
            self.price,
            self.currency,
            json.dumps(self.specs, ensure_ascii=False),
            self.sales,
            self.rating,
        )


def fetch_page(url: str, headers: Optional[Dict[str, str]] = None) -> Optional[str]:
    """Fetch a web page and return its HTML content.

    Args:
        url: The page URL to fetch.
        headers: Optional HTTP headers to send with the request.

    Returns:
        The HTML content as a string if successful, otherwise None.
    """
    try:
        logging.debug(f"Fetching URL: {url}")
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        return resp.text
    except Exception as exc:
        logging.warning(f"Failed to fetch {url}: {exc}")
        return None


def parse_shopee(query: str, max_results: int = 10) -> List[Product]:
    """Scrape search results from Shopee for a given query.

    Shopee uses client‑side rendering with dynamic content. This
    function relies on Shopee's public search API to obtain
    products in JSON form. It falls back to parsing HTML if the
    API call is blocked or fails.

    Args:
        query: Search keyword(s) for the target smartphone model.
        max_results: Maximum number of products to return.

    Returns:
        A list of Product objects.
    """
    results: List[Product] = []
    # Example using the public API (may change at any time)
    api_url = (
        f"https://shopee.co.th/api/v4/search/search_items?by=relevancy&keyword={query}&limit={max_results}&newest=0&order=desc&page_type=search"
    )
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99 Safari/537.36"
    }
    data_text = fetch_page(api_url, headers)
    if data_text:
        try:
            data = json.loads(data_text)
            for item in data.get("items", []):
                model = item.get("item_basic", {})
                name = model.get("name", "")
                price = model.get("price", 0) / 100000.0  # price in THB (Shopee stores price*10^5)
                sales = model.get("historical_sold", None)
                rating = model.get("item_rating", {}).get("rating_star", None)
                specs = {
                    "brand": model.get("brand", "Unknown"),
                    "model_id": model.get("itemid", "")
                }
                results.append(
                    Product(
                        marketplace="Shopee",
                        name=name,
                        price=price,
                        currency="THB",
                        specs=specs,
                        sales=sales,
                        rating=rating,
                    )
                )
        except json.JSONDecodeError:
            logging.warning("Failed to parse Shopee JSON response; falling back to HTML parsing.")
    if not results:
        # Fallback: parse HTML search page (simplified – structure may change)
        search_url = f"https://shopee.co.th/search?keyword={query}"
        html = fetch_page(search_url, headers)
        if html:
            soup = BeautifulSoup(html, "html.parser")
            items = soup.select("div.shopee-search-item-result__item")
            for item in items[:max_results]:
                name = item.find("div", class_="_10Wbs- _5SSWfi UjjMrh").get_text(strip=True)
                price_text = item.find("span", class_="_341bF0").get_text(strip=True).replace(",", "")
                price = float(price_text.replace("฿", ""))
                specs = {"unknown": "unknown"}
                results.append(
                    Product(
                        marketplace="Shopee",
                        name=name,
                        price=price,
                        currency="THB",
                        specs=specs,
                    )
                )
    return results


def parse_lazada(query: str, max_results: int = 10) -> List[Product]:
    """Scrape search results from Lazada.

    Lazada's search page includes a JSON script tag with product data.

    Args:
        query: Search keyword(s).
        max_results: Maximum number of products to return.

    Returns:
        A list of Product objects.
    """
    results: List[Product] = []
    search_url = f"https://www.lazada.co.th/catalog/?q={query}"
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99 Safari/537.36",
        "referer": "https://www.google.com/",
    }
    html = fetch_page(search_url, headers)
    if not html:
        return results
    soup = BeautifulSoup(html, "html.parser")
    # Lazada often embeds product info in a script tag named "__NEXT_DATA__"
    script = soup.find("script", id="__NEXT_DATA__")
    if script:
        try:
            data = json.loads(script.string)
            # navigate through JSON to find list of products
            products_data = (
                data.get("props", {})
                .get("pageProps", {})
                .get("initialState", {})
                .get("products", {})
                .get("items", [])
            )
            for item in products_data[:max_results]:
                name = item.get("name", "")
                price = item.get("price", {}).get("value", 0.0)
                currency = item.get("price", {}).get("currencyCode", "THB")
                rating = item.get("ratingScore", None)
                sales = item.get("soldCount", None)
                specs = {
                    "brand": item.get("brandName", "Unknown"),
                    "sku": item.get("sku", "")
                }
                results.append(
                    Product(
                        marketplace="Lazada",
                        name=name,
                        price=price,
                        currency=currency,
                        specs=specs,
                        sales=sales,
                        rating=rating,
                    )
                )
        except Exception as exc:
            logging.warning(f"Failed to parse Lazada JSON: {exc}")
    return results


def parse_jd_central(query: str, max_results: int = 10) -> List[Product]:
    """Scrape search results from JD Central (Thailand).

    JD Central uses a mixture of HTML and JSON. This function looks
    for `<script type="application/ld+json">` blocks describing
    products.

    Args:
        query: Search keyword(s).
        max_results: Maximum number of products to return.

    Returns:
        A list of Product objects.
    """
    results: List[Product] = []
    search_url = f"https://www.jd.co.th/search?keyword={query}"
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99 Safari/537.36"
    }
    html = fetch_page(search_url, headers)
    if not html:
        return results
    soup = BeautifulSoup(html, "html.parser")
    scripts = soup.find_all("script", type="application/ld+json")
    for script in scripts:
        try:
            data = json.loads(script.string)
            if isinstance(data, dict) and data.get("@type") == "Product":
                name = data.get("name", "")
                offers = data.get("offers", {})
                price = float(offers.get("price", 0.0))
                currency = offers.get("priceCurrency", "THB")
                rating = data.get("aggregateRating", {}).get("ratingValue", None)
                sales = None  # JD Central doesn't expose sales count publicly
                specs = {
                    "brand": data.get("brand", {}).get("name", "Unknown"),
                    "sku": data.get("sku", "")
                }
                results.append(
                    Product(
                        marketplace="JD Central",
                        name=name,
                        price=price,
                        currency=currency,
                        specs=specs,
                        sales=sales,
                        rating=rating,
                    )
                )
                if len(results) >= max_results:
                    break
        except Exception:
            continue
    return results


def create_tables(db_path: str) -> None:
    """Create SQLite tables for storing product history.

    The table `products` stores individual scrape events with
    timestamped prices and metadata.

    Args:
        db_path: Path to the SQLite database file.
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            timestamp TEXT,
            marketplace TEXT,
            name TEXT,
            price REAL,
            currency TEXT,
            specs TEXT,
            sales INTEGER,
            rating REAL
        )
        """
    )
    conn.commit()
    conn.close()


def save_to_db(products: List[Product], db_path: str) -> None:
    """Insert a list of Product records into the SQLite database."""
    if not products:
        return
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.executemany(
        "INSERT INTO products VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [p.as_tuple() for p in products],
    )
    conn.commit()
    conn.close()


def save_to_csv(products: List[Product], csv_path: str) -> None:
    """Write product records to a CSV file.

    Args:
        products: List of Product objects.
        csv_path: Destination CSV file path.
    """
    fieldnames = [
        "timestamp",
        "marketplace",
        "name",
        "price",
        "currency",
        "specs",
        "sales",
        "rating",
    ]
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if f.tell() == 0:
            writer.writeheader()
        for p in products:
            row = asdict(p)
            row["timestamp"] = p.timestamp.isoformat()
            row["specs"] = json.dumps(p.specs, ensure_ascii=False)
            writer.writerow(row)


def detect_price_anomalies(db_path: str, window: int = 5, threshold_pct: float = 0.2) -> pd.DataFrame:
    """Detect price anomalies by comparing the latest price to a moving average.

    Args:
        db_path: SQLite database path.
        window: Number of previous entries to consider for the rolling mean.
        threshold_pct: Fractional drop considered anomalous (e.g. 0.2 means 20% drop).

    Returns:
        DataFrame containing rows with anomalies (columns: name, marketplace,
        current_price, rolling_mean, drop_pct).
    """
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM products", conn, parse_dates=["timestamp"])
    conn.close()
    anomalies = []
    if df.empty:
        return pd.DataFrame()
    for (marketplace, name), group in df.groupby(["marketplace", "name"]):
        group = group.sort_values("timestamp")
        group["rolling_mean"] = group["price"].rolling(window=window, min_periods=1).mean()
        group["drop_pct"] = (group["rolling_mean"] - group["price"]) / group["rolling_mean"]
        latest = group.iloc[-1]
        if latest["drop_pct"] >= threshold_pct:
            anomalies.append(
                {
                    "marketplace": marketplace,
                    "name": name,
                    "current_price": latest["price"],
                    "rolling_mean": latest["rolling_mean"],
                    "drop_pct": latest["drop_pct"],
                }
            )
    return pd.DataFrame(anomalies)


def scrape_all(keywords: List[str], db_path: str, csv_path: str, limit: int = 10) -> None:
    """Scrape all marketplaces for a list of keyword queries and store data.

    Args:
        keywords: List of model names to search for.
        db_path: SQLite database path.
        csv_path: CSV file path.
        limit: Maximum number of results per site.
    """
    create_tables(db_path)
    all_products: List[Product] = []
    for kw in keywords:
        logging.info(f"Scraping data for '{kw}'")
        try:
            all_products.extend(parse_shopee(kw, max_results=limit))
        except Exception as exc:
            logging.error(f"Shopee scraper failed for {kw}: {exc}")
        try:
            all_products.extend(parse_lazada(kw, max_results=limit))
        except Exception as exc:
            logging.error(f"Lazada scraper failed for {kw}: {exc}")
        try:
            all_products.extend(parse_jd_central(kw, max_results=limit))
        except Exception as exc:
            logging.error(f"JD Central scraper failed for {kw}: {exc}")
    if all_products:
        save_to_db(all_products, db_path)
        save_to_csv(all_products, csv_path)
    else:
        logging.warning("No products were scraped.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape smartphone data from e‑commerce sites.")
    parser.add_argument("keywords", nargs="*", help="List of keywords/models to search for")
    parser.add_argument("--db", default="./data/products.db", help="SQLite database path")
    parser.add_argument("--csv", default="./data/products.csv", help="CSV file path")
    parser.add_argument("--limit", type=int, default=10, help="Max results per marketplace")
    args = parser.parse_args()

    if not args.keywords:
        print("Please provide at least one keyword to search for.")
        exit(1)
    scrape_all(args.keywords, db_path=args.db, csv_path=args.csv, limit=args.limit)