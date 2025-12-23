"""
telegram_bot.py
----------------

This script implements a simple Telegram bot that monitors the
scraped product database for significant events and sends alerts
to a configured chat. It can be run periodically (e.g. via
cron) or as a long‑running service.

Alerts include:
  • Price drops greater than 5% compared to a rolling mean.
  • A surge of negative reviews (>100 words of negative sentiment per day).
  • The appearance of new smartphone models not previously seen.

Sentiment analysis of reviews is stubbed out because accessing
platform APIs (Twitter/Facebook) requires authentication keys. To
fully enable this feature, provide API credentials and implement
`fetch_recent_reviews()`.
"""

import json
import os
import logging
import sqlite3
import time
from typing import List, Set

import pandas as pd
import requests

from scraper.scrape_engine import detect_price_anomalies

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
DB_PATH = os.environ.get('PRODUCT_DB_PATH', './data/products.db')

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')


def send_telegram_message(message: str) -> None:
    """Send a message to the configured Telegram chat."""
    if not TELEGRAM_TOKEN or not CHAT_ID:
        logging.warning('Telegram token or chat ID not set; skipping message.')
        return
    url = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage'
    payload = {'chat_id': CHAT_ID, 'text': message, 'parse_mode': 'Markdown'}
    try:
        resp = requests.post(url, data=payload, timeout=10)
        resp.raise_for_status()
    except Exception as exc:
        logging.error(f'Failed to send Telegram message: {exc}')


def check_price_drop() -> None:
    """Check for price anomalies and notify if drop exceeds 5%."""
    anomalies = detect_price_anomalies(DB_PATH, window=5, threshold_pct=0.05)
    if not anomalies.empty:
        for _, row in anomalies.iterrows():
            msg = (
                f"⚠️ *Price Drop Alert*\n"
                f"Product: {row['name']}\n"
                f"Marketplace: {row['marketplace']}\n"
                f"Current Price: {row['current_price']:.2f} THB\n"
                f"Rolling Mean: {row['rolling_mean']:.2f} THB\n"
                f"Drop: {row['drop_pct']*100:.1f}%"
            )
            send_telegram_message(msg)


def fetch_recent_reviews() -> List[str]:
    """Stub: Fetch recent reviews from social media or marketplace APIs.

    To enable this feature, integrate with Twitter's X API or Facebook
    Graph API and return a list of raw review texts. The stub returns
    an empty list.
    """
    # TODO: implement real fetching using API tokens
    return []


def analyse_sentiment(texts: List[str]) -> int:
    """Very simple sentiment analysis stub. Counts words typically associated with negativity."""
    negative_keywords = {'bad', 'slow', 'poor', 'terrible', 'disappointing', 'worst', 'hate'}
    negative_word_count = 0
    for text in texts:
        words = text.lower().split()
        negative_word_count += sum(1 for w in words if w in negative_keywords)
    return negative_word_count


def check_reviews() -> None:
    """Check if negative reviews exceed threshold and send alert."""
    reviews = fetch_recent_reviews()
    neg_count = analyse_sentiment(reviews)
    if neg_count > 100:
        msg = (
            f"🚨 *Negative Review Surge*\n"
            f"Detected {neg_count} negative words in recent reviews. Immediate action recommended."
        )
        send_telegram_message(msg)


def check_new_listings(seen_products: Set[str]) -> Set[str]:
    """Identify newly listed products since the last check and send alert."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query('SELECT DISTINCT name FROM products', conn)
    conn.close()
    current_products = set(df['name'].tolist())
    new_products = current_products - seen_products
    for product in new_products:
        msg = f"🆕 *New Product Listed*\nProduct: {product}"
        send_telegram_message(msg)
    return current_products


def main():
    seen_products: Set[str] = set()
    # Initial load of existing products
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query('SELECT DISTINCT name FROM products', conn)
    conn.close()
    seen_products = set(df['name'].tolist())
    logging.info(f'Loaded {len(seen_products)} existing products.')
    while True:
        try:
            check_price_drop()
            check_reviews()
            seen_products = check_new_listings(seen_products)
        except Exception as exc:
            logging.error(f'Error in bot loop: {exc}')
        # Sleep for 10 minutes between checks
        time.sleep(600)


if __name__ == '__main__':
    main()