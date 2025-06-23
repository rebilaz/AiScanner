"""Calculate rarity scores for NFTs within a collection."""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, List

import pandas as pd
import requests
from google.cloud import bigquery
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
DEST_TABLE = os.getenv("BQ_NFT_RARITY_TABLE", "nft_rarity_scores")
OPENSEA_API_KEY = os.getenv("OPENSEA_API_KEY")

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def fetch_collection_metadata(slug: str) -> List[Dict[str, Any]]:
    """Download all token metadata from OpenSea."""
    url = f"https://api.opensea.io/api/v2/collection/{slug}/nfts"
    headers = {"Accept": "application/json"}
    if OPENSEA_API_KEY:
        headers["X-API-KEY"] = OPENSEA_API_KEY
    cursor = None
    all_items: List[Dict[str, Any]] = []
    pbar = tqdm(desc="fetching", unit="item")
    while True:
        params = {"limit": 50}
        if cursor:
            params["cursor"] = cursor
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=10)
            if resp.status_code != 200:
                logging.error("OpenSea API returned %s", resp.status_code)
                break
            data = resp.json()
            items = data.get("nfts", [])
            all_items.extend(items)
            pbar.update(len(items))
            cursor = data.get("next")
            if not cursor:
                break
            time.sleep(0.2)
        except Exception as exc:  # pragma: no cover - network issues
            logging.exception("Failed to fetch metadata: %s", exc)
            break
    pbar.close()
    return all_items


def compute_frequencies(items: List[Dict[str, Any]]) -> Dict[str, int]:
    """Compute attribute frequency across the collection."""
    freq: Dict[str, int] = {}
    for item in items:
        for attr in item.get("traits", []):
            key = f"{attr.get('trait_type')}::{attr.get('value')}"
            freq[key] = freq.get(key, 0) + 1
    return freq


def compute_rarity_scores(items: List[Dict[str, Any]], freq: Dict[str, int]) -> List[Dict[str, Any]]:
    """Calculate rarity score for each NFT."""
    results: List[Dict[str, Any]] = []
    for item in items:
        score = 0.0
        for attr in item.get("traits", []):
            key = f"{attr.get('trait_type')}::{attr.get('value')}"
            count = freq.get(key, 0)
            if count:
                score += 1 / count
        results.append(
            {
                "token_id": item.get("identifier"),
                "rarity_score": score,
                "attributes": item.get("traits"),
                "metadata_json": item,
            }
        )
    return results


def store_rarity_scores(client: bigquery.Client, collection: str, items: List[Dict[str, Any]]) -> None:
    """Upload rarity scores to BigQuery."""
    if not items:
        logging.info("No rarity data to store")
        return
    table_ref = f"{client.project}.{DATASET}.{DEST_TABLE}"
    df = pd.DataFrame(items)
    df.insert(0, "collection_address", collection.lower())
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    logging.info("Uploading %d rarity scores to %s", len(df), table_ref)
    client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()


async def run_rarity_calculator(collection_slug: str, collection_address: str) -> None:
    """Main function to compute rarity scores for a collection."""
    if not PROJECT_ID or not DATASET:
        logging.error("Missing GCP configuration")
        return

    client = bigquery.Client(project=PROJECT_ID)
    logging.info("Fetching metadata for %s", collection_slug)
    items = fetch_collection_metadata(collection_slug)
    if not items:
        logging.error("No metadata fetched")
        return

    freq = compute_frequencies(items)
    scores = compute_rarity_scores(items, freq)
    store_rarity_scores(client, collection_address, scores)


if __name__ == "__main__":
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(description="Compute NFT rarity scores")
    parser.add_argument("--slug", required=True, help="OpenSea collection slug")
    parser.add_argument("--address", required=True, help="Collection contract address")
    args = parser.parse_args()

    asyncio.run(run_rarity_calculator(args.slug, args.address))
