from datetime import datetime
import logging
import pandas as pd
from pathlib import Path
import requests
import shutil
import time


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
STRING_DATE = datetime.now().strftime('%Y_%m_%d')

def delete_partition_recursively(partition_path: str = 'data/') -> None:
    """
    Recursively deletes a directory if it exists and contains .parquet files.
 
    Args:
        partition_path (str): The path to the directory to be cleaned.
    """
    target_dir = Path(partition_path)

    if not target_dir.exists():
        logging.info(f"Directory not found: {partition_path}")
        return

    has_parquet = any(target_dir.rglob("*.parquet"))

    if not has_parquet:
        logging.warning(f"No .parquet files found in {target_dir.name}. Skipping deletion.")
        return

    try:
        shutil.rmtree(target_dir)
        logging.info(f"Successfully deleted partition and all subfolders: {target_dir.name}")
    except OSError as e:
        logging.error(f"Error removing directory {target_dir.name}: {e}")


def fetch_data_with_pagination(base_url: str) -> list:
    """
    Fetches all data from the API handling pagination and basic retries.

    Args:
        base_url (str): The API endpoint URL.

    Returns:
        list: A list of dictionaries containing all records.
    """
    all_data = []
    page = 1
    per_page = 200  # Max allowed

    while True:
        try:
            url = f"{base_url}?page={page}&per_page={per_page}"
            logging.info(f"Fetching page {page}...")
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()

            if not data:
                break

            all_data.extend(data)
            page += 1
            time.sleep(0.5) # Respect API rate limits

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching page {page}: {e}")
            raise e

    logging.info(f"Total records fetched: {len(all_data)}")
    return all_data


def bronze_layer(url: str = 'https://api.openbrewerydb.org/v1/breweries') -> None:
    """
    Ingests raw data from the API into the Bronze layer (Raw).
    Adds a 'date_request' column for partitioning.
    """
    logging.info("Starting Bronze Layer processing...")
    raw_data = fetch_data_with_pagination(url)

    df = pd.DataFrame(raw_data)
    df['date_request'] = STRING_DATE

    # Clean existing partition to ensure idempotency
    delete_partition_recursively(partition_path=f'data/bronze/date_request={STRING_DATE}')

    output_path = 'data/bronze/'
    Path(output_path).mkdir(parents=True, exist_ok=True)

    df.to_parquet(path=output_path, partition_cols=['date_request'])
    logging.info("Bronze Layer completed.")


def silver_layer() -> None:
    """
    Reads from Bronze, transforms data, and saves to Silver layer.
    Partitions by 'date_request' and 'country'.
    """
    logging.info("Starting Silver Layer processing...")
    try:
        df = pd.read_parquet('data/bronze/')
    except Exception:
        logging.error("Bronze layer data not found. Run bronze_layer first.")
        return

    # Ensure ID is string and clean whitespace
    df['id'] = df['id'].astype(str).str.strip()

    # Clean existing partition
    delete_partition_recursively(partition_path=f'data/silver/date_request={STRING_DATE}')

    output_path = 'data/silver/'
    Path(output_path).mkdir(parents=True, exist_ok=True)

    df.to_parquet(path=output_path, partition_cols=['date_request', 'country'])
    logging.info("Silver Layer completed.")


def gold_layer(list_agg: list = ['brewery_type', 'country', 'state_province']) -> None:
    """
    Reads from Silver, aggregates brewery counts, and saves to Gold layer.

    Args:
        list_agg (list): Columns to group by.
    """
    logging.info("Starting Gold Layer processing...")
    try:
        df = pd.read_parquet('data/silver/')
    except Exception:
        logging.error("Silver layer data not found. Run silver_layer first.")
        return

    # Filter only necessary columns to optimize memory
    df = df[list_agg]

    df_agg = (
        df
        .groupby(by=list_agg, observed=False)
        .size()
        .reset_index()
        .rename(columns={0: 'count'})
    )

    df_agg['date_request'] = STRING_DATE

    delete_partition_recursively(partition_path=f'data/gold/date_request={STRING_DATE}')

    output_path = 'data/gold/'
    Path(output_path).mkdir(parents=True, exist_ok=True)

    df_agg.to_parquet(path=output_path, partition_cols=['date_request'])
    logging.info("Gold Layer completed.")


if __name__ == "__main__":
    bronze_layer()
    silver_layer()
    gold_layer()
