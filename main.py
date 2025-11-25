from datetime import datetime
import logging
import pandas as pd
from pathlib import Path
import requests
import shutil
import time
import sys


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
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
    start_fetch = time.time()
    all_data = []
    page = 1
    per_page = 200 # Max allowed

    logging.info("Connecting to API...")

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

    elapsed = time.time() - start_fetch
    logging.info(f"API Fetch complete: {len(all_data)} records in {elapsed:.2f}s")
    return all_data


def check_data_quality(new_df: pd.DataFrame, partition_path: str, threshold: float = 0.0) -> bool:
    """
    Validates the new data volume against existing data (Quality Gate).
    """
    path = Path(partition_path)

    # If no previous data exists, it's the first run. Pass.
    if not path.exists():
        logging.info("First run detected (no existing data). Quality Check Passed.")
        return True

    try:
        # Read only one column to count rows quickly
        existing_df = pd.read_parquet(path, columns=[new_df.columns[0]])
        old_count = len(existing_df)
        new_count = len(new_df)

        logging.info(f"Quality Check | Old: {old_count} vs New: {new_count} rows")

        if old_count == 0:
            return True

        # Case 1: Data Growth or Stable (Ideal scenario)
        if new_count >= old_count:
            logging.info("Data volume validated (Growth/Stable).")
            return True

        # Case 2: Data Shrinkage (Check Threshold)
        diff_pct = (old_count - new_count) / old_count

        if diff_pct <= threshold:
            logging.warning(f"Volume drop {diff_pct:.1%} within limit ({threshold:.0%}). Accepted.")
            return True
        else:
            logging.error(f"CRITICAL: Volume drop {diff_pct:.1%} exceeds limit. Blocked to prevent data loss.")
            return False

    except Exception as e:
        logging.warning(f"Could not read existing data: {e}. Proceeding.")
        return True


def bronze_layer(url: str = 'https://api.openbrewerydb.org/v1/breweries') -> None:
    """
    Ingests raw data from the API into the Bronze layer (Raw).
    Adds a 'date_request' column for partitioning.
    """
    t0 = time.time()
    logging.info("Starting Bronze Layer processing...")

    try:
        raw_data = fetch_data_with_pagination(url)
    except Exception:
        logging.error("Failed to fetch data from API.")
        return

    if not raw_data:
        logging.warning("API returned 0 records. Aborting.")
        return

    df = pd.DataFrame(raw_data)
    df['date_request'] = STRING_DATE

    partition_dir = f'data/bronze/date_request={STRING_DATE}'

    # --- Quality Gate Implementation ---
    if not check_data_quality(df, partition_dir):
        logging.error("Pipeline stopped at Bronze Layer due to Data Quality issues.")
        return
    # -----------------------------------

    delete_partition_recursively(partition_path=partition_dir)

    output_path = 'data/bronze/'
    Path(output_path).mkdir(parents=True, exist_ok=True)

    df.to_parquet(path=output_path, partition_cols=['date_request'])
    logging.info(f"<<< BRONZE FINISHED: Ingested {len(df)} rows in {time.time() - t0:.2f}s")


def silver_layer() -> None:
    """
    Reads from Bronze, transforms data, and saves to Silver layer.
    Partitions by 'date_request' and 'country'.
    """
    t0 = time.time()
    logging.info("Starting Silver Layer processing...")

    try:
        df = pd.read_parquet('data/bronze/')
    except Exception:
        logging.error("Bronze layer data not found. Run bronze_layer first.")
        return

    initial_rows = len(df)

    # Ensure ID is string and clean whitespace
    df['id'] = df['id'].astype(str).str.strip()

    # Handle NaNs in text columns to prevent Parquet schema issues
    text_cols = ['name', 'brewery_type', 'street', 'city', 'state_province', 'country']
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).replace('nan', '').replace('None', '')

    delete_partition_recursively(partition_path=f'data/silver/date_request={STRING_DATE}')

    output_path = 'data/silver/'
    Path(output_path).mkdir(parents=True, exist_ok=True)

    df.to_parquet(path=output_path, partition_cols=['date_request', 'country'])
    logging.info(f"<<< SILVER FINISHED: Processed {initial_rows} rows in {time.time() - t0:.2f}s")


def gold_layer(list_agg: list = ['brewery_type', 'country', 'state_province']) -> None:
    """
    Reads from Silver, aggregates brewery counts, and saves to Gold layer.

    Args:
        list_agg (list): Columns to group by.
    """
    t0 = time.time()
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
    logging.info(f"<<< GOLD FINISHED: Created {len(df_agg)} aggregated rows in {time.time() - t0:.2f}s")


if __name__ == "__main__":
    total_start = time.time()
    logging.info("PIPELINE EXECUTION STARTED")

    bronze_layer()
    silver_layer()
    gold_layer()

    logging.info(f"PIPELINE COMPLETED in {time.time() - total_start:.2f}s"
