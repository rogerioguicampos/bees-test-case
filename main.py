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


def bronze_layer(url: str='https://api.openbrewerydb.org/v1/breweries'):
    raw_json_data = requests.get(url).json()
    df = pd.DataFrame(raw_json_data)
    df['date_request'] = STRING_DATE

    delete_partition_recursively(partition_path=f'data/bronze/date_request={STRING_DATE}')
    df.to_parquet(path=f'data/bronze/', partition_cols=['date_request'])


def silver_layer():
    df = pd.read_parquet('data/bronze/')

    delete_partition_recursively(partition_path=f'data/silver/date_request={STRING_DATE}')
    df.to_parquet(path='data/silver/', partition_cols=['date_request', 'country'])


def gold_layer(list_agg: list=['brewery_type', 'country', 'state_province']):
    df = pd.read_parquet('data/silver/')[list_agg]

    df_agg = (
        df
        .groupby(by=list_agg, observed=False)
        .size()
        .reset_index()
        .rename(columns={0: 'count'})
    )

    df_agg['date_request'] = STRING_DATE

    delete_partition_recursively(partition_path=f'data/gold/date_request={STRING_DATE}')
    df_agg.to_parquet(path='data/gold/', partition_cols=['date_request'])


if __name__ == "__main__":
    bronze_layer()
    silver_layer()
    gold_layer()
