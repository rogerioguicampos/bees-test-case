from datetime import datetime
import pandas as pd
from pathlib import Path
import requests
import shutil


string_date = datetime.now().strftime('%Y_%m_%d')

def delete_partition_recursively(partition_path: str='data/'):
    target_dir = Path(partition_path)

    if not target_dir.exists():
        print(f"Directory not found: {partition_path}")
        return

    has_parquet = any(target_dir.rglob("*.parquet"))

    if not has_parquet:
        print(f"No .parquet files found in {target_dir.name}. Skipping deletion.")
        return

    try:
        shutil.rmtree(target_dir)
        print(f"Successfully deleted partition and all subfolders: {target_dir.name}")
    except OSError as e:
        print(f"Error removing directory {target_dir.name}: {e}")


def bronze_layer(url: str='https://api.openbrewerydb.org/v1/breweries'):
    raw_json_data = requests.get(url).json()
    df = pd.DataFrame(raw_json_data)
    df['date_request'] = string_date

    delete_partition_recursively(partition_path=f'data/bronze/date_request={string_date}')
    df.to_parquet(path=f'data/bronze/', partition_cols=['date_request'])


def silver_layer():
    df = pd.read_parquet('data/bronze/')

    delete_partition_recursively(partition_path=f'data/silver/date_request={string_date}')
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

    df_agg['date_request'] = string_date

    delete_partition_recursively(partition_path=f'data/gold/date_request={string_date}')
    df_agg.to_parquet(path='data/gold/', partition_cols=['date_request'])


if __name__ == "__main__":
    bronze_layer()
    silver_layer()
    gold_layer()
