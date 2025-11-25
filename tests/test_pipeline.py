import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from main import bronze_layer, silver_layer, gold_layer, delete_partition_recursively

# Mock data to simulate API response
MOCK_API_DATA = [
    {"id": "b1", "name": "Brew 1", "brewery_type": "micro", "country": "USA", "state_province": "CA"},
    {"id": "b2", "name": "Brew 2", "brewery_type": "large", "country": "USA", "state_province": "NY"},
]

@pytest.fixture
def mock_parquet_read():
    """Fixture to mock reading parquet files"""
    df = pd.DataFrame(MOCK_API_DATA)
    df['date_request'] = '2024_01_01'
    return df

def test_delete_partition_recursively(tmp_path):
    # Create a dummy directory and file
    d = tmp_path / "data"
    d.mkdir()
    p = d / "test.parquet"
    p.write_text("content")
    
    delete_partition_recursively(str(d))
    assert not d.exists()

@patch('main.requests.get')
@patch('main.pd.DataFrame.to_parquet')
def test_bronze_layer(mock_to_parquet, mock_get):
    # Mock API pagination: First call returns data, second returns empty (end of pages)
    mock_response_1 = MagicMock()
    mock_response_1.json.return_value = MOCK_API_DATA
    mock_response_1.raise_for_status.return_value = None

    mock_response_2 = MagicMock()
    mock_response_2.json.return_value = []
    
    mock_get.side_effect = [mock_response_1, mock_response_2]

    bronze_layer()
    
    assert mock_get.call_count == 2
    mock_to_parquet.assert_called_once()

@patch('main.pd.read_parquet')
@patch('main.pd.DataFrame.to_parquet')
def test_silver_layer(mock_to_parquet, mock_read, mock_parquet_read):
    mock_read.return_value = mock_parquet_read
    
    silver_layer()
    
    mock_read.assert_called()
    # Verify partitioning by country
    call_args = mock_to_parquet.call_args
    assert 'country' in call_args[1]['partition_cols']

@patch('main.pd.read_parquet')
@patch('main.pd.DataFrame.to_parquet')
def test_gold_layer(mock_to_parquet, mock_read, mock_parquet_read):
    mock_read.return_value = mock_parquet_read
    
    gold_layer()
    
    # Check if aggregation happened (columns should be agg columns + count)
    saved_df = mock_to_parquet.call_args[0][0] # Get the dataframe passed to to_parquet
    assert 'count' in saved_df.columns
