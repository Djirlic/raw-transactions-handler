from unittest.mock import patch

import polars as pl
import pytest

import handler.transform as transform


@patch("handler.transform.pl.DataFrame.write_parquet")
def test_transform_dataframe_to_parquet__with_succes_returns_path(mock_write_parquet):
    mock_write_parquet.return_value = None
    df = pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    filename = "data.parquet"

    result_path = transform.transform_dataframe_to_parquet(df, filename)

    assert result_path == "/tmp/data.parquet"
    mock_write_parquet.assert_called_once_with("/tmp/data.parquet")


@patch("handler.transform.pl.DataFrame.write_parquet")
def test_transform_dataframe_to_parquet_with_exception_raises_exception(mock_write_parquet):
    mock_write_parquet.side_effect = Exception("Failed to write Parquet file")
    df = pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    filename = "data.parquet"

    with pytest.raises(Exception, match="Failed to write Parquet file"):
        transform.transform_dataframe_to_parquet(df, filename)
