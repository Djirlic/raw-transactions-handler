import logging

import polars as pl

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def transform_dataframe_to_parquet(dataframe: pl.DataFrame, output_path: str) -> None:
    """
    Transform DataFrame to Parquet file.

    Args:
        dataframe (pl.DataFrame): DataFrame containing the loaded CSV data.
        output_path (str): Path to the output Parquet file.
    """
    logger.info(f"Transforming data to: {output_path}")
    dataframe.write_parquet(output_path)
