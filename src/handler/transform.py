import logging

import polars as pl

logger = logging.getLogger(__name__)


def transform_csv_to_parquet(input_path: str, output_path: str) -> None:
    """
    Transform CSV file to Parquet file.

    Args:
        input_path (str): Path to the input CSV file.
        output_path (str): Path to the output Parquet file.
    """
    logger.info(f"Transforming file from: {input_path} to: {output_path}")
    df = pl.read_csv(input_path)
    df.write_parquet(output_path)
