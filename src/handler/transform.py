import logging

import polars as pl

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def transform_dataframe_to_parquet(dataframe: pl.DataFrame, filename: str) -> str:
    """
    Transform DataFrame to Parquet file.

    Args:
        dataframe (pl.DataFrame): DataFrame containing the loaded CSV data.
        filename (str): Filename for the output Parquet file.

    Returns:
        str: Path to the output Parquet file.
    """
    logger.info(f"Transforming data into {filename}")
    tmp_path = f"/tmp/{filename}"
    try:
        dataframe.write_parquet(tmp_path)
        logger.info(f"File written to {tmp_path}")
        return tmp_path
    except Exception as e:
        logger.exception(f"Failed to write Parquet file: {e}")
        raise
