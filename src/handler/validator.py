import logging

import polars as pl

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_and_validate_csv(input_path: str) -> pl.DataFrame:
    """
    Load and validate CSV file.
    This function reads a CSV file, validates its schema, and checks for null-only columns.

    Args:
        input_path (str): Path to the input CSV file.

    Returns:
        pl.DataFrame: DataFrame containing the loaded CSV data.
    """
    logger.info(f"Loading and validating file from: {input_path}")
    expected_schema_mapping = {
        "trans_date_trans_time": pl.Datetime(),
        "cc_num": pl.Int64(),
        "merchant": pl.Utf8(),
        "category": pl.Utf8(),
        "amt": pl.Float64(),
        "first": pl.Utf8(),
        "last": pl.Utf8(),
        "gender": pl.Utf8(),
        "street": pl.Utf8(),
        "city": pl.Utf8(),
        "state": pl.Utf8(),
        "zip": pl.Utf8(),
        "lat": pl.Float64(),
        "long": pl.Float64(),
        "city_pop": pl.Int64(),
        "job": pl.Utf8(),
        "dob": pl.Date(),
        "trans_num": pl.Utf8(),
        "unix_time": pl.Int64(),
        "merch_lat": pl.Float64(),
        "merch_long": pl.Float64(),
        "is_fraud": pl.Int8(),
    }
    expected_schema = pl.Schema(expected_schema_mapping, check_dtypes=True)
    try:
        df = pl.read_csv(input_path, schema=expected_schema, try_parse_dates=True, quote_char='"')
        logger.info(f"Loaded {len(df)} rows from {input_path}")
    except Exception as e:
        logger.exception(f"Failed to load CSV with Polars: {e}")
        raise

    expected_columns = set(expected_schema_mapping.keys())

    null_only_columns = {
        col for col in expected_columns if col in df.columns and df[col].null_count() == len(df)
    }

    if null_only_columns:
        for col in null_only_columns:
            logger.error(f"Missing column in the DataFrame: {col}")

        raise ValueError(f"Missing columns in the DataFrame: {null_only_columns}")

    validate_is_fraud_column(df)
    validate_zip_column(df)

    logger.info(f"Loaded and validated {len(df)} rows from {input_path}")
    return df


def validate_is_fraud_column(df: pl.DataFrame) -> None:
    is_fraud_col = df.get_column("is_fraud")

    invalid = is_fraud_col.filter(~is_fraud_col.is_in([0, 1]))

    if invalid.len() > 0:
        raise ValueError(f"Invalid values found in is_fraud column: {invalid}")


def validate_zip_column(df: pl.DataFrame) -> None:
    zip_col = df.get_column("zip")
    invalid = zip_col.filter(~zip_col.cast(str).str.contains(r"^\d{5}(-\d{4})?$"))

    if invalid.len() > 0:
        raise ValueError(f"Invalid ZIP codes found: {invalid}")
