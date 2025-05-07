import logging
import urllib.parse

import handler.storage as storage
import handler.transform as transform
import handler.validator as validator

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handle_event(event, context) -> None:
    logger.debug("Starting event handling")
    record = event.get("Records", [{}])[0]
    logger.info(f"Processing record: {record}")

    bucket_name = record.get("s3", {}).get("bucket", {}).get("name")
    object_key = urllib.parse.unquote_plus(record.get("s3", {}).get("object", {}).get("key"))

    if not bucket_name or not object_key:
        logger.error("Missing bucket name or object key in event")
        raise ValueError("Invalid event structure")

    file_path = storage.download_file_from_s3(bucket_name, object_key)
    logger.info(f"File downloaded to: {file_path}")

    df = validator.load_and_validate_csv(file_path)
    logger.info(f"File validated: {file_path}")

    parquet_file_path = transform.transform_dataframe_to_parquet(df, "data.parquet")
    logger.info(f"File transformed to Parquet: {parquet_file_path}")

    storage.upload_file_to_s3(
        object_key.replace("raw/", "refined/").replace(".csv", ".parquet"), parquet_file_path
    )
