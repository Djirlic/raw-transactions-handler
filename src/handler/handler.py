import logging
import urllib.parse

import handler.storage as storage
import handler.transform as transform

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handle_event(event, context) -> None:
    logger.info("Starting event handling")
    record = event.get("Records", [{}])[0]
    logger.info(f"Processing record: {record}")

    bucket_name = record.get("s3", {}).get("bucket", {}).get("name")
    object_key = urllib.parse.unquote_plus(record.get("s3", {}).get("object", {}).get("key"))

    if not bucket_name or not object_key:
        logger.error("Missing bucket name or object key in event")
        raise ValueError("Invalid event structure")

    file_path = storage.download_file_from_s3(bucket_name, object_key)
    logger.info(f"File downloaded to: {file_path}")

    output_path = f"/tmp/{object_key}.parquet"
    logger.info(f"Transforming file to: {output_path}")
    transform.transform_csv_to_parquet(file_path, output_path)
