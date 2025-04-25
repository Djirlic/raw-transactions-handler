import logging
import urllib.parse

import handler.storage as storage

logger = logging.getLogger(__name__)


def handle_event(event, context) -> None:
    logger.info("Starting event handling")
    record = event.get("Records", [{}])[0]
    logger.info(f"Processing record: {record}")
    bucket_name = record.get("s3", {}).get("bucket", {}).get("name")
    object_key = urllib.parse.unquote_plus(record.get("s3", {}).get("object", {}).get("key"))
    file_path = storage.download_file_from_s3(bucket_name, object_key)
    logger.info(f"File downloaded to: {file_path}")
