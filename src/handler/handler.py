import logging
import urllib.parse

import handler.log_manager as log_manager
import handler.storage as storage
import handler.transform as transform
import handler.validator as validator
from handler.log_type import LogType

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

REFINEMENT_LOG_KEY = "refinement-log.json"
QUARANTINE_LOG_KEY = "quarantine-log.json"


def handle_event(event, context) -> None:
    logger.debug("Starting event handling")
    record = event.get("Records", [{}])[0]
    logger.info(f"Processing record: {record}")

    bucket_name = record.get("s3", {}).get("bucket", {}).get("name")
    object_key = urllib.parse.unquote_plus(record.get("s3", {}).get("object", {}).get("key"))

    if not bucket_name or not object_key:
        logger.error("Missing bucket name or object key in event")
        raise ValueError("Invalid event structure")

    file_path = None

    try:
        file_path = storage.download_file_from_s3(bucket_name, object_key)
        logger.info(f"File downloaded to: {file_path}")

        df = validator.load_and_validate_csv(file_path)
        logger.info(f"File validated: {file_path}")

        parquet_file_path = transform.transform_dataframe_to_parquet(df, "data.parquet")
        logger.info(f"File transformed to Parquet: {parquet_file_path}")

        refined_key = object_key.replace("raw/", "refined/").replace(".csv", ".parquet")
        storage.upload_file_to_s3(refined_key, parquet_file_path)

        logger.info(f"File uploaded to refined bucket: {refined_key}")
        log_data = log_manager.download_log_file(REFINEMENT_LOG_KEY)
        log_data = log_manager.update_log(log_data, LogType.REFINEMENT, refined_key)
        log_manager.upload_log_file(REFINEMENT_LOG_KEY, log_data)
    except Exception as e:
        logger.exception(f"Error processing file {object_key}: {e}")

        if file_path:
            quarantine_key = object_key.replace("raw/", "quarantine/")
            storage.upload_file_to_s3(quarantine_key, file_path)

            logger.info(f"File moved to quarantine: {quarantine_key}")
            quarantine_log_data = log_manager.download_log_file(QUARANTINE_LOG_KEY)
            quarantine_log_data = log_manager.update_log(
                quarantine_log_data, LogType.QUARANTINE, quarantine_key
            )
            log_manager.upload_log_file(QUARANTINE_LOG_KEY, quarantine_log_data)

        raise
