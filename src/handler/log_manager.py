import json
import logging
import os
from datetime import datetime

from botocore.exceptions import ClientError

from handler.log_type import LogType
from handler.storage import REFINED_BUCKET_NAME, download_file_from_s3, upload_file_to_s3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def download_log_file(log_key: str) -> dict:
    """
    Download the log file and return its contents as a dictionary.
    If the log file doesn't exist, return an empty dictionary.
    """
    try:
        if not REFINED_BUCKET_NAME:
            raise ValueError("REFINED_BUCKET_NAME environment variable is not set")
        local_path = download_file_from_s3(REFINED_BUCKET_NAME, log_key)
        with open(local_path, "r") as file:
            return json.load(file)
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            logger.warning(f"Log file {log_key} not found, creating a new one.")
            return {}
        logger.error(f"Error downloading log file {log_key}: {e}")
        raise
    except Exception as e:
        logger.exception(f"Error downloading log file {log_key}: {e}")
        raise


def update_log(log_data: dict, log_type: LogType, object_key: str) -> dict:
    """
    Update the log data with the new entry.
    """
    entry = {"timestamp": datetime.now().isoformat(), "file": object_key}

    log_key = "ingested_files" if log_type == LogType.REFINEMENT else "quarantined_files"
    log_data.setdefault(log_key, []).append(entry)

    return log_data


def upload_log_file(log_key: str, log_data: dict) -> None:
    """
    Upload the updated log file to S3.
    """
    local_path = f"/tmp/{os.path.basename(log_key)}"
    try:
        with open(local_path, "w") as file:
            json.dump(log_data, file, indent=2)

        upload_file_to_s3(log_key, local_path)
        logger.info(f"Log file {log_key} successfully updated in bucket {REFINED_BUCKET_NAME}.")
    except Exception as e:
        logger.exception(f"Error uploading log file {log_key}: {e}")
        raise
