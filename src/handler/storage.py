import logging
import os

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
REFINED_BUCKET_NAME = os.getenv("REFINED_BUCKET_NAME")


def download_file_from_s3(bucket_name: str, object_key: str) -> str:
    """
    Download a file from an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        object_key (str): The S3 object key (path) of the file to download.
    Returns:
        str: The local path where the file is downloaded.
    """
    local_path = f"/tmp/{os.path.basename(object_key)}"

    s3.download_file(bucket_name, object_key, local_path)

    return local_path


def upload_file_to_s3(object_key: str, file_path: str) -> None:
    """
    Upload a file to an S3 bucket.

    Args:
        object_key (str): The S3 object key (path) where the file will be uploaded.
        file_path (str): The local path of the file to upload.

    Returns:
        None
    """
    logger.info(f"Attempting to upload {file_path} to s3://{REFINED_BUCKET_NAME}/{object_key}")
    try:
        s3.upload_file(file_path, REFINED_BUCKET_NAME, object_key)
        logger.info(f"File successfully uploaded to s3://{REFINED_BUCKET_NAME}/{object_key}")
    except Exception as e:
        logger.error(f"Error uploading {file_path} to s3://{REFINED_BUCKET_NAME}/{object_key}: {e}")
        raise
