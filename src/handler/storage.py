import os

import boto3

s3 = boto3.client("s3")


def download_file_from_s3(bucket_name: str, object_key: str) -> str:
    local_path = f"/tmp/{os.path.basename(object_key)}"

    s3.download_file(bucket_name, object_key, local_path)

    return local_path
