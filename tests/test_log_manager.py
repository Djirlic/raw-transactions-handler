import json
from unittest.mock import mock_open, patch

import pytest
from botocore.exceptions import ClientError

import handler.log_manager as log_manager
from handler.log_type import LogType


@patch("handler.log_manager.REFINED_BUCKET_NAME", "test_bucket")
@patch("handler.storage.s3")
def test_download_log_file_with_no_such_key_error_returns_empty_dict(mock_s3_client):
    mock_s3_client.download_file.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist."}},
        "GetObject",
    )
    result = log_manager.download_log_file("log_key")
    assert result == {}


@patch("handler.log_manager.REFINED_BUCKET_NAME", "test_bucket")
@patch("handler.storage.s3")
def test_download_log_file_with_client_error_raises_exception(mock_s3_client):
    mock_s3_client.download_file.side_effect = ClientError(
        {"Error": {"Code": "SomeOtherError", "Message": "Some other error occurred."}},
        "GetObject",
    )
    with pytest.raises(ClientError, match="Some other error occurred."):
        log_manager.download_log_file("log_key")


@patch("handler.log_manager.REFINED_BUCKET_NAME", "test_bucket")
@patch("handler.storage.s3")
def test_download_log_file_with_exception_raises_exception(mock_s3_client):
    mock_s3_client.download_file.side_effect = Exception("Some Exception")
    with pytest.raises(Exception, match="Some Exception"):
        log_manager.download_log_file("log_key")


@patch("handler.log_manager.REFINED_BUCKET_NAME", None)
def test_download_log_file_with_empty_refined_bucket_name_raises_value_error():
    with pytest.raises(ValueError, match="REFINED_BUCKET_NAME environment variable is not set"):
        log_manager.download_log_file("log_key")


@patch("handler.log_manager.REFINED_BUCKET_NAME", "test_bucket")
@patch("handler.log_manager.download_file_from_s3")
@patch(
    "builtins.open",
    new_callable=mock_open,
    read_data='{"ingested_files": [{"timestamp": "2025-05-08T21:55:40.592096", "file": "refined/data.parquet"}]}',
)
def test_download_log_file_with_success_returns_dict(mock_open_func, mock_download_file):
    mock_download_file.return_value = "/tmp/test_log.json"

    result = log_manager.download_log_file("test_log.json")

    assert result == {
        "ingested_files": [
            {"timestamp": "2025-05-08T21:55:40.592096", "file": "refined/data.parquet"}
        ]
    }
    mock_download_file.assert_called_once_with("test_bucket", "test_log.json")
    mock_open_func.assert_called_once_with("/tmp/test_log.json", "r")


def test_update_log_with_refinement_type_returns_updated_log():
    log_data = {"ingested_files": []}
    object_key = "refined/data.parquet"

    result = log_manager.update_log(log_data, LogType.REFINEMENT, object_key)

    assert "ingested_files" in result
    assert len(result["ingested_files"]) == 1
    assert result["ingested_files"][0]["file"] == object_key
    assert "timestamp" in result["ingested_files"][0]


def test_update_log_with_quarantine_type_returns_updated_log():
    log_data = {"quarantined_files": []}
    object_key = "quarantine/data.csv"

    result = log_manager.update_log(log_data, LogType.QUARANTINE, object_key)

    assert "quarantined_files" in result
    assert len(result["quarantined_files"]) == 1
    assert result["quarantined_files"][0]["file"] == object_key
    assert "timestamp" in result["quarantined_files"][0]


@patch("handler.log_manager.upload_file_to_s3")
@patch("builtins.open", new_callable=mock_open)
@patch("handler.log_manager.REFINED_BUCKET_NAME", "test_bucket")
def test_upload_log_file_with_success_returns_none(mock_open_func, mock_upload_file):
    log_data = {
        "ingested_files": [
            {"timestamp": "2025-05-08T21:55:40.592096", "file": "refined/data.parquet"}
        ]
    }
    log_key = "refinement-log.json"

    log_manager.upload_log_file(log_key, log_data)

    mock_open_func.assert_called_once_with("/tmp/refinement-log.json", "w")
    handle = mock_open_func()
    written_content = "".join(call.args[0] for call in handle.write.call_args_list)
    assert json.loads(written_content) == log_data
    mock_upload_file.assert_called_once_with(log_key, "/tmp/refinement-log.json")


@patch("handler.log_manager.upload_file_to_s3")
@patch("builtins.open", new_callable=mock_open)
@patch("handler.log_manager.REFINED_BUCKET_NAME", "test_bucket")
def test_upload_log_file_with_exception_raises_exception(mock_open_func, mock_upload_file):
    log_data = {
        "ingested_files": [
            {"timestamp": "2025-05-08T21:55:40.592096", "file": "refined/data.parquet"}
        ]
    }
    log_key = "refinement-log.json"

    mock_upload_file.side_effect = Exception("Upload failed")

    with pytest.raises(Exception, match="Upload failed"):
        log_manager.upload_log_file(log_key, log_data)
