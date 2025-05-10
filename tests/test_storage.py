from unittest.mock import patch

import pytest

import handler.storage as storage


@patch("handler.storage.s3")
def test_download_file_from_s3_with_exception_raises_exception(mock_s3):
    mock_s3.download_file.side_effect = Exception("Download error")
    with pytest.raises(Exception, match="Download error"):
        storage.download_file_from_s3("bucket", "object_key")


@patch("handler.storage.s3")
def test_download_file_from_s3_with_success_returns_path(mock_s3):
    mock_s3.download_file.return_value = None

    result = storage.download_file_from_s3("bucket", "file.csv")
    assert result == "/tmp/file.csv"


@patch("handler.storage.s3")
def test_upload_file_to_s3_with_exception_raises_exception(mock_s3):
    mock_s3.upload_file.side_effect = Exception("Upload error")
    with pytest.raises(Exception, match="Upload error"):
        storage.upload_file_to_s3("object_key", "file/path/data.csv")


@patch("handler.storage.s3")
def test_upload_file_to_s3_with_success_returns_none(mock_s3):
    mock_s3.upload_file.return_value = None

    result = storage.upload_file_to_s3("object_key", "file/path/data.csv")
    assert result is None
