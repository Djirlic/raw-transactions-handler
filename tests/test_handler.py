from unittest.mock import ANY, Mock, patch

import polars as pl
import pytest

import handler.handler as handler


@pytest.fixture
def dummy_event():
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "test-bucket"},
                    "object": {"key": "raw/2025/01/01/data.csv"},
                }
            }
        ]
    }


@pytest.fixture
def dummy_context():
    return object()


@pytest.fixture
def mock_data():
    return pl.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})


@pytest.fixture
def mock_log_data():
    return {"ingested_files": []}


@patch("handler.storage.download_file_from_s3")
def test_handle_with_no_bucket_name_in_event_raises_error(
    mock_download, dummy_event, dummy_context
):
    event = {"Records": [{"s3": {"bucket": {}, "object": {"key": "raw/2025/01/01/data.csv"}}}]}
    with pytest.raises(ValueError, match="Invalid event structure"):
        handler.handle_event(event, dummy_context)


@patch("handler.storage.download_file_from_s3")
@patch("handler.log_manager.download_log_file")
@patch("handler.log_manager.upload_log_file")
def test_handle_with_download_file_error_raises_error(
    mock_upload_log, mock_download_log, mock_download, dummy_event, dummy_context
):
    mock_download.side_effect = Exception("Download error")
    with pytest.raises(Exception, match="Download error"):
        handler.handle_event(dummy_event, dummy_context)


@patch("handler.storage.download_file_from_s3", return_value="/tmp/data.csv")
@patch("handler.validator.load_and_validate_csv")
@patch("handler.log_manager.download_log_file")
@patch("handler.log_manager.upload_log_file")
def test_handle_with_validation_error_raises_error(
    mock_upload_log, mock_download_log, mock_validate, mock_download, dummy_event, dummy_context
):
    mock_validate.side_effect = Exception("Validation error")
    with pytest.raises(Exception):
        handler.handle_event(dummy_event, dummy_context)


@patch("handler.storage.download_file_from_s3", return_value="/tmp/data.csv")
@patch("handler.validator.load_and_validate_csv")
@patch("handler.storage.upload_file_to_s3")
@patch("handler.log_manager.download_log_file")
@patch("handler.log_manager.upload_log_file")
def test_handle_with_upload_error_raises_error(
    mock_upload_log,
    mock_download_log,
    mock_upload,
    mock_validate,
    mock_download,
    dummy_event,
    dummy_context,
):
    mock_validate.return_value = Mock()
    mock_upload.side_effect = Exception("Upload error")
    with pytest.raises(Exception, match="Upload error"):
        handler.handle_event(dummy_event, dummy_context)


@patch("handler.storage.download_file_from_s3", return_value="/tmp/data.csv")
@patch("handler.validator.load_and_validate_csv")
@patch("handler.storage.upload_file_to_s3")
@patch("handler.log_manager.download_log_file")
@patch("handler.log_manager.upload_log_file")
def test_handle_with_upload_log_error_raises_error(
    mock_upload_log,
    mock_download_log,
    mock_upload,
    mock_validate,
    mock_download,
    dummy_event,
    dummy_context,
):
    mock_validate.return_value = Mock()
    mock_upload_log.side_effect = Exception("Upload log error")
    with pytest.raises(Exception, match="Upload log error"):
        handler.handle_event(dummy_event, dummy_context)


@patch("handler.storage.download_file_from_s3")
@patch("handler.storage.upload_file_to_s3")
@patch("handler.validator.load_and_validate_csv")
@patch("handler.transform.transform_dataframe_to_parquet")
@patch("handler.log_manager.download_log_file")
@patch("handler.log_manager.upload_log_file")
def test_handle_with_all_steps_succeeding_transforms_and_logs_refinement(
    mock_upload_log_file,
    mock_download_log_file,
    mock_transform,
    mock_validate,
    mock_upload,
    mock_download,
    dummy_event,
    dummy_context,
    mock_data,
    mock_log_data,
):
    mock_download.return_value = "/tmp/dummy.csv"
    mock_validate.return_value = mock_data
    mock_transform.return_value = "/tmp/data.parquet"
    mock_upload.return_value = None
    mock_download_log_file.return_value = mock_log_data

    handler.handle_event(dummy_event, dummy_context)

    mock_download.assert_called_once_with("test-bucket", "raw/2025/01/01/data.csv")
    mock_validate.assert_called_once_with("/tmp/dummy.csv")
    mock_transform.assert_called_once_with(mock_data, "data.parquet")
    mock_upload.assert_called_once_with("refined/2025/01/01/data.parquet", "/tmp/data.parquet")
    mock_download_log_file.assert_called_once_with("refinement-log.json")
    mock_upload_log_file.assert_called_once_with(
        "refinement-log.json",
        {"ingested_files": [{"timestamp": ANY, "file": "refined/2025/01/01/data.parquet"}]},
    )
