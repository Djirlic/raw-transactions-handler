import polars as pl

import handler.handler as handler


def test_handle(mocker):
    dummy_event = {
        "Records": [
            {"s3": {"bucket": {"name": "dummy-bucket"}, "object": {"key": "raw/data/dummy.csv"}}}
        ]
    }
    dummy_context = object()
    mock_download = mocker.Mock(return_value="/tmp/dummy.csv")

    df = pl.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    mock_validate = mocker.Mock(return_value=df)
    mock_transform = mocker.Mock(return_value="/tmp/data.parquet")
    mock_upload = mocker.Mock()
    mock_download_log_file = mocker.Mock(return_value={"ingested_files": []})
    mock_upload_log_file = mocker.Mock()

    mocker.patch("handler.storage.download_file_from_s3", mock_download)
    mocker.patch("handler.storage.upload_file_to_s3", mock_upload)
    mocker.patch("handler.validator.load_and_validate_csv", mock_validate)
    mocker.patch("handler.transform.transform_dataframe_to_parquet", mock_transform)
    mocker.patch("handler.log_manager.download_log_file", mock_download_log_file)
    mocker.patch("handler.log_manager.upload_log_file", mock_upload_log_file)

    result = handler.handle_event(dummy_event, dummy_context)

    mock_download.assert_called_once_with("dummy-bucket", "raw/data/dummy.csv")
    mock_validate.assert_called_once_with("/tmp/dummy.csv")
    mock_transform.assert_called_once_with(df, "data.parquet")
    mock_upload.assert_called_once_with("refined/data/dummy.parquet", "/tmp/data.parquet")
    mock_download_log_file.assert_called_once_with("refinement-log.json")
    mock_upload_log_file.assert_called_once_with(
        "refinement-log.json",
        {"ingested_files": [{"timestamp": mocker.ANY, "file": "refined/data/dummy.parquet"}]},
    )
    assert result is None
