import polars as pl

import handler.handler as handler


def test_handle(mocker):
    dummy_event = {
        "Records": [{"s3": {"bucket": {"name": "dummy-bucket"}, "object": {"key": "dummy.csv"}}}]
    }
    dummy_context = object()
    mock_download = mocker.Mock(return_value="/tmp/dummy.csv")

    df = pl.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    mock_validate = mocker.Mock(return_value=df)
    mock_transform = mocker.Mock()

    mocker.patch("handler.storage.download_file_from_s3", mock_download)
    mocker.patch("handler.validator.load_and_validate_csv", mock_validate)
    mocker.patch("handler.transform.transform_dataframe_to_parquet", mock_transform)

    result = handler.handle_event(dummy_event, dummy_context)

    mock_download.assert_called_once_with("dummy-bucket", "dummy.csv")
    mock_validate.assert_called_once_with("/tmp/dummy.csv")
    mock_transform.assert_called_once_with(df, "/tmp/dummy.csv.parquet")
    assert result is None
