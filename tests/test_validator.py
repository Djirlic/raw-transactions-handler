from io import StringIO
from unittest.mock import patch

import polars as pl
import pytest

import handler.validator as validator


@pytest.fixture
def input_path():
    return "/tmp/dummy.csv"


@pytest.fixture
def missing_columns_csv():
    csv_data = """
    trans_date_trans_time,cc_num,merchant,category,amt,first,last,gender,street,city,state,zip,lat,long,city_pop,job,dob,trans_num,unix_time,merch_lat,merch_long
    2019-01-20 00:00:23,4935858973307492,fraud_Miller-Hauck,grocery_pos,100.0,Lance,Wagner,M,6003 Brady Shoal Apt. 449,Irwinton,GA,31042,32.8088,-83.17399999999999,1841,Film/video editor,1975-06-01,77993e691b6dc1739db700be27fb9adf,1327017623,32.886358,-83.371659
    2019-01-20 00:02:05,676309913934,fraud_Murray-Smitham,grocery_pos,50.0,Robert,Martinez,M,3683 Parrish Circles,Pueblo,CO,81005,38.2352,-104.66,151815,Further education lecturer,1988-01-04,fa6d1757257185718850f6dd308a609d,1327017725,38.994192,-105.02271
    """
    lines = csv_data.strip().split("\n")
    cleaned_lines = [line.strip() for line in lines]
    return StringIO("\n".join(cleaned_lines))


@patch("handler.validator.pl.read_csv")
def test_load_and_validate_csv_with_read_csv_exception(mock_read_csv, input_path):
    mock_read_csv.side_effect = Exception("Failed to read CSV")

    with pytest.raises(Exception, match="Failed to read CSV"):
        validator.load_and_validate_csv(input_path)


@patch("handler.validator.pl.read_csv")
def test_load_and_validate_csv_with_invalid_fraud_value_raises_exception(mock_read_csv, input_path):
    mock_data = pl.DataFrame(
        {
            "trans_date_trans_time": ["2019-01-01 00:00:18", "2019-01-01 00:00:44"],
            "cc_num": [1234567890123456, 9876543210987654],
            "merchant": ["merchant1", "merchant2"],
            "category": ["groceries", "fuel"],
            "amt": [100.0, 50.0],
            "first": ["John", "Jane"],
            "last": ["Doe", "Smith"],
            "gender": ["M", "F"],
            "street": ["Street 1", "Street 2"],
            "city": ["City 1", "City 2"],
            "state": ["NY", "CA"],
            "zip": ["12345", "67890"],
            "lat": [40.7128, 34.0522],
            "long": [-74.006, -118.2437],
            "city_pop": [100000, 200000],
            "job": ["Engineer", "Doctor"],
            "dob": ["1980-01-01", "1990-01-01"],
            "trans_num": ["abc123", "def456"],
            "unix_time": [1327017623, 1327017725],
            "merch_lat": [40.7128, 34.0522],
            "merch_long": [-74.006, -118.2437],
            "is_fraud": [0, 3],
        }
    )

    mock_read_csv.return_value = mock_data

    with pytest.raises(Exception, match="Invalid values found in is_fraud column"):
        validator.load_and_validate_csv(input_path)


def test_load_and_validate_csv_with_missing_columns(missing_columns_csv, tmp_path):
    temp_csv_path = tmp_path / "missing_columns.csv"
    with open(temp_csv_path, "w") as f:
        f.write(missing_columns_csv.getvalue())

    with pytest.raises(ValueError, match="Missing column in the DataFrame: is_fraud"):
        validator.load_and_validate_csv(str(temp_csv_path))


@patch("handler.validator.pl.read_csv")
def test_load_and_validate_csv_with_zip_value_raises_exception(mock_read_csv, input_path):
    mock_data = pl.DataFrame(
        {
            "trans_date_trans_time": ["2019-01-01 00:00:18", "2019-01-01 00:00:44"],
            "cc_num": [1234567890123456, 9876543210987654],
            "merchant": ["merchant1", "merchant2"],
            "category": ["groceries", "fuel"],
            "amt": [100.0, 50.0],
            "first": ["John", "Jane"],
            "last": ["Doe", "Smith"],
            "gender": ["M", "F"],
            "street": ["Street 1", "Street 2"],
            "city": ["City 1", "City 2"],
            "state": ["NY", "CA"],
            "zip": ["12345-6", "67821"],
            "lat": [40.7128, 34.0522],
            "long": [-74.006, -118.2437],
            "city_pop": [100000, 200000],
            "job": ["Engineer", "Doctor"],
            "dob": ["1980-01-01", "1990-01-01"],
            "trans_num": ["abc123", "def456"],
            "unix_time": [1327017623, 1327017725],
            "merch_lat": [40.7128, 34.0522],
            "merch_long": [-74.006, -118.2437],
            "is_fraud": [0, 1],
        }
    )

    mock_read_csv.return_value = mock_data

    with pytest.raises(Exception):
        validator.load_and_validate_csv(input_path)


@patch("handler.validator.pl.read_csv")
def test_load_and_validate_csv_with_valid_csv_returns_dataframe(mock_read_csv, input_path):
    mock_data = pl.DataFrame(
        {
            "trans_date_trans_time": ["2019-01-01 00:00:18", "2019-01-01 00:00:44"],
            "cc_num": [1234567890123456, 9876543210987654],
            "merchant": ["merchant1", "merchant2"],
            "category": ["groceries", "fuel"],
            "amt": [100.0, 50.0],
            "first": ["John", "Jane"],
            "last": ["Doe", "Smith"],
            "gender": ["M", "F"],
            "street": ["Street 1", "Street 2"],
            "city": ["City 1", "City 2"],
            "state": ["NY", "CA"],
            "zip": ["12345", "67821"],
            "lat": [40.7128, 34.0522],
            "long": [-74.006, -118.2437],
            "city_pop": [100000, 200000],
            "job": ["Engineer", "Doctor"],
            "dob": ["1980-01-01", "1990-01-01"],
            "trans_num": ["abc123", "def456"],
            "unix_time": [1327017623, 1327017725],
            "merch_lat": [40.7128, 34.0522],
            "merch_long": [-74.006, -118.2437],
            "is_fraud": [0, 1],
        }
    )

    mock_read_csv.return_value = mock_data

    result = validator.load_and_validate_csv(input_path)
    assert result.equals(mock_data)
