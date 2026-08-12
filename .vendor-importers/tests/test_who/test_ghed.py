"""Tests for GHED module"""

from bblocks.data_importers.who.ghed import GHED, URL
from bblocks.data_importers.config import DataExtractionError, DataFormattingError

import pytest
from unittest import mock
from pathlib import Path
import io
import requests
import pandas as pd
from bblocks.data_importers.protocols import DataImporter

TEST_FILE_PATH = "tests/test_data/test_ghed.XLSX"


def test_protocol():
    """Test that importer class implements the DataImporter protocol"""

    importer_obj = GHED()

    assert isinstance(
        importer_obj, DataImporter
    ), "GHED does not implement DataImporter protocol"
    assert hasattr(importer_obj, "get_data"), "GHED does not have get_data method"
    assert hasattr(importer_obj, "clear_cache"), "GHED does not have clear_cache method"


@pytest.fixture
def mock_raw_data():
    """Fixture for simulating raw data as BytesIO from the test file"""
    with open(TEST_FILE_PATH, "rb") as file:
        raw_data_content = file.read()
    return io.BytesIO(raw_data_content)


# Fixture for mocking the response
@pytest.fixture
def mock_successful_response():
    """Fixture for a successful response mock"""
    mock_response = mock.Mock()
    mock_response.status_code = 200
    mock_response.content = b"some binary content"
    return mock_response


@pytest.fixture
def mock_http_error_response():
    """Fixture for an HTTP error response (non-200 status code)"""
    mock_response = mock.Mock()
    mock_response.status_code = 404
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "404 Client Error"
    )
    return mock_response


def test_init_without_data_file():
    """Test initialization without a data file
    No file path passed, so it should not raise an exception
    """
    ghed = GHED()
    assert ghed._data_file is None
    assert ghed._raw_data is None
    assert ghed._data is None
    assert ghed._metadata is None


@mock.patch("bblocks.data_importers.who.ghed.Path.exists", return_value=True)
def test_init_with_valid_data_file(mock_exists):
    """Test initialization with a valid data file
    Passing a valid file path, so it should not raise an exception
    """
    ghed = GHED(data_file="some_valid_path.xlsx")
    assert isinstance(ghed._data_file, Path)
    mock_exists.assert_called_once()

    assert ghed._raw_data is None
    assert ghed._data is None
    assert ghed._metadata is None


@mock.patch("bblocks.data_importers.who.ghed.Path.exists", return_value=False)
def test_init_with_invalid_data_file(mock_exists):
    """Test initialization with an invalid data file
    Should raise a FileNotFoundError
    """
    with pytest.raises(FileNotFoundError):
        GHED(data_file="invalid_path.xlsx")
    mock_exists.assert_called_once()


@mock.patch("bblocks.data_importers.who.ghed.requests.get")
def test_extract_raw_data_success(mock_get, mock_successful_response):
    """Test successful data extraction
    Should return a BytesIO object with the correct content
    """
    # Use the fixture to set the mock response
    mock_get.return_value = mock_successful_response

    # Call the method
    result = GHED._extract_raw_data()

    # Check that the result is a BytesIO object with the correct content
    assert isinstance(result, io.BytesIO)
    assert result.getvalue() == b"some binary content"
    mock_get.assert_called_once_with(URL)


# Test case 2: Simulate network failure (RequestException)
@mock.patch("bblocks.data_importers.who.ghed.requests.get")
def test_extract_raw_data_request_exception(mock_get):
    """Test network failure during data extraction"""
    # Set the mock to raise a RequestException
    mock_get.side_effect = requests.exceptions.RequestException("Network error")

    # Assert that a DataExtractionError is raised when calling the method
    with pytest.raises(DataExtractionError):
        GHED._extract_raw_data()

    # Ensure that the GET request was attempted
    mock_get.assert_called_once_with(URL)


# Test case 3: Simulate non-200 status code (e.g., 404)
@mock.patch("bblocks.data_importers.who.ghed.requests.get")
def test_extract_raw_data_http_error(mock_get, mock_http_error_response):
    """Test HTTP error during data extraction"""
    # Use the fixture to set the mock response
    mock_get.return_value = mock_http_error_response

    # Assert that a DataExtractionError is raised when calling the method
    with pytest.raises(DataExtractionError):
        GHED._extract_raw_data()

    # Ensure that the GET request was made to the correct URL
    mock_get.assert_called_once_with(URL)


def test_format_metadata_success(mock_raw_data):
    """Test the _format_metadata method for successful formatting"""

    ghed = GHED()
    ghed._raw_data = mock_raw_data
    result = ghed._format_metadata()

    expected_metadata = pd.read_feather(
        "tests/test_data/formatted_metadata_ghed.feather", dtype_backend="pyarrow"
    )
    pd.testing.assert_frame_equal(result, expected_metadata)


def test_format_metadata_malformed_data():
    """Test the _format_metadata method raises a DataFormattingError when the data is malformed"""
    # Create some malformed data (e.g., random binary data that cannot be read as Excel)

    ghed = GHED()
    ghed._raw_data = io.BytesIO(b"invalid raw content")

    # Call the method and ensure it raises a DataFormattingError
    with pytest.raises(DataFormattingError):
        result = ghed._format_metadata()


def test_format_metadata_missing_columns(mock_raw_data):
    """Test the _format_metadata method raises a DataFormattingError when required columns are missing"""

    # format the raw data to remove a required column
    raw_df = pd.read_excel(mock_raw_data, sheet_name="Metadata")
    raw_df.drop(columns=["variable code"], inplace=True)
    modified_raw_data = io.BytesIO()

    with pd.ExcelWriter(modified_raw_data, engine="xlsxwriter") as writer:
        raw_df.to_excel(writer, sheet_name="Metadata")
    modified_raw_data.seek(0)  # Reset the pointer to the start of the BytesIO object

    ghed = GHED()
    ghed._raw_data = modified_raw_data

    # test
    with pytest.raises(DataFormattingError):
        ghed._format_metadata()


def test_format_data_success(mock_raw_data):
    """Test the _format_data method for successful formatting"""

    ghed = GHED()
    ghed._raw_data = mock_raw_data
    ghed._indicators = (
        ghed._format_codes()
    )  # Ensure that the codes are formatted before formatting data
    result = ghed._format_data()
    expected_data = pd.read_feather(
        "tests/test_data/formatted_data_ghed.feather", dtype_backend="pyarrow"
    )

    pd.testing.assert_frame_equal(result, expected_data)


def test_format_data_malformed_data():
    """Test the _format_data method raises a DataFormattingError when the data is malformed"""
    ghed = GHED()
    ghed._raw_data = io.BytesIO(b"invalid raw content")

    with pytest.raises(DataFormattingError):
        ghed._format_data()


def test_format_data_missing_columns(mock_raw_data):
    """Test the _format_data method raises a DataFormattingError when required columns are missing"""

    # format the raw data to remove a required column
    raw_df = pd.read_excel(mock_raw_data, sheet_name="Data")
    raw_df.drop(columns=["location"], inplace=True)
    modified_raw_data = io.BytesIO()
    with pd.ExcelWriter(modified_raw_data, engine="xlsxwriter") as writer:
        raw_df.to_excel(writer, sheet_name="Data")
    modified_raw_data.seek(0)  # Reset the pointer to the start of the BytesIO object

    ghed = GHED()
    ghed._raw_data = modified_raw_data

    # test
    with pytest.raises(DataFormattingError):
        result = ghed._format_data()


def test_format_data_missing_codes(mock_raw_data):
    """Test the _format_data method raises a DataFormattingError when the data contains invalid code tab"""

    # Load the entire Excel file to remove the "Codebook" sheet
    with pd.ExcelFile(mock_raw_data) as xls:
        sheet_names = xls.sheet_names  # Get all sheet names

        # Remove the "Codebook" sheet from the list of sheets
        sheet_names.remove("Codebook")
        modified_raw_data = io.BytesIO()

        # Write back only the remaining sheets, excluding "Codebook"
        with pd.ExcelWriter(modified_raw_data, engine="xlsxwriter") as writer:
            for sheet in sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet)
                df.to_excel(writer, sheet_name=sheet)

    ghed = GHED()
    ghed._raw_data = modified_raw_data

    # test
    with pytest.raises(DataFormattingError):
        ghed._indicators = (
            ghed._format_codes()
        )  # Ensure that the codes are formatted before formatting data
        result = ghed._format_data()


def test_format_data_merging_error(mock_raw_data):
    """Test the _format_data method raises a DataFormattingError when the data cannot be merged"""

    def test_format_data_codebook_missing_variable_code(mock_raw_data):
        """Test the _format_data method raises a DataFormattingError when 'variable code' column is missing from the Codebook"""

    # Read the "Data" sheet normally
    data_df = pd.read_excel(mock_raw_data, sheet_name="Data")

    # Read the "Codebook" sheet and drop the "variable code" column to simulate it being missing
    codebook_df = pd.read_excel(mock_raw_data, sheet_name="Codebook")
    codebook_df.drop(columns=["variable code"], inplace=True)

    # Write both modified DataFrame objects to a new BytesIO object
    modified_raw_data = io.BytesIO()
    with pd.ExcelWriter(modified_raw_data, engine="xlsxwriter") as writer:
        data_df.to_excel(writer, sheet_name="Data", index=False)
        codebook_df.to_excel(writer, sheet_name="Codebook", index=False)

    # Reset the pointer to the start of the BytesIO object
    modified_raw_data.seek(0)

    # Initialize the GHED object and set the modified raw data
    ghed = GHED()
    ghed._raw_data = modified_raw_data

    # Test
    with pytest.raises(DataFormattingError):
        ghed._indicators = (
            ghed._format_codes()
        )  # Ensure that the codes are formatted before formatting data
        result = ghed._format_data()


def test_read_local_data_success(mock_raw_data):
    """Test the _read_local_data method for successful reading"""
    # Create a mock Path object for the test
    mock_path = Path(TEST_FILE_PATH)

    # Call the method with the mock path
    result = GHED._read_local_data(mock_path)

    # Check that the result is a BytesIO object containing the correct content
    assert isinstance(result, io.BytesIO)
    assert result.getvalue() == mock_raw_data.getvalue()


@mock.patch("bblocks.data_importers.who.ghed.Path.open", side_effect=IOError)
def test_read_local_data_read_error(mock_open):
    """Test the _read_local_data method raises a DataExtractionError when the file cannot be read"""
    # Create a mock path for a corrupted or inaccessible file
    mock_path = Path("corrupted_file.xlsx")

    # Call the method and ensure it raises a DataExtractionError
    with pytest.raises(DataExtractionError):
        GHED._read_local_data(mock_path)

    # Check that the file open was attempted
    mock_open.assert_called_once_with("rb")


@mock.patch("bblocks.data_importers.who.ghed.GHED._extract_raw_data")
def test_load_data_no_local_file(mock_extract_raw_data, mock_raw_data):
    """Test that _extract_raw_data is called when no local file is provided"""

    mock_extract_raw_data.return_value = mock_raw_data

    ghed = GHED()
    ghed._load_data()

    # Ensure that _extract_raw_data is called
    mock_extract_raw_data.assert_called_once()


@mock.patch("bblocks.data_importers.who.ghed.GHED._extract_raw_data")
def test_load_data_invalid_data_format(mock_extract_raw_data):
    """Check that the data validation fails when the data format is invalid"""

    mock_extract_raw_data.return_value = io.BytesIO(b"invalid raw content")

    ghed = GHED()
    with pytest.raises(DataFormattingError):
        ghed._load_data()


@mock.patch("bblocks.data_importers.who.ghed.GHED._read_local_data")
def test_load_data_with_local_file(mock_read_local_data, mock_raw_data):
    """Test that _read_local_data is called when a local file is provided"""

    mock_read_local_data.return_value = mock_raw_data

    # Initialize GHED with a local file
    ghed = GHED(data_file=TEST_FILE_PATH)

    # Call the method to load data
    ghed._load_data()

    # Ensure that _read_local_data is called
    mock_read_local_data.assert_called_once()


def test_clear_cache():
    """Test that clear_cache sets _raw_data, _data, and _metadata to None"""

    ghed = GHED()
    ghed._raw_data = io.BytesIO(b"some raw data")
    ghed._data = pd.DataFrame({"column": [1, 2, 3]})
    ghed._metadata = pd.DataFrame({"column": ["meta1", "meta2", "meta3"]})

    # Ensure that the cache is populated before calling clear_cache
    assert ghed._raw_data is not None
    assert ghed._data is not None
    assert ghed._metadata is not None

    # Call clear_cache
    ghed.clear_cache()

    # Ensure that all cached data is cleared (set to None)
    assert ghed._raw_data is None
    assert ghed._data is None
    assert ghed._metadata is None


@mock.patch("bblocks.data_importers.who.ghed.Path.exists", return_value=True)
@mock.patch("bblocks.data_importers.who.ghed.open", new_callable=mock.mock_open)
def test_export_raw_data_success(mock_file_open, mock_path_exists):
    """Test that export_raw_data successfully saves the raw data to disk"""

    ghed = GHED()
    ghed._raw_data = io.BytesIO(b"some raw data")
    ghed.export_raw_data(
        directory="some_valid_directory", file_name="ghed_test", overwrite=True
    )

    # test
    mock_file_open.assert_called_once_with(
        Path("some_valid_directory") / "ghed_test.xlsx", "wb"
    )
    mock_file_open().write.assert_called_once_with(b"some raw data")


@mock.patch("bblocks.data_importers.who.ghed.Path.exists", return_value=True)
@mock.patch("bblocks.data_importers.who.ghed.open", new_callable=mock.mock_open)
def test_export_raw_data_file_exists_no_overwrite(mock_file_open, mock_path_exists):
    """Test that export_raw_data raises a FileExistsError if the file exists and overwrite is False"""

    ghed = GHED()
    ghed._raw_data = io.BytesIO(b"some raw data")
    mock_path_exists.return_value = True

    # Test that a FileExistsError is raised when overwrite is False
    with pytest.raises(FileExistsError):
        ghed.export_raw_data(
            directory="some_valid_directory", file_name="ghed_test", overwrite=False
        )

    # Ensure that the file was not opened for writing
    mock_file_open.assert_not_called()


@mock.patch("bblocks.data_importers.who.ghed.Path.exists", return_value=False)
def test_export_raw_data_directory_not_found(mock_path_exists):
    """Test that export_raw_data raises a FileNotFoundError if the directory does not exist"""

    ghed = GHED()
    ghed._raw_data = io.BytesIO(b"some raw data")
    mock_path_exists.return_value = False

    # Test that a FileNotFoundError is raised
    with pytest.raises(FileNotFoundError):
        ghed.export_raw_data(directory="non_existent_directory", file_name="ghed_test")

    # Ensure that the file was not opened for writing
    mock_path_exists.assert_called_once_with()


@mock.patch("bblocks.data_importers.who.ghed.GHED._load_data")
@mock.patch("bblocks.data_importers.who.ghed.Path.exists", return_value=True)
@mock.patch("bblocks.data_importers.who.ghed.open", new_callable=mock.mock_open)
def test_export_raw_data_calls_load_data_if_raw_data_none(
    mock_file_open, mock_path_exists, mock_load_data, mock_raw_data
):
    """Test that _load_data is called if _raw_data is None in export_raw_data"""

    ghed = GHED()
    mock_load_data.side_effect = lambda: setattr(ghed, "_raw_data", mock_raw_data)
    ghed.export_raw_data(
        directory="some_valid_directory", file_name="ghed_test", overwrite=True
    )

    # test
    mock_load_data.assert_called_once()
    mock_file_open.assert_called_once_with(
        Path("some_valid_directory") / "ghed_test.xlsx", "wb"
    )
    mock_file_open().write.assert_called_once_with(mock_raw_data.getvalue())


@mock.patch("bblocks.data_importers.who.ghed.GHED._load_data")
def test_get_data_calls_load_data_if_data_none(mock_load_data):
    """Test that get_data calls _load_data if _data is None and returns a DataFrame"""

    ghed = GHED()
    mock_load_data.side_effect = lambda: setattr(
        ghed, "_data", pd.DataFrame({"column": [1, 2, 3]})
    )
    result = ghed.get_data()

    # test
    mock_load_data.assert_called_once()
    assert isinstance(result, pd.DataFrame)


@mock.patch("bblocks.data_importers.who.ghed.GHED._load_data")
def test_get_indicators_calls_load_data_if_indicators_none(mock_load_data):
    """Test that get_indicators calls _load_data if _indicators is None and returns a DataFrame"""

    ghed = GHED()
    mock_load_data.side_effect = lambda: setattr(
        ghed, "_indicators", pd.DataFrame({"indicator": ["ind1", "ind2"]})
    )
    result = ghed.get_indicators()

    # test
    mock_load_data.assert_called_once()
    assert isinstance(result, pd.DataFrame)


@mock.patch("bblocks.data_importers.who.ghed.GHED._load_data")
def test_get_indicators_does_not_call_load_data_if_indicators_exists(mock_load_data):
    """Test that get_indicators does not call _load_data if _indicators is already populated"""

    ghed = GHED()
    ghed._indicators = pd.DataFrame({"indicator": ["ind1", "ind2"]})
    # Simulate that indicators are already loaded
    result = ghed.get_indicators()
    mock_load_data.assert_not_called()


@mock.patch("bblocks.data_importers.who.ghed.GHED._load_data")
def test_get_data_does_not_call_load_data_if_data_exists(mock_load_data):
    """Test that get_data does not call _load_data if _data is already populated"""

    ghed = GHED()
    ghed._data = pd.DataFrame(
        {"column": [1, 2, 3]}
    )  # Simulate that data is already loaded
    result = ghed.get_data()

    mock_load_data.assert_not_called()
    assert isinstance(result, pd.DataFrame)
    assert result.equals(ghed._data)  # Ensure it's the same data


@mock.patch("bblocks.data_importers.who.ghed.GHED._load_data")
def test_get_metadata_calls_load_data_if_metadata_none(mock_load_data):
    """Test that get_metadata calls _load_data if _metadata is None and returns a DataFrame"""

    ghed = GHED()
    mock_load_data.side_effect = lambda: setattr(
        ghed, "_metadata", pd.DataFrame({"column": ["meta1", "meta2"]})
    )
    result = ghed.get_metadata()

    # test
    mock_load_data.assert_called_once()
    assert isinstance(result, pd.DataFrame)


@mock.patch("bblocks.data_importers.who.ghed.GHED._load_data")
def test_get_metadata_does_not_call_load_data_if_metadata_exists(mock_load_data):
    """Test that get_metadata does not call _load_data if _metadata is already populated"""
    ghed = GHED()
    ghed._metadata = pd.DataFrame(
        {"column": ["meta1", "meta2"]}
    )  # Simulate that metadata is already loaded
    result = ghed.get_metadata()

    # test
    mock_load_data.assert_not_called()
    assert isinstance(result, pd.DataFrame)
    assert result.equals(ghed._metadata)  # Ensure it's the same metadata
