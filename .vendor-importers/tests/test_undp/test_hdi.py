"""Unit tests for the HumanDevelopmentIndex importer
"""

import pytest
from unittest import mock
import pandas as pd
import numpy as np
from io import BytesIO

from bblocks.data_importers.undp import hdi
from bblocks.data_importers.config import Fields, DataExtractionError
from bblocks.data_importers.protocols import DataImporter


def test_protocol():
    """Test that importer class implements the DataImporter protocol"""

    importer_obj = hdi.HumanDevelopmentIndex()

    assert isinstance(
        importer_obj, DataImporter
    ), "HumanDevelopmentIndex does not implement DataImporter protocol"
    assert hasattr(
        importer_obj, "get_data"
    ), "HumanDevelopmentIndex does not have get_data method"
    assert hasattr(
        importer_obj, "clear_cache"
    ), "HumanDevelopmentIndex does not have clear_cache method"


# ------------------------------------------------------------------------------
# FIXTURES
# ------------------------------------------------------------------------------
@pytest.fixture
def mock_hdi_csv_content():
    """
    Mock CSV content for HDI data.
    This CSV fixture includes a small snippet of columns that represent the
    structure of your HDI data: iso3, country, region, hdicode, indicator_year pairs.
    """
    csv_str = (
        "iso3,country,region,hdicode,HDI_2000,HDI_2001,INEQUALITY_INDEX_2000\n"
        "ABC,Testland,TestRegion,High,0.500,0.505,0.1\n"
        "XYZ,Mockland,MockRegion,Low,0.300,0.305,0.2\n"
    )
    return csv_str.encode(hdi.DATA_ENCODING)


@pytest.fixture
def mock_hdi_metadata_content():
    """
    Mock Excel content for HDI metadata.
    Typically includes columns like 'Full name', 'Short name', 'Time series', 'Note'.
    """
    metadata_df = pd.DataFrame(
        {
            "Full name": ["Human Development Index", "Inequality Index"],
            "Short name": ["HDI", "INEQUALITY_INDEX"],
            "Time series": ["1990-2023", "1990-2023"],
            "Note": [
                "Composite index of life, ed, GNI",
                "Measures inequality dimension",
            ],
        }
    )

    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
        metadata_df.to_excel(writer, index=False)
    return excel_buffer.getvalue()


@pytest.fixture
def mock_hdi_df():
    """A pandas DataFrame that simulates the raw HDI data *before* cleaning."""
    return pd.DataFrame(
        {
            "iso3": ["ABC", "XYZ"],
            "country": ["Testland", "Mockland"],
            "region": ["TestRegion", "MockRegion"],
            "hdicode": ["High", "Low"],
            "HDI_2000": [0.5, 0.3],
            "HDI_2001": [0.505, 0.305],
            "INEQUALITY_INDEX_2000": [0.1, 0.2],
        }
    )


@pytest.fixture
def mock_metadata_df():
    """A pandas DataFrame that simulates the metadata *before* cleaning."""
    return pd.DataFrame(
        {
            "Full name": ["Human Development Index", "Inequality Index"],
            "Short name": ["HDI", "INEQUALITY_INDEX"],
            "Time series": ["1990-2023", "1990-2023"],
            "Note": [
                "Composite index of life, ed, GNI",
                "Measures inequality dimension",
            ],
        }
    )


# ------------------------------------------------------------------------------
# TESTS FOR LOWER-LEVEL FUNCTIONS
# ------------------------------------------------------------------------------
def test_request_hdi_data_success(mock_hdi_csv_content):
    """hdi._request_hdi_data should return a valid response when requests.get succeeds."""
    with mock.patch("requests.get") as mock_get:
        mock_response = mock.Mock()
        mock_response.content = mock_hdi_csv_content
        mock_response.raise_for_status = mock.Mock()
        mock_get.return_value = mock_response

        response = hdi._request_hdi_data(hdi.DATA_URL, timeout=5)
        mock_get.assert_called_once_with(hdi.DATA_URL, timeout=5)
        assert response.content == mock_hdi_csv_content
        mock_response.raise_for_status.assert_called_once()


def test_request_hdi_data_failure():
    """hdi._request_hdi_data should raise DataExtractionError when requests.get fails."""
    with mock.patch("requests.get", side_effect=Exception("Network error")):
        with pytest.raises(
            DataExtractionError, match="Error requesting HDI data: Network error"
        ):
            hdi._request_hdi_data(hdi.DATA_URL, timeout=5)


def test_read_hdi_data_success(mock_hdi_csv_content):
    """
    hdi.read_hdi_data should return a DataFrame with correct shape/columns
    when it successfully reads CSV data.
    """
    with mock.patch(
        "bblocks.data_importers.undp.hdi._request_hdi_data"
    ) as mock_request:
        mock_response = mock.Mock()
        mock_response.content = mock_hdi_csv_content
        mock_request.return_value = mock_response

        df = hdi.read_hdi_data(encoding=hdi.DATA_ENCODING, timeout=5)

        assert isinstance(df, pd.DataFrame)
        # 2 rows, 7 columns
        assert df.shape == (2, 7)
        expected_cols = [
            "iso3",
            "country",
            "region",
            "hdicode",
            "HDI_2000",
            "HDI_2001",
            "INEQUALITY_INDEX_2000",
        ]
        for col in expected_cols:
            assert col in df.columns


def test_read_hdi_data_parser_error():
    # 1) Create a mock response object with actual bytes for content
    mock_response = mock.Mock()
    mock_response.content = b"some raw CSV bytes"

    # 2) Patch _request_hdi_data so it returns this mock response
    with mock.patch(
        "bblocks.data_importers.undp.hdi._request_hdi_data", return_value=mock_response
    ):
        # 3) Force pandas.read_csv to raise a ParserError
        with mock.patch(
            "pandas.read_csv", side_effect=pd.errors.ParserError("Invalid CSV")
        ):
            # 4) Now your code attempts read_csv -> hits the mock side_effect -> raises
            with pytest.raises(DataExtractionError, match="Error reading HDI data:"):
                hdi.read_hdi_data()


def test_read_hdi_metadata_success(mock_hdi_metadata_content):
    """hdi.read_hdi_metadata should return a DataFrame with correct shape/columns on success."""
    with mock.patch(
        "bblocks.data_importers.undp.hdi._request_hdi_data"
    ) as mock_request:
        mock_response = mock.Mock()
        mock_response.content = mock_hdi_metadata_content
        mock_request.return_value = mock_response

        meta_df = hdi.read_hdi_metadata(timeout=5)
        assert isinstance(meta_df, pd.DataFrame)
        # 2 rows, 4 columns
        assert meta_df.shape == (2, 4)
        assert "Full name" in meta_df.columns
        assert "Short name" in meta_df.columns
        assert "Time series" in meta_df.columns
        assert "Note" in meta_df.columns


def test_read_hdi_metadata_parser_error():
    """If the Excel is malformed, hdi.read_hdi_metadata should raise DataExtractionError."""
    with mock.patch(
        "bblocks.data_importers.undp.hdi._request_hdi_data"
    ) as mock_request:
        mock_response = mock.Mock()
        mock_response.content = b"NOT_AN_EXCEL_FILE"
        mock_request.return_value = mock_response

        with pytest.raises(DataExtractionError, match="Error reading HDI metadata:"):
            hdi.read_hdi_metadata()


def test_clean_metadata(mock_metadata_df):
    """
    Test that hdi.clean_metadata:
    - drops rows with no 'Time series'
    - renames columns to the Fields
    - converts dtypes to pyarrow
    """
    # Add a row with NaN "Time series" to see if it's dropped
    mock_metadata_df.loc[len(mock_metadata_df)] = [
        "Extra",
        "EXTRA_CODE",
        np.nan,
        "No time series here",
    ]

    cleaned = hdi.clean_metadata(mock_metadata_df)
    # One row should be dropped => we get 2 original rows left
    assert cleaned.shape[0] == 2, "Rows with NaN in 'Time series' should be dropped."

    for col in [
        Fields.indicator_name,
        Fields.indicator_code,
        Fields.time_range,
        Fields.notes,
    ]:
        assert col in cleaned.columns, f"{col} not found in cleaned metadata columns."

    # Ensure arrow dtypes
    for dtype in cleaned.dtypes:
        assert isinstance(
            dtype, pd.ArrowDtype
        ), "Columns are not pyarrow dtypes in cleaned metadata."


def test_clean_data(mock_hdi_df, mock_metadata_df):
    """
    Test that hdi.clean_data:
    - melts from wide to long,
    - extracts 'year' from the column suffix,
    - maps indicator_code -> indicator_name from metadata,
    - converts columns to arrow dtypes.
    """
    meta_cleaned = hdi.clean_metadata(mock_metadata_df)

    cleaned_df = hdi.clean_data(mock_hdi_df, meta_cleaned)

    # We have 3 indicator columns: HDI_2000, HDI_2001, INEQUALITY_INDEX_2000
    # For 2 rows => 6 rows after melt
    assert cleaned_df.shape[0] == 6, "Melted rows don't match expected count."

    required_cols = [
        Fields.entity_code,
        Fields.entity_name,
        Fields.region_code,
        Fields.indicator_code,
        Fields.value,
        Fields.indicator_name,
        Fields.year,
    ]
    for col in required_cols:
        assert col in cleaned_df.columns, f"{col} missing in cleaned data."

    # Check year extraction
    unique_years = sorted(cleaned_df[Fields.year].dropna().unique().tolist())
    # [2000, 2001]
    assert unique_years == [2000, 2001], "Years not extracted correctly."

    # Check indicator_code & indicator_name were mapped correctly
    unique_codes = cleaned_df[Fields.indicator_code].unique().tolist()
    assert "HDI" in unique_codes
    assert "INEQUALITY_INDEX" in unique_codes

    # Check arrow dtypes
    for dtype in cleaned_df.dtypes:
        assert isinstance(
            dtype, pd.ArrowDtype
        ), "DataFrame columns are not pyarrow dtypes."


# ------------------------------------------------------------------------------
# TESTS FOR HumanDevelopmentIndex CLASS
# ------------------------------------------------------------------------------
@pytest.fixture
def mock_read_hdi_data_func(mock_hdi_df):
    """Mocks hdi.read_hdi_data to return mock_hdi_df."""
    with mock.patch(
        "bblocks.data_importers.undp.hdi.read_hdi_data", return_value=mock_hdi_df
    ):
        yield


@pytest.fixture
def mock_read_hdi_metadata_func(mock_metadata_df):
    """Mocks hdi.read_hdi_metadata to return mock_metadata_df."""
    with mock.patch(
        "bblocks.data_importers.undp.hdi.read_hdi_metadata",
        return_value=mock_metadata_df,
    ):
        yield


def test_hdi_init():
    """Test initial state of hdi.HumanDevelopmentIndex class."""
    hdi_importer = hdi.HumanDevelopmentIndex()
    assert hdi_importer._data_df is None, "Data should not be loaded at init."
    assert hdi_importer._metadata_df is None, "Metadata should not be loaded at init."


def test_extract_metadata(mock_read_hdi_metadata_func):
    """Test hdi.HumanDevelopmentIndex._extract_metadata loads and cleans metadata."""
    hdi_importer = hdi.HumanDevelopmentIndex()
    hdi_importer._extract_metadata()

    assert (
        hdi_importer._metadata_df is not None
    ), "Metadata DataFrame should be set after _extract_metadata()."
    assert (
        Fields.indicator_name in hdi_importer._metadata_df.columns
    ), "Expected column from cleaned metadata not found."


def test_extract_data(mock_read_hdi_data_func, mock_read_hdi_metadata_func):
    """
    Test hdi.HumanDevelopmentIndex._extract_data loads data and metadata (if not already present),
    cleans it, and populates _data_df.
    """
    hdi_importer = hdi.HumanDevelopmentIndex()
    hdi_importer._extract_data()

    assert (
        hdi_importer._data_df is not None
    ), "Data DataFrame should be set after _extract_data()."
    assert hdi_importer._metadata_df is not None, "Metadata should also be loaded."
    assert (
        Fields.entity_code in hdi_importer._data_df.columns
    ), "Cleaned data missing expected columns."


def test_get_metadata_cached(mock_read_hdi_metadata_func):
    """
    Test hdi.HumanDevelopmentIndex.get_metadata when metadata is already cached.
    Should return the cached version, not re-fetch.
    """
    hdi_importer = hdi.HumanDevelopmentIndex()
    cached_meta = pd.DataFrame({"test_col": [1, 2, 3]})
    hdi_importer._metadata_df = cached_meta

    meta = hdi_importer.get_metadata()
    assert meta.equals(cached_meta), "get_metadata did not return the cached metadata."


def test_get_metadata_not_cached(mock_read_hdi_metadata_func):
    """
    Test hdi.HumanDevelopmentIndex.get_metadata when metadata is not cached,
    so the method should call _extract_metadata.
    """
    hdi_importer = hdi.HumanDevelopmentIndex()
    meta = hdi_importer.get_metadata()

    assert hdi_importer._metadata_df is not None, "Metadata was not loaded."
    assert isinstance(meta, pd.DataFrame), "Expected a DataFrame from get_metadata."


def test_get_data_cached(mock_read_hdi_data_func, mock_read_hdi_metadata_func):
    """
    Test hdi.HumanDevelopmentIndex.get_data when data is already cached.
    Should just return the cached data, not re-fetch.
    """
    hdi_importer = hdi.HumanDevelopmentIndex()
    cached_data = pd.DataFrame({"test_col": [10, 20, 30]})
    hdi_importer._data_df = cached_data

    data = hdi_importer.get_data()
    assert data.equals(cached_data), "get_data did not return the cached data."


def test_get_data_not_cached(mock_read_hdi_data_func, mock_read_hdi_metadata_func):
    """
    Test hdi.HumanDevelopmentIndex.get_data when data is not cached,
    so it should call _extract_data.
    """
    hdi_importer = hdi.HumanDevelopmentIndex()
    data = hdi_importer.get_data()

    assert hdi_importer._data_df is not None, "Data was not loaded."
    assert isinstance(data, pd.DataFrame), "Expected a DataFrame from get_data."
    # Confirm metadata also gets loaded
    assert (
        hdi_importer._metadata_df is not None
    ), "Metadata should also be loaded by get_data."


def test_clear_cache(mock_read_hdi_data_func, mock_read_hdi_metadata_func):
    """
    Test hdi.HumanDevelopmentIndex.clear_cache resets both _data_df and _metadata_df to None.
    """
    hdi_importer = hdi.HumanDevelopmentIndex()
    # Load data so _data_df and _metadata_df are populated
    hdi_importer.get_data()
    assert hdi_importer._data_df is not None
    assert hdi_importer._metadata_df is not None

    hdi_importer.clear_cache()
    assert (
        hdi_importer._data_df is None
    ), "_data_df should be None after clearing cache."
    assert (
        hdi_importer._metadata_df is None
    ), "_metadata_df should be None after clearing cache."
