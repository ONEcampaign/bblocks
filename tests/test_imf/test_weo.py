"""Test weo module"""

import pytest
from unittest import mock
import pandas as pd

from bblocks.importers import WEO
from bblocks.importers.config import (
    Fields,
    DataExtractionError,
    DataFormattingError,
)
from bblocks.importers.protocols import DataImporter


def test_protocol():
    """Test that importer class implements the DataImporter protocol"""

    importer_obj = WEO()

    assert isinstance(importer_obj, DataImporter), (
        "WEO does not implement DataImporter protocol"
    )
    assert hasattr(importer_obj, "get_data"), "WEO does not have get_data method"
    assert hasattr(importer_obj, "clear_cache"), "WEO does not have clear_cache method"


@pytest.fixture
def mock_weo_data():
    """Fixture for simulating WEO data as a DataFrame, matching imf-reader 2.0's schema.

    REF_AREA_CODE is a string: ISO3 for real countries (USA, LIE), a synthetic
    G-prefixed code for aggregates (G001, World). REF_AREA_IMF_CODE carries
    the pre-translation legacy numeric IMF code for every row, and is null for
    entities that never had a legacy code, such as Liechtenstein.
    """
    data = {
        "UNIT_CODE": ["B", "B", "B", "B"],
        "CONCEPT_CODE": ["NGDP_D", "NGDP_D", "NGDP_D", "NGDP_D"],
        "REF_AREA_CODE": ["USA", "USA", "G001", "LIE"],
        "REF_AREA_IMF_CODE": pd.array([111, 111, 1, None], dtype="Int64"),
        "FREQ_CODE": ["A", "A", "A", "A"],
        "LASTACTUALDATE": [2023, 2023, 2023, 2023],
        "SCALE_CODE": [1, 1, 1, 1],
        "NOTES": [
            "See notes for: Gross domestic product, constant prices (National currency) Gross domestic product, current prices (National currency).",
            "See notes for: Gross domestic product, constant prices (National currency) Gross domestic product, current prices (National currency).",
            "See notes for: Gross domestic product, constant prices (National currency) Gross domestic product, current prices (National currency).",
            "See notes for: Gross domestic product, constant prices (National currency) Gross domestic product, current prices (National currency).",
        ],
        "TIME_PERIOD": [1980, 1981, 1981, 1981],
        "OBS_VALUE": [39.372, 43.097, 41.5, 21.0],
        "UNIT_LABEL": ["Index", "Index", "Index", "Index"],
        "CONCEPT_LABEL": [
            "Gross domestic product, deflator",
            "Gross domestic product, deflator",
            "Gross domestic product, deflator",
            "Gross domestic product, deflator",
        ],
        "REF_AREA_LABEL": [
            "United States",
            "United States",
            "World",
            "Liechtenstein, Principality of",
        ],
        "FREQ_LABEL": ["Annual", "Annual", "Annual", "Annual"],
        "SCALE_LABEL": ["Units", "Units", "Units", "Units"],
    }
    return pd.DataFrame(data)


@pytest.fixture
def mock_fetch_data(mock_weo_data):
    """Fixture to mock the weo.fetch_data method"""
    with (
        mock.patch("imf_reader.weo.fetch_data", return_value=mock_weo_data),
        mock.patch("imf_reader.weo.fetch_data.last_version_fetched", ("April", 2023)),
    ):
        yield


@pytest.fixture
def mock_fetch_data_specific_version(mock_weo_data):
    """Fixture to mock the weo.fetch_data method for a specific version"""

    with (
        mock.patch("imf_reader.weo.fetch_data", return_value=mock_weo_data),
        mock.patch("imf_reader.weo.fetch_data.last_version_fetched", ("October", 2022)),
    ):
        yield


# TESTS


def test_weo_init():
    """Test WEO class initialization"""
    weo_importer = WEO()

    assert weo_importer._data == {}
    assert weo_importer._latest_version is None


def test_format_data(mock_weo_data):
    """Test the _format_data method for successful formatting"""

    weo_importer = WEO()

    # Call the _format_data method with mock data
    formatted_data = weo_importer._format_data(mock_weo_data)

    # Verify that the correct columns have been renamed
    expected_columns = {
        "obs_value": Fields.value,
        "time_period": Fields.year,
        "ref_area_code": Fields.entity_code,
        "ref_area_imf_code": Fields.imf_code,
        "ref_area_label": Fields.entity_name,
        "concept_code": Fields.indicator_code,
        "concept_label": Fields.indicator_name,
        "unit_label": Fields.unit,
        "lastactualdate": "last_actual_date",
    }

    # Check that the columns have been renamed and are in lowercase
    for old_col, new_col in expected_columns.items():
        assert new_col in formatted_data.columns, f"Column {new_col} missing"
        assert old_col.lower() not in formatted_data.columns, (
            f"Old column {old_col} still present"
        )

    # Ensure all column names are lowercase
    assert all(col.islower() for col in formatted_data.columns), (
        "Not all columns are lowercase"
    )

    # Check that all dtypes are pyarrow types
    assert all(isinstance(dtype, pd.ArrowDtype) for dtype in formatted_data.dtypes), (
        "Not all columns have pyarrow types"
    )


def test_load_data_no_version_no_cached_data(mock_fetch_data, mock_weo_data):
    """Test _load_data when no version is passed and no cached data exists"""

    # Initialize the WEO object (no data is loaded yet)
    weo_importer = WEO()

    # Ensure no data exists in the cache initially
    assert weo_importer._data == {}, "Data cache should be empty initially"
    assert weo_importer._latest_version is None, (
        "Latest version should be None initially"
    )

    # Call the _load_data method (without passing a version)
    weo_importer._load_data()

    # Ensure data is fetched and cached with a tuple key
    assert weo_importer._latest_version == (
        "April",
        2023,
    ), "Latest version not updated correctly"
    assert (
        "April",
        2023,
    ) in weo_importer._data, "Data not stored correctly for the version"

    # Ensure the fetched data is a DataFrame
    assert isinstance(weo_importer._data[("April", 2023)], pd.DataFrame), (
        "Fetched data should be a DataFrame"
    )


def test_load_data_with_specific_version(
    mock_fetch_data_specific_version, mock_weo_data
):
    """Test _load_data when a specific version is passed"""

    # Initialize the WEO object (no data is loaded yet)
    weo_importer = WEO()

    # Call the _load_data method with a specific version
    version = ("October", 2022)
    weo_importer._load_data(version=version)

    # Ensure the specific version data is fetched and cached
    assert version in weo_importer._data, (
        f"Data not stored correctly for version {version}"
    )

    # Ensure the fetched data is a DataFrame
    assert isinstance(weo_importer._data[version], pd.DataFrame), (
        "Fetched data should be a DataFrame"
    )

    # Since we passed a specific version, _latest_version should not be updated
    assert weo_importer._latest_version is None, (
        "Latest version should remain None when a specific version is passed"
    )


def test_load_data_with_existing_data(mock_fetch_data, mock_weo_data):
    """Test _load_data when there is already some data cached and the latest version is requested"""

    # Initialize the WEO object
    weo_importer = WEO()

    # Manually load data for a specific version (October 2022)
    weo_importer._data[("October", 2022)] = mock_weo_data
    weo_importer._latest_version = None  # No latest version yet

    # Ensure the specific version data is preloaded
    assert (
        "October",
        2022,
    ) in weo_importer._data, "October 2022 data not preloaded correctly"
    assert weo_importer._latest_version is None, "Latest version should remain None"

    # Now, load the latest version data
    weo_importer._load_data()

    # Ensure both versions are now in the cache
    assert (
        "October",
        2022,
    ) in weo_importer._data, "October 2022 data missing after loading latest"
    assert (
        "April",
        2023,
    ) in weo_importer._data, "Latest version data not cached correctly"

    # Ensure _latest_version is updated correctly to the latest version
    assert weo_importer._latest_version == (
        "April",
        2023,
    ), "Latest version not updated correctly"

    # Ensure both cached datasets are DataFrames
    assert isinstance(weo_importer._data[("October", 2022)], pd.DataFrame), (
        "October 2022 data should be a DataFrame"
    )
    assert isinstance(weo_importer._data[("April", 2023)], pd.DataFrame), (
        "Latest version data should be a DataFrame"
    )


def test_load_data_data_extraction_error():
    """Test _load_data raises DataExtractionError when data fetch fails"""

    # Mock fetch_data to raise an exception (simulating a failure in fetching data)
    with mock.patch("imf_reader.weo.fetch_data", side_effect=Exception("Fetch error")):
        weo_importer = WEO()

        # Ensure _load_data raises DataExtractionError
        with pytest.raises(
            DataExtractionError, match="Failed to fetch data: Fetch error"
        ):
            weo_importer._load_data()


def test_load_data_data_formatting_error(mock_weo_data):
    """Test _load_data raises DataFormattingError when data formatting fails"""

    # Mock fetch_data to return valid data
    with mock.patch("imf_reader.weo.fetch_data", return_value=mock_weo_data):
        # Mock _format_data to raise an exception (simulating a formatting failure)
        with mock.patch.object(
            WEO, "_format_data", side_effect=Exception("Formatting error")
        ):
            weo_importer = WEO()

            # Ensure _load_data raises DataFormattingError
            with pytest.raises(
                DataFormattingError, match="Error formatting data: Formatting error"
            ):
                weo_importer._load_data()


def test_clear_cache(mock_fetch_data, mock_weo_data):
    """Test that clear_cache sets _data and _latest_version to initial state"""

    # Initialize the WEO object and load some data
    weo_importer = WEO()
    weo_importer._load_data()

    # Ensure data is cached
    assert ("April", 2023) in weo_importer._data, "Data not cached correctly"
    assert weo_importer._latest_version == (
        "April",
        2023,
    ), "Latest version not set correctly"

    # Clear the cache
    weo_importer.clear_cache()

    # Ensure the cache is cleared
    assert weo_importer._data == {}, "Data cache should be empty after clearing"
    assert weo_importer._latest_version is None, (
        "Latest version should be None after clearing"
    )


def test_get_data_no_version(mock_fetch_data, mock_weo_data):
    """Test get_data when no version is passed"""

    # Initialize the WEO object
    weo_importer = WEO()

    # Call get_data (should load the latest version)
    data = weo_importer.get_data()

    # Ensure the data is fetched and cached under the correct version key
    assert weo_importer._latest_version == (
        "April",
        2023,
    ), "Latest version not updated correctly"
    assert (
        "April",
        2023,
    ) in weo_importer._data, "Latest version data not cached correctly"

    # Ensure the returned data is a DataFrame
    assert isinstance(data, pd.DataFrame), "Returned data should be a DataFrame"


def test_get_data_with_cached_latest_data(mock_weo_data):
    """Test get_data when the latest version data is already cached"""

    # Initialize the WEO object
    weo_importer = WEO()

    # Manually cache the latest version data
    weo_importer._data[("April", 2023)] = mock_weo_data
    weo_importer._latest_version = ("April", 2023)

    # Call get_data (should retrieve from cache, not fetch)
    data = weo_importer.get_data()

    # Ensure it didn't re-fetch but used the cached data
    assert weo_importer._latest_version == (
        "April",
        2023,
    ), "Latest version not maintained correctly"
    assert (
        "April",
        2023,
    ) in weo_importer._data, "Latest version data should still be cached"

    # Ensure the returned data is a DataFrame and from cache
    assert isinstance(data, pd.DataFrame), "Returned data should be a DataFrame"
    assert data.equals(mock_weo_data), "Returned data should match the cached data"


def test_get_data_with_specific_version(
    mock_fetch_data_specific_version, mock_weo_data
):
    """Test get_data with a specific version passed"""

    # Initialize the WEO object
    weo_importer = WEO()

    # Call get_data for the specific version (October 2022)
    version = ("October", 2022)
    data = weo_importer.get_data(version=version)

    # Ensure the specific version data is fetched and cached
    assert version in weo_importer._data, (
        f"Data for version {version} not cached correctly"
    )

    # Ensure the returned data is a DataFrame
    assert isinstance(data, pd.DataFrame), "Returned data should be a DataFrame"

    # Ensure the latest version is not updated when fetching a specific version
    assert weo_importer._latest_version is None, (
        "Latest version should not be updated when fetching a specific version"
    )


# entity_code / imf_code (imf-reader 2.0 schema) tests


def test_entity_code_is_iso3_by_default(mock_weo_data):
    """entity_code should carry the ISO3 / G-prefixed code by default"""

    weo_importer = WEO()
    formatted = weo_importer._format_data(mock_weo_data)

    assert formatted[Fields.entity_code].tolist() == ["USA", "USA", "G001", "LIE"]


def test_imf_code_carries_legacy_numeric_code(mock_weo_data):
    """imf_code should carry the legacy numeric IMF area code, null where there is none"""

    weo_importer = WEO()
    formatted = weo_importer._format_data(mock_weo_data)

    assert formatted[Fields.imf_code].tolist()[:3] == [111, 111, 1]
    assert pd.isna(formatted[Fields.imf_code].tolist()[3])


def test_aggregates_keep_g_prefixed_entity_code(mock_weo_data):
    """Aggregate rows (e.g. World) should keep their G-prefixed entity_code"""

    weo_importer = WEO()
    formatted = weo_importer._format_data(mock_weo_data)

    aggregate_rows = formatted[formatted[Fields.entity_name] == "World"]
    assert (aggregate_rows[Fields.entity_code] == "G001").all()
    assert (aggregate_rows[Fields.imf_code] == 1).all()


def test_legacy_entity_codes_true_uses_numeric_entity_code(mock_weo_data):
    """legacy_entity_codes=True should populate entity_code from the legacy numeric code"""

    weo_importer = WEO(legacy_entity_codes=True)
    formatted = weo_importer._format_data(mock_weo_data)

    assert formatted[Fields.entity_code].tolist()[:3] == [111, 111, 1]
    # imf_code is still available and matches entity_code in this mode
    assert formatted[Fields.imf_code].tolist() == formatted[Fields.entity_code].tolist()


def test_legacy_entity_codes_true_is_null_for_entities_with_no_legacy_code(
    mock_weo_data,
):
    """legacy_entity_codes=True should leave entity_code null for entities with no
    legacy numeric code, and warn naming them, rather than raising or dropping rows"""

    weo_importer = WEO(legacy_entity_codes=True)
    with mock.patch("bblocks.importers.imf.weo.logger.warning") as log:
        formatted = weo_importer._format_data(mock_weo_data)

    liechtenstein_rows = formatted[
        formatted[Fields.entity_name] == "Liechtenstein, Principality of"
    ]
    assert liechtenstein_rows[Fields.entity_code].isna().all()

    # the other rows are unaffected
    assert not formatted[Fields.entity_code].iloc[:3].isna().any()

    log.assert_called_once()
    assert "Liechtenstein, Principality of" in log.call_args[0][0]


def test_legacy_entity_codes_true_without_legacy_column_fails_loudly(mock_weo_data):
    """legacy_entity_codes=True should raise clearly if imf-reader stops returning the legacy column"""

    data_without_legacy_code = mock_weo_data.drop(columns=["REF_AREA_IMF_CODE"])
    weo_importer = WEO(legacy_entity_codes=True)

    with pytest.raises(ValueError, match="legacy_entity_codes=True requires"):
        weo_importer._format_data(data_without_legacy_code)


def test_legacy_entity_codes_is_keyword_only():
    """legacy_entity_codes must be passed as a keyword argument"""

    with pytest.raises(TypeError):
        WEO(True)  # noqa


def test_get_data_with_cached_specific_version(mock_weo_data):
    """Test get_data when a specific version is already cached"""

    # Initialize the WEO object
    weo_importer = WEO()

    # Manually cache the specific version data (October 2022)
    version = ("October", 2022)
    weo_importer._data[version] = mock_weo_data

    # Call get_data for the specific version (should retrieve from cache)
    data = weo_importer.get_data(version=version)

    # Ensure the specific version data is still cached
    assert version in weo_importer._data, (
        f"Data for version {version} should still be cached"
    )

    # Ensure the returned data is a DataFrame and from cache
    assert isinstance(data, pd.DataFrame), "Returned data should be a DataFrame"
    assert data.equals(mock_weo_data), "Returned data should match the cached data"

    # Ensure the latest version is not updated when fetching a specific version
    assert weo_importer._latest_version is None, (
        "Latest version should not be updated when fetching a specific version"
    )
