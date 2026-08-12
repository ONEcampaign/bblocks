import io
from zipfile import ZipFile
from unittest import mock

import pandas as pd
import pytest

from bblocks.data_importers.unaids import unaids
from bblocks.data_importers.config import DataExtractionError, Fields
from bblocks.data_importers.protocols import DataImporter


def test_protocol():
    """Test that importer class implements the DataImporter protocol"""

    importer_obj = unaids.UNAIDS()

    assert isinstance(
        importer_obj, DataImporter
    ), "UNAIDS does not implement DataImporter protocol"
    assert hasattr(importer_obj, "get_data"), "UNAIDS does not have get_data method"
    assert hasattr(
        importer_obj, "clear_cache"
    ), "UNAIDS does not have clear_cache method"


@pytest.fixture
def sample_zip_response():
    csv_content = "a,b\n1,2\n3,4\n"
    buffer = io.BytesIO()
    with ZipFile(buffer, "w") as zf:
        zf.writestr("data.csv", csv_content)
    response = mock.Mock()
    response.content = buffer.getvalue()
    expected_df = pd.DataFrame({"a": [1, 3], "b": [2, 4]})
    return response, expected_df


def test_get_response_success():
    mock_response = mock.Mock()
    mock_response.raise_for_status = mock.Mock()
    with mock.patch(
        "bblocks.data_importers.unaids.unaids.requests.get",
        return_value=mock_response,
    ) as mock_get:
        resp = unaids.get_response("http://example.com", verify=True)

    mock_get.assert_called_once_with("http://example.com", verify=True)
    mock_response.raise_for_status.assert_called_once()
    assert resp is mock_response


def test_get_response_error():
    with mock.patch(
        "bblocks.data_importers.unaids.unaids.requests.get",
        side_effect=Exception("error"),
    ):
        with pytest.raises(Exception, match="error"):
            unaids.get_response("http://example.com")


def test_read_csv_from_zip_response(sample_zip_response):
    response, expected_df = sample_zip_response
    df = unaids.read_csv_from_zip_response(response)
    pd.testing.assert_frame_equal(df, expected_df)


def test_read_csv_from_zip_response_multiple_files(sample_zip_response):
    _, _ = sample_zip_response
    buffer = io.BytesIO()
    with ZipFile(buffer, "w") as zf:
        zf.writestr("a.csv", "x")
        zf.writestr("b.csv", "y")
    response = mock.Mock(content=buffer.getvalue())
    with pytest.raises(DataExtractionError, match="Multiple files"):
        unaids.read_csv_from_zip_response(response)


def test_read_csv_from_zip_response_no_files():
    buffer = io.BytesIO()
    with ZipFile(buffer, "w"):
        pass
    response = mock.Mock(content=buffer.getvalue())
    with pytest.raises(DataExtractionError, match="No files"):
        unaids.read_csv_from_zip_response(response)


def test_format_data(sample_zip_response):
    df = pd.DataFrame(
        {
            "Indicator": ["ind"],
            "Unit": ["unit"],
            "Subgroup": ["sub"],
            "Area": ["A"],
            "Area ID": ["AAA"],
            "Time Period": [2020],
            "Source": ["src"],
            "Data value": [1.0],
            "Formatted": ["1"],
            "Footnote": ["note"],
        }
    )
    formatted = unaids.format_data(df)
    expected_columns = [
        Fields.indicator_name,
        Fields.unit,
        "subgroup",
        Fields.entity_name,
        Fields.entity_code,
        Fields.year,
        Fields.source,
        Fields.value,
        "value_formatted",
        Fields.footnote,
    ]
    assert list(formatted.columns) == expected_columns
    assert isinstance(formatted.dtypes[Fields.year], pd.ArrowDtype)


class TestUNAIDS:
    def test_init(self):
        with mock.patch("bblocks.data_importers.unaids.unaids.logger.warning"):
            ua = unaids.UNAIDS()
        assert ua.verify_ssl is False
        assert ua._data == {
            "Estimates": None,
            "Laws and Policies": None,
            "Key Populations": None,
            "GAM": None,
        }

    def test_load_data(self, sample_zip_response):
        response, df = sample_zip_response
        with (
            mock.patch(
                "bblocks.data_importers.unaids.unaids.get_response",
                return_value=response,
            ) as mock_get,
            mock.patch(
                "bblocks.data_importers.unaids.unaids.read_csv_from_zip_response",
                return_value=df,
            ) as mock_read,
            mock.patch(
                "bblocks.data_importers.unaids.unaids.format_data",
                return_value=df,
            ) as mock_format,
            mock.patch(
                "bblocks.data_importers.unaids.unaids.DataFrameValidator"
            ) as mock_validator,
        ):
            validator_instance = mock_validator.return_value
            ua = unaids.UNAIDS()
            ua._load_data("Estimates")

        mock_get.assert_called_once_with(unaids.URLS["Estimates"], verify=False)
        mock_read.assert_called_once_with(response)
        mock_format.assert_called_once_with(df)
        validator_instance.validate.assert_called_once()
        pd.testing.assert_frame_equal(ua._data["Estimates"], df)

    def test_get_data_loads_when_not_cached(self, sample_zip_response):
        response, df = sample_zip_response
        ua = unaids.UNAIDS()
        ua._data["Estimates"] = None
        with (
            mock.patch.object(
                ua,
                "_load_data",
                side_effect=lambda d: ua._data.update({d: df}),
            ) as mock_load,
        ):
            data = ua.get_data("Estimates")
        mock_load.assert_called_once_with("Estimates")
        pd.testing.assert_frame_equal(data, df)

    def test_get_data_returns_cached(self):
        ua = unaids.UNAIDS()
        df = pd.DataFrame({"a": [1]})
        ua._data["Estimates"] = df
        with mock.patch.object(ua, "_load_data") as mock_load:
            result = ua.get_data("Estimates")
        mock_load.assert_not_called()
        assert result is df

    def test_get_data_invalid_dataset(self):
        ua = unaids.UNAIDS()
        with pytest.raises(ValueError, match="Invalid dataset"):
            ua.get_data("wrong")

    def test_clear_cache(self):
        ua = unaids.UNAIDS()
        ua._data["Estimates"] = pd.DataFrame({"a": [1]})
        with mock.patch("bblocks.data_importers.unaids.unaids.logger.info") as log:
            ua.clear_cache()
        assert ua._data == {k: None for k in ua._data}
        log.assert_called_once_with("Cache cleared.")
