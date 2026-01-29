"""Tests for BACI module."""

from unittest import mock

import pandas as pd
import pytest
import requests
from bs4 import BeautifulSoup

from bblocks.data_importers.baci import baci
from bblocks.data_importers.config import Fields, DataExtractionError
from bblocks.data_importers.protocols import DataImporter


@pytest.fixture(autouse=True)
def clear_baci_cache():
    baci._DATA_CACHE.clear()
    yield
    baci._DATA_CACHE.clear()


def test_protocol():
    """Test that importer class implements the DataImporter protocol"""

    importer_obj = baci.BACI()

    assert isinstance(
        importer_obj, DataImporter
    ), "BACI does not implement DataImporter protocol"
    assert hasattr(importer_obj, "get_data"), "BACI does not have get_data method"
    assert hasattr(importer_obj, "clear_cache"), "BACI does not have clear_cache method"


def test_get_soup_success():
    html = b"<html><head><title>BACI</title></head><body></body></html>"
    mock_response = mock.Mock()
    mock_response.content = html
    mock_response.raise_for_status = mock.Mock()

    with mock.patch("requests.get", return_value=mock_response) as mock_get:
        soup = baci._get_soup()

    mock_get.assert_called_once_with(baci.URL)
    assert soup.find("title").text == "BACI"


def test_get_soup_failure():
    with mock.patch(
        "requests.get", side_effect=requests.RequestException("boom")
    ):
        with pytest.raises(DataExtractionError, match="Failed to fetch BACI page"):
            baci._get_soup()


def test_parse_data_links_success():
    html = """
    <html>
      <section id="download-links">
        <a href="link1.zip">HS22</a>
        <a href="link2.zip">HS12</a>
        <a href="other">NOTHS</a>
      </section>
    </html>
    """
    soup = BeautifulSoup(html, "html.parser")

    links = baci._parse_data_links(soup)

    assert links == {"HS22": "link1.zip", "HS12": "link2.zip"}


def test_parse_data_links_missing_section():
    soup = BeautifulSoup("<html></html>", "html.parser")

    with pytest.raises(DataExtractionError, match="Failed to parse BACI data links"):
        baci._parse_data_links(soup)


def test_parse_data_links_empty_results():
    html = """
    <html>
      <section id="download-links">
        <a href="link1.zip">OTHER</a>
      </section>
    </html>
    """
    soup = BeautifulSoup(html, "html.parser")

    with pytest.raises(DataExtractionError, match="No BACI data links found"):
        baci._parse_data_links(soup)


def test_extract_data_links_calls_helpers():
    with (
        mock.patch("bblocks.data_importers.baci.baci._get_soup", return_value="soup") as mock_soup,
        mock.patch(
            "bblocks.data_importers.baci.baci._parse_data_links",
            return_value={"HS22": "link"},
        ) as mock_parse,
    ):
        links = baci.extract_data_links()

    mock_soup.assert_called_once()
    mock_parse.assert_called_once_with("soup")
    assert links == {"HS22": "link"}


def test_validate_hs_version_cleans_and_validates():
    with mock.patch(
        "bblocks.data_importers.baci.baci.extract_data_links",
        return_value={"HS22": "link"},
    ):
        assert baci._validate_hs_version(" hs22 ") == "HS22"


def test_validate_hs_version_rejects_invalid():
    with mock.patch(
        "bblocks.data_importers.baci.baci.extract_data_links",
        return_value={"HS22": "link"},
    ):
        with pytest.raises(ValueError, match="HS version"):
            baci._validate_hs_version("HS99")


def test_extract_data_success():
    data = pd.DataFrame(
        {
            Fields.year: pd.Series([2020], dtype="int64"),
            Fields.exporter_code: [1],
            Fields.importer_code: [2],
            Fields.product_code: [100],
            Fields.value: [1.0],
            Fields.quantity: [2.0],
        }
    )
    product_codes = pd.DataFrame(
        {
            Fields.product_code: [100],
            Fields.product_description: ["Widgets"],
        }
    )
    country_codes = pd.DataFrame(
        {
            Fields.country_code: [1, 2],
            Fields.country_name: ["A", "B"],
            Fields.iso3_code: ["AAA", "BBB"],
        }
    )

    class DummyManager:
        def __init__(self, hs_version: str, url: str):
            self.hs_version = hs_version
            self.url = url
            self.data = data
            self.product_codes = product_codes
            self.country_codes = country_codes
            self.metadata = {"Title": "BACI"}

        def extract(self):
            return None

    mock_validator = mock.Mock()

    with (
        mock.patch(
            "bblocks.data_importers.baci.baci.extract_data_links",
            return_value={"HS22": "link"},
        ),
        mock.patch(
            "bblocks.data_importers.baci.baci.BaciDataManager", DummyManager
        ) as mock_manager,
        mock.patch(
            "bblocks.data_importers.baci.baci.DataFrameValidator",
            return_value=mock_validator,
        ),
    ):
        manager = baci.extract_data.__wrapped__("HS22")

    assert isinstance(manager, DummyManager)
    assert manager.hs_version == "HS22"
    assert manager.url == "link"
    assert mock_validator.validate.call_count == 3
    assert mock_manager is DummyManager


def test_extract_data_raises_without_metadata():
    data = pd.DataFrame(
        {
            Fields.year: [2020],
            Fields.exporter_code: [1],
            Fields.importer_code: [2],
            Fields.product_code: [100],
            Fields.value: [1.0],
            Fields.quantity: [2.0],
        }
    )

    class DummyManager:
        def __init__(self, hs_version: str, url: str):
            self.data = data
            self.product_codes = pd.DataFrame(
                {Fields.product_code: [100], Fields.product_description: ["Widgets"]}
            )
            self.country_codes = pd.DataFrame(
                {
                    Fields.country_code: [1],
                    Fields.country_name: ["A"],
                    Fields.iso3_code: ["AAA"],
                }
            )
            self.metadata = None

        def extract(self):
            return None

    with (
        mock.patch(
            "bblocks.data_importers.baci.baci.extract_data_links",
            return_value={"HS22": "link"},
        ),
        mock.patch(
            "bblocks.data_importers.baci.baci.BaciDataManager", DummyManager
        ),
        mock.patch("bblocks.data_importers.baci.baci.DataFrameValidator"),
    ):
        with pytest.raises(DataExtractionError, match="No metadata found"):
            baci.extract_data.__wrapped__("HS22")


def test_add_product_labels():
    data = pd.DataFrame({Fields.product_code: [100], "x": [1]})
    product_codes = pd.DataFrame(
        {Fields.product_code: [100], Fields.product_description: ["Widgets"]}
    )
    manager = mock.Mock(product_codes=product_codes)

    labeled = baci._add_product_labels(data, manager)

    assert Fields.product_description in labeled.columns
    assert labeled[Fields.product_description].iloc[0] == "Widgets"


def test_add_country_labels():
    data = pd.DataFrame(
        {Fields.exporter_code: [1], Fields.importer_code: [2], "x": [1]}
    )
    country_codes = pd.DataFrame(
        {
            Fields.country_code: [1, 2],
            Fields.country_name: ["Alpha", "Beta"],
            Fields.iso3_code: ["AAA", "BBB"],
        }
    )
    manager = mock.Mock(country_codes=country_codes)

    labeled = baci._add_country_labels(data, manager)

    assert Fields.exporter_name in labeled.columns
    assert Fields.importer_name in labeled.columns
    assert Fields.exporter_iso3_code in labeled.columns
    assert Fields.importer_iso3_code in labeled.columns


def test_baci_available_hs_versions_caches():
    importer = baci.BACI()

    with mock.patch(
        "bblocks.data_importers.baci.baci.extract_data_links",
        return_value={"HS22": "link"},
    ) as mock_links:
        versions = importer.available_hs_versions()

    assert versions == ["HS22"]
    assert importer._hs_versions == {"HS22": "link"}
    mock_links.assert_called_once()

    with mock.patch(
        "bblocks.data_importers.baci.baci.extract_data_links"
    ) as mock_links_again:
        versions_again = importer.available_hs_versions()

    assert versions_again == ["HS22"]
    mock_links_again.assert_not_called()


def test_baci_get_data_includes_labels():
    importer = baci.BACI()

    data = pd.DataFrame(
        {
            Fields.product_code: [100],
            Fields.exporter_code: [1],
            Fields.importer_code: [2],
            Fields.value: [10.0],
            Fields.quantity: [1.0],
        }
    )
    product_codes = pd.DataFrame(
        {Fields.product_code: [100], Fields.product_description: ["Widgets"]}
    )
    country_codes = pd.DataFrame(
        {
            Fields.country_code: [1, 2],
            Fields.country_name: ["Alpha", "Beta"],
            Fields.iso3_code: ["AAA", "BBB"],
        }
    )

    importer._data["HS22"] = mock.Mock(
        data=data, product_codes=product_codes, country_codes=country_codes
    )

    with (
        mock.patch("bblocks.data_importers.baci.baci._validate_hs_version", return_value="HS22"),
        mock.patch.object(importer, "_load_data"),
    ):
        labeled = importer.get_data(
            hs_version="HS22",
            include_product_labels=True,
            include_country_labels=True,
        )

    assert Fields.product_description in labeled.columns
    assert Fields.exporter_name in labeled.columns
    assert Fields.importer_name in labeled.columns


def test_baci_get_country_codes_product_codes_metadata():
    importer = baci.BACI()
    manager = mock.Mock(
        country_codes=pd.DataFrame({Fields.country_code: [1]}),
        product_codes=pd.DataFrame({Fields.product_code: [100]}),
        metadata={"Title": "BACI"},
    )
    importer._data["HS22"] = manager

    with (
        mock.patch("bblocks.data_importers.baci.baci._validate_hs_version", return_value="HS22"),
        mock.patch.object(importer, "_load_data"),
    ):
        assert importer.get_country_codes("HS22").equals(manager.country_codes)
        assert importer.get_product_codes("HS22").equals(manager.product_codes)
        assert importer.get_metadata("HS22") == manager.metadata


def test_baci_clear_cache():
    importer = baci.BACI()
    importer._hs_versions = {"HS22": "link"}
    importer._data = {"HS22": mock.Mock()}

    with mock.patch.object(baci._DATA_CACHE, "clear") as mock_clear:
        importer.clear_cache()

    mock_clear.assert_called_once()
    assert importer._hs_versions is None
    assert importer._data == {}
