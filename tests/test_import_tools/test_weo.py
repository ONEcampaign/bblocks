"""Tests for the WEO data import tool."""

import pytest
from datetime import datetime
import xml.etree.ElementTree as ET

from unittest.mock import Mock, patch

from bblocks.import_tools import weo


def test_get_files():
    """Tests the get_files method of the Parser class."""

    mock_zipfile = Mock()
    mock_zipfile.namelist.return_value = ["file.xml", "file.xsd"]
    mock_parse = Mock()
    mock_parse.return_value.getroot.return_value = ET.Element("root")
    ET.parse = mock_parse

    parser = weo.Parser(mock_zipfile)
    parser.get_files()

    # assert that the data_file and schema_file attributes are not None
    assert parser.data_file is not None
    assert parser.schema_file is not None

    # check the contents of the data_file and schema_file attributes
    assert parser.data_file.tag == "root"
    assert parser.schema_file.tag == "root"


def test_get_files_error():
    """Tests the get_files method of the Parser class."""

    mock_zipfile = Mock()
    mock_zipfile.namelist.return_value = ["file.xml", "file.xsd", "file2.xml"]

    mock_parse = Mock()
    mock_parse.return_value.getroot.return_value = ET.Element("root")
    ET.parse = mock_parse

    parser = weo.Parser(mock_zipfile)

    with pytest.raises(ValueError):
        parser.get_files()


def test_get_files_error2():
    """Tests the get_files method of the Parser class."""

    mock_zipfile = Mock()
    mock_zipfile.namelist.return_value = ["file.xml", "file.xsd"]
    mock_parse = Mock()
    mock_parse.return_value.getroot.return_value = None
    ET.parse = mock_parse

    parser = weo.Parser(mock_zipfile)

    with pytest.raises(ValueError):
        parser.get_files()


@patch("bblocks.import_tools.weo.datetime")
def test_gen_latest_version(mock_datetime):
    """Test gen_latest_version function."""

    # test when month is between April and October
    mock_now = datetime(2025, 6, 26, 10, 0, 0)
    mock_datetime.now.return_value = mock_now

    assert weo.gen_latest_version() == (2025, 1)

    # test when month is after November
    mock_now = datetime(2025, 11, 26, 10, 0, 0)
    mock_datetime.now.return_value = mock_now

    assert weo.gen_latest_version() == (2025, 2)

    # test when month is before April
    mock_now = datetime(2025, 3, 26, 10, 0, 0)
    mock_datetime.now.return_value = mock_now

    assert weo.gen_latest_version() == (2024, 2)


def test_roll_back_version():
    """Test roll_back_version function."""

    assert weo.roll_back_version((2025, 1)) == (2024, 2)
    assert weo.roll_back_version((2025, 2)) == (2025, 1)

    with pytest.raises(ValueError):
        weo.roll_back_version((2025, 3))


def test_smdx_query_url():
    """Test smdx_query_url function."""

    assert (
        weo._smdx_query_url((2025, 1))
        == "https://www.imf.org//en/Publications/WEO/weo-database/2025/April/download-entire-database"
    )
    assert (
        weo._smdx_query_url((2025, 2))
        == "https://www.imf.org//en/Publications/WEO/weo-database/2025/October/download-entire-database"
    )

    with pytest.raises(ValueError):
        weo._smdx_query_url((2025, 3))


def test_parse_sdmx_query_response():
    """Test parse_sdmx_query_response function."""

    mocked_content = '<html><a href="example.com">SDMX Data</a></html>'
    assert weo._parse_sdmx_query_response(mocked_content) == "example.com"

    assert weo._parse_sdmx_query_response("") is None


class TestWEO:
    """Test the WEO class."""

    def test_init_valid_version(self):
        version = (2022, 2)
        w = weo.WEO(version=version)
        assert w.version == version

    def test_init_latest_version(self):
        w = weo.WEO()
        assert w.version == "latest"

    def test_weo_class_invalid_version(self):
        invalid_version = "2022"
        with pytest.raises(ValueError):
            weo.WEO(version=invalid_version)
