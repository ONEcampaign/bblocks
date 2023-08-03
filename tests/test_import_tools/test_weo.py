"""Tests for the WEO data import tool."""

import pytest
from datetime import datetime
import xml.etree.ElementTree as ET
from unittest.mock import Mock, patch
import pandas as pd

from bblocks.import_tools import imf_weo


def test_smdx_query_url():
    """Test smdx_query_url function."""

    # test April version
    assert (
        imf_weo._smdx_query_url((2023, 1))
        == f"{imf_weo.BASE_URL}/en/Publications/WEO/weo-database/2023/April/download-entire-database"
    )

    # test October version
    assert (
        imf_weo._smdx_query_url((2023, 2))
        == f"{imf_weo.BASE_URL}/en/Publications/WEO/weo-database/2023/October/download-entire-database"
    )

    # test invalid version
    with pytest.raises(ValueError):
        imf_weo._smdx_query_url((2023, 3))


def test_get_files():
    """Tests the get_files method of the Parser class."""

    mock_zipfile = Mock()
    mock_zipfile.namelist.return_value = ["file.xml", "file.xsd"]
    mock_parse = Mock()
    mock_parse.return_value.getroot.return_value = ET.Element("root")
    ET.parse = mock_parse

    parser = imf_weo.Parser(mock_zipfile)
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

    parser = imf_weo.Parser(mock_zipfile)

    with pytest.raises(ValueError):
        parser.get_files()


def test_get_files_error2():
    """Tests the get_files method of the Parser class."""

    mock_zipfile = Mock()
    mock_zipfile.namelist.return_value = ["file.xml", "file.xsd"]
    mock_parse = Mock()
    mock_parse.return_value.getroot.return_value = None
    ET.parse = mock_parse

    parser = imf_weo.Parser(mock_zipfile)

    with pytest.raises(ValueError):
        parser.get_files()


def test_extract_data():
    """ """

    sample_xml = b'<ns0:StructureSpecificData xmlns:ns0="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message" xmlns:ns1="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common" xmlns:ns2="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/structurespecific" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">\n\t<ns0:Header>\n\t\t<ns0:ID>WEO_PUB</ns0:ID>\n\t\t<ns0:Test>false</ns0:Test>\n\t\t<ns0:Prepared>2023-04-07T15:10:57</ns0:Prepared>\n\t\t<ns0:Sender id="IMF" />\n\t\t<ns0:Structure structureID="IMF_WEO_PUB_1_0" namespace="urn:sdmx:org.sdmx.infomodel.datastructure.DataStructure=IMF:WEO_PUB(1.0):ObsLevelDim:TIME_PERIOD" dimensionAtObservation="TIME_PERIOD">\n\t\t\t<ns1:Structure>\n\t\t\t\t<Ref agencyID="IMF" id="WEO_PUB" version="1.0" />\n\t\t\t</ns1:Structure>\n\t\t</ns0:Structure>\n\t</ns0:Header>\n\t<ns0:DataSet ns2:dataScope="DataStructure" xsi:type="ns1:DataSetType" ns2:structureRef="IMF_WEO_PUB_1_0">\n\t\t<Series UNIT="B" CONCEPT="NGDP_D" REF_AREA="111" FREQ="A" LASTACTUALDATE="2022" NOTES="See notes for:   Gross domestic product, constant prices (National currency)  Gross domestic product, current prices (National currency).">\n\t\t\t<Obs TIME_PERIOD="1980" OBS_VALUE="42.246" />\n\t\t\t<Obs TIME_PERIOD="1981" OBS_VALUE="46.243" />\n\t\t\t<Obs TIME_PERIOD="2028" OBS_VALUE="145.376" />\n\t\t</Series>\n\t\t<Series UNIT="B" CONCEPT="NGDP_D" REF_AREA="112" FREQ="A" LASTACTUALDATE="2022" NOTES="See notes for:   Gross domestic product, constant prices (National currency)  Gross domestic product, current prices (National currency).">\n\t\t\t<Obs TIME_PERIOD="1980" OBS_VALUE="25.782" />\n\n\t\t\t<Obs TIME_PERIOD="2028" OBS_VALUE="130.54" />\n\t\t</Series>\n\t\t<Series UNIT="B" CONCEPT="NGDP_D" REF_AREA="135" FREQ="A" LASTACTUALDATE="2020" NOTES="See notes for:   Gross domestic product, constant prices (National currency)  Gross domestic product, current prices (National currency).">\n\t\t\t<Obs TIME_PERIOD="1980" OBS_VALUE="n/a" />\n\t\t\t<Obs TIME_PERIOD="1981" OBS_VALUE="n/a" />\n\t\t\t<Obs TIME_PERIOD="1982" OBS_VALUE="n/a" />\n\t\t</Series>\n\n\t</ns0:DataSet>\n</ns0:StructureSpecificData>'

    tree = ET.fromstring(sample_xml)

    # Create a mock for the data_file attribute
    mock_zipfile = Mock()
    instance = imf_weo.Parser(folder=mock_zipfile)  # Create an instance of your class

    # Set the data_file attribute to the XML tree
    instance.data_file = tree

    # Call the _extract_data method
    instance._extract_data()

    # Assert that the DataFrame is created and contains the expected data
    assert isinstance(instance.data, pd.DataFrame)
    assert len(instance.data) > 0
    assert instance.data.columns.tolist() == [
        "UNIT",
        "CONCEPT",
        "REF_AREA",
        "FREQ",
        "LASTACTUALDATE",
        "NOTES",
        "TIME_PERIOD",
        "OBS_VALUE",
    ]


@patch("bblocks.import_tools.imf_weo.datetime")
def test_gen_latest_version(mock_datetime):
    """Test gen_latest_version function."""

    # test when month is between April and October
    mock_now = datetime(2025, 6, 26, 10, 0, 0)
    mock_datetime.now.return_value = mock_now

    assert imf_weo.gen_latest_version() == (2025, 1)

    # test when month is after November
    mock_now = datetime(2025, 11, 26, 10, 0, 0)
    mock_datetime.now.return_value = mock_now

    assert imf_weo.gen_latest_version() == (2025, 2)

    # test when month is before April
    mock_now = datetime(2025, 3, 26, 10, 0, 0)
    mock_datetime.now.return_value = mock_now

    assert imf_weo.gen_latest_version() == (2024, 2)


def test_roll_back_version():
    """Test roll_back_version function."""

    assert imf_weo.roll_back_version((2025, 1)) == (2024, 2)
    assert imf_weo.roll_back_version((2025, 2)) == (2025, 1)

    with pytest.raises(ValueError):
        imf_weo.roll_back_version((2025, 3))


def test_parse_sdmx_query_response():
    """Test parse_sdmx_query_response function."""

    mocked_content = '<html><a href="example.com">SDMX Data</a></html>'
    assert imf_weo._parse_sdmx_query_response(mocked_content) == "example.com"

    assert imf_weo._parse_sdmx_query_response("") is None


class TestWEO:
    """Test the WEO class."""

    def test_init_valid_version(self):
        version = (2022, 2)
        w = imf_weo.WEO(version=version)
        assert w.version == version

    def test_init_latest_version(self):
        w = imf_weo.WEO()
        assert w.version == "latest"

    def test_weo_class_invalid_version(self):
        invalid_version = "2022"
        with pytest.raises(ValueError):
            imf_weo.WEO(version=invalid_version)
