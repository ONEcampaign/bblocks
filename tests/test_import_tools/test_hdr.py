"""Tests for the hdr module."""

import pytest
from bblocks.config import BBPaths
from bblocks.import_tools import hdr
import requests
import pandas as pd
import numpy as np
import io

from bblocks.import_tools.common import get_response

from bs4 import BeautifulSoup
from unittest.mock import Mock, patch, MagicMock


def test_parse_html():
    """Tests the _parse_html function."""

    html = """
    <div class="section data-links-files">
        <p>some text</p>
    <div class="section data-links-files">
        <a href="data_link_1">Data Link 1</a>
        <a href="data_link_2">Data Link 2</a>
    </div>
    """

    soup = BeautifulSoup(html, "html.parser")
    assert hdr._parse_html(soup) == ("data_link_1", "data_link_2")


def test_read_data_csv():
    """"""
    response = Mock()
    response.headers = {"Content-Type": "text/csv"}
    response.content = b"col1,col2\n1,2\n3,4\n"

    df = hdr.read_data(response)
    expected = pd.DataFrame({"col1": [1, 3], "col2": [2, 4]})

    pd.testing.assert_frame_equal(df, expected)


def test_read_data_excel():
    """ """
    response = Mock()
    response.headers = {"Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
    response.content = b"col1,col2\n1,2\n3,4\n"

    with patch("pandas.read_excel") as mock_read_excel:
        mock_read_excel.return_value = pd.DataFrame({"col1": [1, 3], "col2": [2, 4]})
        df = hdr.read_data(response)

        mock_read_excel.assert_called_once_with(response.content, sheet_name="codebook", engine="openpyxl")

    expected = pd.DataFrame({"col1": [1, 3], "col2": [2, 4]})
    pd.testing.assert_frame_equal(df, expected)


def test_read_data_other_format():
    """ """

    response = Mock()
    response.headers = {"Content-Type": "other/format"}
    response.content = b"col1,col2\n1,2\n3,4\n"

    with pytest.raises(ValueError, match=r"Could not read data"):
        hdr.read_data(response)


def test_create_code_dict():
    """ """

    metadata_df = pd.DataFrame(
        {
            "Short name": ["code1", "code2", "code3", "code4"],
            "Full name": ["name1", "name2", np.nan, "name4"],
            "other": [1, 2, np.nan, 4],
        }
    )

    expected = {"code1": "name1", "code2": "name2", "code4": "name4"}
    assert hdr.create_code_dict(metadata_df) == expected


def test_format_data():
    """ """

    data_df = pd.DataFrame(
        {
            "country": ["country1", "country2", "country3"],
            "iso3": ["code1", "code2", "code3"],
            "hdicode": ["code4", "code5", "code6"],
            "region": ["region1", "region2", "region3"],
            "var_2020": [1, 2, 3],

        }
    )

    code_dict = {"var": "variable"}

    expected = pd.DataFrame(
        { "iso3": ["code1", "code2", "code3"],
            "country": ["country1", "country2", "country3"],

            "hdicode": ["code4", "code5", "code6"],
            "region": ["region1", "region2", "region3"],
          "variable": ["var", "var", "var"],
            "value": [1, 2, 3],
            "year": [2020, 2020, 2020],
            "variable_name": ["variable", "variable", "variable"],
        }
    )

    pd.testing.assert_frame_equal(hdr.format_data(data_df, code_dict), expected, check_dtype=False)


def test_available_indicators():
    """ """

    assert len(hdr.available_indicators('all')) > len(hdr.available_indicators('hdi'))
    with pytest.raises(ValueError, match="Composite index not found"):
        hdr.available_indicators('invalid')














