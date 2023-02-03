"""Tests for the hdr module."""

import pytest
from bblocks.config import BBPaths
from bblocks.import_tools import hdr

from bs4 import BeautifulSoup
from unittest.mock import MagicMock

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


