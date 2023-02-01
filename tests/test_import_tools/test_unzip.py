"""tests for unzip.py"""
import pandas as pd
import pytest
from bblocks.import_tools.unzip import read_zipped_csv

VALID_ZIP_URL = "https://apimgmtstzgjpfeq2u763lag.blob.core.windows.net/content/MediaLibrary/bdds/SDG.zip"
VALID_URL = "https://apiportal.uis.unesco.org/bdds"
VALID_PATH = "SDG_LABEL.csv"


def test_read_zipped_csv_invalid_url():
    """test read_zipped_csv with invalid url"""
    with pytest.raises(ValueError) as exception_info:
        read_zipped_csv("", VALID_PATH)
        assert exception_info.match("invalid url")
