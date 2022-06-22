"""tests for unzip.py"""

import pytest
from package.import_tools.unzip import read_zipped_csv

VALID_URL = 'https://apimgmtstzgjpfeq2u763lag.blob.core.windows.net/content/MediaLibrary/bdds/SDG.zip'
VALID_PATH = 'SDG_LABEL.csv'

def test_valid_url_and_path():
    """ """
    assert len(read_zipped_csv(VALID_URL, VALID_PATH))>0

def test_invalid_url():
    """ """
    with pytest.raises(ValueError) as exception_info:
        read_zipped_csv("", VALID_PATH)
        assert exception_info.match('invalid url')

def test_invalid_path():
    """ """
    with pytest.raises(ValueError) as exception_info:
        read_zipped_csv(VALID_URL, "")
        assert exception_info.match('invalid file path')


