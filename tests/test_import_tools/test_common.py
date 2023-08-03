"""Tests for the common module."""

import pytest
import requests
from unittest.mock import patch
from zipfile import ZipFile
import tempfile
import os
import io

from bblocks.import_tools import common


def test_get_response():
    with patch("requests.get") as mock_get:
        mock_get.return_value.raise_for_status.return_value = None
        mock_get.return_value.status_code = 200

        url = "https://www.example.com"
        response = common.get_response(url)

        mock_get.assert_called_once_with(url)
        assert response.status_code == 200


def test_get_response_status_not_200():
    """test get_response function when the status code is not 200"""

    with patch("requests.get") as mock_get:
        mock_get.return_value.raise_for_status.side_effect = (
            requests.exceptions.HTTPError
        )
        mock_get.return_value.status_code = 404

        url = "https://www.example.com"
        with pytest.raises(requests.exceptions.HTTPError):
            common.get_response(url)

        mock_get.assert_called_once_with(url)


def test_get_response_connection_error():
    """test get_response function when there is a connection error"""

    with patch("requests.get") as mock_get:
        mock_get.return_value.raise_for_status.side_effect = (
            requests.exceptions.ConnectionError
        )

        url = "https://www.example.com"
        with pytest.raises(requests.exceptions.ConnectionError):
            common.get_response(url)

        mock_get.assert_called_once_with(url)


def test_unzip():
    """Test unzip with a valid file-like object"""

    # create a mock file-like object
    mock_zip = io.BytesIO()
    with ZipFile(mock_zip, "w") as zf:
        zf.writestr("test.txt", "This is a test file")

    assert isinstance(common.unzip(mock_zip), ZipFile)


def test_unzip_exceptions():
    """Test unzip exceptions"""

    # test file not found
    with pytest.raises(FileNotFoundError, match="Could not find file"):
        common.unzip("non_existent_file.zip")

    # test bad zip file
    mock_zip = io.BytesIO(b"invalid zip data")
    with pytest.raises(ValueError, match="The file could not be unzipped"):
        common.unzip(mock_zip)


def test_unzip_valid_path():
    """Test unzip when the path is valid"""

    # create a temporary zipfile
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as temp:
        with ZipFile(temp.name, "w") as zf:
            zf.writestr("test.txt", "This is a test file")
        temp.close()
        # pass the path of the zipfile to the unzip function
        assert isinstance(common.unzip(temp.name), ZipFile)
    # remove the tempfile after the test
    os.remove(temp.name)
