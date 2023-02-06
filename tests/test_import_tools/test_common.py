"""Tests for the common module."""

import pytest
import requests
from unittest.mock import Mock, patch

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
