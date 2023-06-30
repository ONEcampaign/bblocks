"""Tests for the world_bank_projects module."""

import pytest
import pandas as pd
import numpy as np
import requests
from unittest.mock import Mock, patch, MagicMock

from bblocks.import_tools import world_bank_projects


class TestQueryAPI:
    """Test QueryAPI class."""

    def test_init(self):
        """Test initialization of QueryAPI object."""

        # test that error is raised if end_date is before start_date
        with pytest.raises(ValueError):
            world_bank_projects.QueryAPI(start_date='2020-01-01', end_date='2019-01-01')

        # test that error is raised if max_rows_per_response is greater than 1000
        with pytest.raises(ValueError):
            world_bank_projects.QueryAPI(max_rows_per_response=1001)

        # test that start_date is dropped if end_date is None
        assert 'strdate' not in world_bank_projects.QueryAPI(end_date='2020-01-01',
                                                             start_date=None)._params

        # test that end_date is dropped if start_date is None
        assert 'enddate' not in world_bank_projects.QueryAPI(start_date='2020-01-01',
                                                             end_date=None)._params

    def test_request(self):
        """ """
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'projects': {'P1234': {'name': 'Test Project'}}}

        with patch("requests.get", return_value=mock_response) as mock_get:
            assert world_bank_projects.QueryAPI()._request() == {'P1234': {'name': 'Test Project'}}

    def test_request_error(self):
        """Test that error is raised if request fails."""

        with patch("requests.get") as mock_get:
            mock_get.return_value.raise_for_status.side_effect = (
                requests.exceptions.HTTPError
            )
            mock_get.return_value.status_code = 404
            mock_get.json.return_value = {'projects': {'P1234': {'name': 'Test Project'}}}

            with pytest.raises(Exception):
                world_bank_projects.QueryAPI()._request()

    def test_request_data_no_data(self):
        """Test request_data method."""

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'projects': {}}  # test that empty response is handled

        with pytest.raises(world_bank_projects.EmptyDataException):
            with patch("requests.get", return_value=mock_response) as mock_get:
                obj = world_bank_projects.QueryAPI()
                obj.request_data()

    def test_request_data(self):
        """Test request_data method."""

        # Mocking the requests.get function
        mocked_get = MagicMock(side_effect=[
            Mock(json=MagicMock(return_value={'projects': {'P1': {'name': 'Test Project 1'},
                                                           'P2': {'name': 'Test Project 2'}
                                                           }
                                              })),
            Mock(json=MagicMock(return_value={'projects':{'P3': {'name': 'Test Project 3'}}})),
            Mock(json=MagicMock(return_value={'projects': {}}))
        ])

        with patch("bblocks.import_tools.world_bank_projects.requests.get", mocked_get):
            obj = world_bank_projects.QueryAPI()
            obj.request_data()

            assert obj.response_data == {'P1': {'name': 'Test Project 1'},
                                         'P2': {'name': 'Test Project 2'},
                                         'P3': {'name': 'Test Project 3'}
                                         }
