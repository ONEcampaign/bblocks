"""Tests for the world_bank_projects module."""

import pytest
import requests
from unittest.mock import Mock, patch, MagicMock

from bblocks.import_tools import world_bank_projects


class TestQueryAPI:
    """Test QueryAPI class."""

    def test_init(self):
        """Test initialization of QueryAPI object."""

        # test that error is raised if end_date is before start_date
        with pytest.raises(ValueError):
            world_bank_projects.QueryAPI(start_date="2020-01-01", end_date="2019-01-01")

        # test that error is raised if max_rows_per_response is greater than 1000
        with pytest.raises(ValueError):
            world_bank_projects.QueryAPI(max_rows_per_response=1001)

        # test that start_date is dropped if end_date is None
        assert (
            "strdate"
            not in world_bank_projects.QueryAPI(
                end_date="2020-01-01", start_date=None
            )._params
        )

        # test that end_date is dropped if start_date is None
        assert (
            "enddate"
            not in world_bank_projects.QueryAPI(
                start_date="2020-01-01", end_date=None
            )._params
        )

    def test_request(self):
        """ """
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "projects": {"P1234": {"name": "Test Project"}}
        }

        with patch("requests.get", return_value=mock_response) as mock_get:
            assert world_bank_projects.QueryAPI()._request() == {
                "P1234": {"name": "Test Project"}
            }

    def test_request_error(self):
        """Test that error is raised if request fails."""

        with patch("requests.get") as mock_get:
            mock_get.return_value.raise_for_status.side_effect = (
                requests.exceptions.HTTPError
            )
            mock_get.return_value.status_code = 404
            mock_get.json.return_value = {
                "projects": {"P1234": {"name": "Test Project"}}
            }

            with pytest.raises(Exception):
                world_bank_projects.QueryAPI()._request()

    def test_request_data_no_data(self):
        """Test request_data method."""

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "projects": {}
        }  # test that empty response is handled

        with pytest.raises(world_bank_projects.EmptyDataException):
            with patch("requests.get", return_value=mock_response) as mock_get:
                obj = world_bank_projects.QueryAPI()
                obj.request_data()

    def test_request_data(self):
        """Test request_data method."""

        # Mocking the requests.get function
        mocked_get = MagicMock(
            side_effect=[
                Mock(
                    json=MagicMock(
                        return_value={
                            "projects": {
                                "P1": {"name": "Test Project 1"},
                                "P2": {"name": "Test Project 2"},
                            }
                        }
                    )
                ),
                Mock(
                    json=MagicMock(
                        return_value={"projects": {"P3": {"name": "Test Project 3"}}}
                    )
                ),
                Mock(json=MagicMock(return_value={"projects": {}})),
            ]
        )

        with patch("bblocks.import_tools.world_bank_projects.requests.get", mocked_get):
            obj = world_bank_projects.QueryAPI()
            obj.request_data()

            assert obj.response_data == {
                "P1": {"name": "Test Project 1"},
                "P2": {"name": "Test Project 2"},
                "P3": {"name": "Test Project 3"},
            }


def test_clean_theme():
    """Test clean_theme function."""

    test_data_dict = {
        "id": "P1234",
        "theme_list": [
            {
                "name": "Environment and Natural Resource Management",
                "code": "80",
                "seqno": "14",
                "percent": "34",
                "theme2": [
                    {
                        "name": "Energy",
                        "code": "86",
                        "seqno": "18",
                        "percent": "13",
                        "theme3": [
                            {
                                "name": "Energy Efficiency",
                                "code": "861",
                                "seqno": "34",
                                "percent": "13",
                            },
                            {
                                "name": "Energy Policies & Reform",
                                "code": "862",
                                "seqno": "35",
                                "percent": "13",
                            },
                        ],
                    },
                    {
                        "name": "Environmental policies and institutions",
                        "code": "84",
                        "seqno": "17",
                        "percent": "13",
                    },
                    {
                        "name": "Environmental Health and Pollution Management",
                        "code": "82",
                        "seqno": "16",
                        "percent": "13",
                        "theme3": [
                            {
                                "name": "Air quality management",
                                "code": "821",
                                "seqno": "33",
                                "percent": "13",
                            }
                        ],
                    },
                    {
                        "name": "Climate change",
                        "code": "81",
                        "seqno": "15",
                        "percent": "34",
                        "theme3": [
                            {
                                "name": "Adaptation",
                                "code": "812",
                                "seqno": "32",
                                "percent": "8",
                            },
                            {
                                "name": "Mitigation",
                                "code": "811",
                                "seqno": "31",
                                "percent": "26",
                            },
                        ],
                    },
                ],
            }
        ],
    }

    formatted = [
        {
            "project ID": "P1234",
            "theme1": "Environment and Natural Resource Management",
            "percent": 34,
        },
        {
            "project ID": "P1234",
            "theme1": "Environment and Natural Resource Management",
            "theme2": "Energy",
            "percent": 13,
        },
        {
            "project ID": "P1234",
            "theme1": "Environment and Natural Resource Management",
            "theme2": "Energy",
            "theme3": "Energy Efficiency",
            "percent": 13,
        },
        {
            "project ID": "P1234",
            "theme1": "Environment and Natural Resource Management",
            "theme2": "Energy",
            "theme3": "Energy Policies & Reform",
            "percent": 13,
        },
        {
            "project ID": "P1234",
            "theme1": "Environment and Natural Resource Management",
            "theme2": "Environmental policies and institutions",
            "percent": 13,
        },
        {
            "project ID": "P1234",
            "theme1": "Environment and Natural Resource Management",
            "theme2": "Environmental Health and Pollution Management",
            "percent": 13,
        },
        {
            "project ID": "P1234",
            "theme1": "Environment and Natural Resource Management",
            "theme2": "Environmental Health and Pollution Management",
            "theme3": "Air quality management",
            "percent": 13,
        },
        {
            "project ID": "P1234",
            "theme1": "Environment and Natural Resource Management",
            "theme2": "Climate change",
            "percent": 34,
        },
        {
            "project ID": "P1234",
            "theme1": "Environment and Natural Resource Management",
            "theme2": "Climate change",
            "theme3": "Adaptation",
            "percent": 8,
        },
        {
            "project ID": "P1234",
            "theme1": "Environment and Natural Resource Management",
            "theme2": "Climate change",
            "theme3": "Mitigation",
            "percent": 26,
        },
    ]

    assert world_bank_projects.clean_theme(test_data_dict) == formatted


def test_clean_theme_no_theme():
    """Test clean_theme function with no theme."""

    test_data_dict = {"id": "P1234"}
    assert world_bank_projects.clean_theme(test_data_dict) == []


def test_get_sector_data():
    """test the get_sector_data function."""

    d = {
        "id": "P1",
        "sector": [
            {"Name": "Agriculture, fishing, and forestry", "code": "BX"},
            {"Name": "Agricultural extension and research", "code": "AX"},
        ],
        "sector1": {"Name": "Agriculture, fishing, and forestry", "Percent": 50},
        "sector2": {"Name": "Agricultural extension and research", "Percent": 50},
    }

    expected = {
        "Agriculture, fishing, and forestry": 50,
        "Agricultural extension and research": 50,
    }

    assert world_bank_projects._get_sector_data(d) == expected


def test_get_sector_data_missing_sector():
    """Test the get_sector_data function with missing sector."""

    d = {
        "id": "P2",
        "sector": [
            {"Name": "Agriculture, fishing, and forestry", "code": "BX"},
            {"Name": "Agricultural extension and research", "code": "AX"},
            {"Name": "Missing sector", "code": "XX"},
        ],
        "sector1": {"Name": "Agriculture, fishing, and forestry", "Percent": 40},
        "sector2": {"Name": "Agricultural extension and research", "Percent": 50},
        "sector3": "Missing sector",
    }

    expected = {
        "Agriculture, fishing, and forestry": 40,
        "Agricultural extension and research": 50,
        "Missing sector": 10,
    }

    assert world_bank_projects._get_sector_data(d) == expected


def test_get_data_no_data_loaded():
    """Test the get_data function with no data loaded."""

    with pytest.raises(world_bank_projects.EmptyDataException):
        proj = world_bank_projects.WorldBankProjects()
        proj.get_data()
