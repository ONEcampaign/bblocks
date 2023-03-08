from typing import Type
from unittest.mock import patch

import pandas as pd
import pytest
import requests

from bblocks import DebtIDS, config, set_bblocks_data_path
from bblocks.import_tools.debt import get_data as ids
from bblocks.import_tools.debt.wb_ids import read_indicators

set_bblocks_data_path(config.BBPaths.tests_data)


def mock_pyjstat_read(*args, **kwargs) -> callable:
    return MockPyjstatWrite


class MockPyjstatWrite:
    error: bool | Exception | Type = False

    def __init__(self, url, *args, **kwargs):
        if "ValueError" in url:
            self.error = ValueError

        elif "HTTPError" in url:
            self.error = requests.exceptions.HTTPError

        elif "JsonError" in url:
            self.error = requests.exceptions.JSONDecodeError

        else:
            self.error = False

        self.data = pd.DataFrame({"country": ["FRA", "GTM"], "value": [1, 2.2]})

    def write(self, *args, **kwargs):
        if not self.error:
            return self.data
        else:
            if self.error == requests.exceptions.JSONDecodeError:
                raise self.error("test", "test", 3)
            raise self.error


def test_get_indicator_data():
    with patch(
            "pyjstat.pyjstat.Dataset.read",
            new_callable=mock_pyjstat_read,
    ):
        ids.get_indicator_data("test")

    with patch("pyjstat.pyjstat.Dataset.read", new_callable=mock_pyjstat_read), patch(
            "time.sleep", return_value=None
    ):
        ids.get_indicator_data("HTTPError")

    with patch("pyjstat.pyjstat.Dataset.read", new_callable=mock_pyjstat_read), patch(
            "time.sleep", return_value=None
    ):
        ids.get_indicator_data("JsonError")

    with patch("pyjstat.pyjstat.Dataset.read", new_callable=mock_pyjstat_read), patch(
            "time.sleep", return_value=None
    ):
        ids.get_indicator_data("ValueError")


def test__time_period():
    test_single = ids._time_period(2014, 2014)
    test_multiple = ids._time_period(2014, 2016)

    expected_single = "yr2014"
    expected_multiple = "yr2014;yr2015;yr2016"

    assert test_single == expected_single
    assert test_multiple == expected_multiple


def test__country_list():
    test_single = ids._country_list("GTM")
    test_multiple = ids._country_list(["GTM", "FRA", "TGO"])

    expected_single = "GTM"
    expected_multiple = "GTM;FRA;TGO"

    assert test_single == expected_single
    assert test_multiple == expected_multiple


def test__clean_ids_response():
    test_df = pd.DataFrame(
        {"country": ["FRA", "FRA", "GTM", "GTM"], "value": [1, pd.NA, 2.2, pd.NA]}
    )

    expected_df = pd.DataFrame(
        {"country": ["FRA", "GTM"], "value": [1, 2.2], "series_code": "test"}
    )

    clean_df = ids._clean_ids_response(test_df, "test")

    pd.testing.assert_frame_equal(clean_df, expected_df)


def _mock_ids_response(pages: int):
    return [
        {"page": 1, "pages": pages, "per_page": "5", "total": 299},
        [
            {
                "id": "ABW",
                "iso2Code": "AW",
                "name": "Aruba",
            },
            {
                "id": "AFE",
                "iso2Code": "ZH",
                "name": "Africa Eastern and Southern",
            },
        ],
    ]


def test_ids_api_url():
    expected_url = (
        "https://api.worldbank.org/v2/sources/6/country/"
        "all/series/DT.AMT.BLAT.CD/time/yr2015;yr2016/"
        "data?format=jsonstat"
    )

    test_url = ids._api_url("DT.AMT.BLAT.CD", "all", 2015, 2016, 6)

    assert test_url == expected_url

    with pytest.raises(TypeError):
        ids._api_url(["ind1", "ind2"], "all", 2015, 2016, 6)


# --------------- Class Tests ---------------


def test__read_indicators():
    result = read_indicators()
    assert isinstance(result, dict)


def test_right_path():
    result = DebtIDS()
    assert result._path == config.BBPaths.tests_data / "ids_data"


def test__stored_data_not_found():
    test = DebtIDS()

    # Check file not found
    not_found = test._check_stored_data(
        indicator="test", start_year=2015, end_year=2016
    )

    assert not_found is False


def test__stored_data_found():
    test = DebtIDS()

    # Check file not found
    found = test._check_stored_data(
        indicator="DT.AMT.BLAT.CD", start_year=2015, end_year=2016
    )

    assert found == "DT.AMT.BLAT.CD_2015-2018.feather"


def test__indicator_parameters():
    test = DebtIDS()

    # Check file not found
    params = test._indicator_parameters(indicator="DT.AMT.BLAT.CD_2015-2018")

    assert params == ("DT.AMT.BLAT.CD", 2015, 2018)


def test_get_available_indicators():
    test = DebtIDS()
    result = test.get_available_indicators()
    assert isinstance(result, dict)


def test_debt_service_indicators_detail_false():
    test = DebtIDS()
    result = test.debt_service_indicators(False)

    assert isinstance(result, dict)

    for key, value in result.items():
        assert "Principal" not in value


def test_debt_service_indicators_detail_true():
    test = DebtIDS()
    result = test.debt_service_indicators(True)

    assert isinstance(result, dict)

    details = ["Principal" in value for value in result.values()]

    assert any(details)


def test_debt_stocks_indicators_detail_false():
    test = DebtIDS()
    result = test.debt_stocks_indicators(False)

    assert isinstance(result, dict)

    for key, value in result.items():
        assert "Bonds" not in value


def test_debt_stocks_indicators_detail_true():
    test = DebtIDS()
    result = test.debt_stocks_indicators(True)

    assert isinstance(result, dict)

    details = ["Bonds" in value for value in result.values()]

    assert any(details)


def test_get_indicator():
    sample_df = pd.DataFrame(
        {
            "country": ["FRA", "GTM"],
            "counterpart-area": ["USA", "DEU"],
            "time": [2015, 2016],
            "value": [1, 2.2],
            "series_code": "test",
        }
    )

    test = DebtIDS()

    with patch(
            "bblocks.import_tools.debt.wb_ids.get_indicator_data", return_value=sample_df
    ) as get, patch("pandas.DataFrame.to_feather", return_value=None) as save:
        test._get_indicator("test", 2015, 2016)

        assert get.assert_called
        assert save.assert_called


def test_load_data_list():
    test = DebtIDS()

    # Test list
    test.load_data(indicators=["DT.AMT.BLAT.CD"], start_year=2015, end_year=2018)

    assert "DT.AMT.BLAT.CD_2015-2018" in test._data.keys()


def test_load_data_str():
    test = DebtIDS()

    # Test str
    test.load_data(indicators="DT.AMT.BLAT.CD", start_year=2015, end_year=2018)

    assert "DT.AMT.BLAT.CD_2015-2018" in test._data.keys()


def test_load_data_invalid():
    test = DebtIDS()

    # Test invalid
    with pytest.raises(ValueError):
        test.load_data(indicators=["xxx"], start_year=2015, end_year=2018)

    # Test valid and invalid
    with pytest.raises(ValueError):
        test.load_data(
            indicators=["DT.AMT.BLAT.CD", "xxx"], start_year=2015, end_year=2018
        )


def test_load_data_not_downloaded():
    test = DebtIDS()
    sample_df = pd.DataFrame(
        {
            "country": ["FRA", "GTM"],
            "counterpart_area": ["USA", "DEU"],
            "year": [2015, 2016],
            "value": [1, 2.2],
            "series_code": "DT.INT.BLAT.CD",
        }
    )

    with patch(
            "bblocks.import_tools.debt.wb_ids.DebtIDS._get_indicator",
            return_value=None,
    ) as get, patch("pandas.read_feather", return_value=sample_df) as read:
        test.load_data(indicators="DT.INT.BLAT.CD", start_year=2015, end_year=2018)

        assert get.assert_called
        assert read.assert_called


def test_update_data_not_loaded():
    test = DebtIDS()

    with pytest.raises(ValueError):
        test.update_data()


def test_update_data_loaded():
    test = DebtIDS()

    test.load_data(indicators="DT.AMT.BLAT.CD", start_year=2015, end_year=2018)

    # No reload
    with patch("bblocks.import_tools.debt.wb_ids.DebtIDS._get_indicator") as get:
        test.update_data(reload_data=False)
        assert get.assert_called

    # Reload
    with patch("bblocks.import_tools.debt.wb_ids.DebtIDS._get_indicator") as get:
        test.update_data(reload_data=True)
        assert get.assert_called


def test_get_data_all():
    test = DebtIDS()

    test.load_data(indicators="DT.AMT.BLAT.CD", start_year=2015, end_year=2018)

    result = test.get_data(indicators='all')

    assert isinstance(result, pd.DataFrame)
    assert result.series_code.unique()[0] == "DT.AMT.BLAT.CD"



def test_get_data_indicator():
    test = DebtIDS()

    test.load_data(indicators="DT.AMT.BLAT.CD", start_year=2015, end_year=2018)

    result = test.get_data(indicators='DT.AMT.BLAT.CD')

    assert isinstance(result, pd.DataFrame)
    assert result.series_code.unique()[0] == "DT.AMT.BLAT.CD"