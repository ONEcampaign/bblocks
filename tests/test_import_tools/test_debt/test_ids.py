import pandas as pd
import pytest

from bblocks.import_tools.debt import ids


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


def test__ids_countries_url():
    expected_url = (
        "https://api.worldbank.org/v2/country?per_page=300&page=3&format=json"
    )

    test_url = ids._ids_countries_url(page=3)

    assert test_url == expected_url


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


def test__get_ids_countries_dict():
    single_response = _mock_ids_response(1)

    test_single = ids._get_ids_countries_dict(single_response)

    expected_single = {"ABW": "Aruba", "AFE": "Africa Eastern and Southern"}

    assert test_single == expected_single


def test_ids_codes():
    assert len(ids.ids_codes()) > 1


def test_ids_api_url():
    expected_url = (
        "https://api.worldbank.org/v2/sources/6/country/"
        "all/series/DT.AMT.BLAT.CD/time/yr2015;yr2016/"
        "_data?format=jsonstat"
    )

    test_url = ids.ids_api_url("DT.AMT.BLAT.CD", "all", 2015, 2016, 6)

    assert test_url == expected_url

    with pytest.raises(TypeError):
        ids.ids_api_url(["ind1", "ind2"], "all", 2015, 2016, 6)
