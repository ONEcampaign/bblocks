from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from bblocks.cleaning_tools.clean import (
    clean_number,
    clean_numeric_series,
    convert_id,
    to_date_column,
    date_to_str,
    format_number,
    convert_to_datetime,
)


def test_clean_number() -> None:
    # Test several types of inputs
    n1 = "1.2"
    n2 = "1,000.23"
    n3 = "5,000,000"
    n5 = "$45.50"

    assert clean_number(n1) == 1.2
    assert clean_number(n1, to=int) == 1
    assert clean_number(n2) == 1000.23
    assert clean_number(n2, to=int) == 1000
    assert clean_number(n3, to=float) == 5_000_000
    assert clean_number(n3, to=int) == 5_000_000
    assert clean_number(n5) == 45.50
    assert clean_number(n5, to=int) == 46


def test_clean_numeric_series() -> None:
    # test a dataframe
    df = pd.DataFrame(
        {
            "a": ["1", "$2.5", "3,000.43"],
            "b": [
                "456,234.1",
                "$5,000,000",
                "strong",
            ],
        }
    )

    # test df with a single column as string
    assert [1.0, 2.5, 3000.43] == clean_numeric_series(df, series_columns="a")[
        "a"
    ].to_list()

    # test df with two columns
    assert [456234.1, 5000000.0] == clean_numeric_series(df, series_columns=["a", "b"])[
        "b"
    ].to_list()[:2]

    # test a series
    s = pd.Series(["1", "$2.5", "3,000.43"])
    assert [1, 2, 3000] == clean_numeric_series(s, to=int).to_list()


def test_convert_id():
    s_dac = pd.Series([3, 4, 12, 9999], index=["i1", "i2", "i3", "m1"])
    s_iso3 = pd.Series(["FRA", "GBR", "USA"], index=["i1", "hi", "i3"])

    s_dac_iso3 = convert_id(
        series=s_dac, from_type="DACcode", to_type="ISO3", not_found=None
    )

    assert s_dac_iso3.to_list() == ["DNK", "FRA", "GBR", 9999]

    s_dac_iso2_nf = convert_id(
        series=s_dac, from_type="DACcode", to_type="ISO2", not_found="not_found"
    )

    assert s_dac_iso2_nf.to_list() == ["DK", "FR", "GB", "not_found"]

    s_iso3_dac = convert_id(series=s_iso3, from_type="ISO3", to_type="DACcode")

    assert s_iso3_dac.to_list() == [4, 12, 302]

    assert (
        convert_id(series=s_iso3, from_type="ISO3", to_type="ISO3").to_list()
        == s_iso3.to_list()
    )

    s_iso3_names = convert_id(series=s_iso3, from_type="ISO3", to_type="name_short")

    assert s_iso3_names.to_list() == ["France", "United Kingdom", "United States"]


def test_to_date_column():
    # Create a sample dataframe with a date column

    df = pd.DataFrame(
        {
            "date": [
                "2020-01-01",
                "2020-01-02",
                "2021-01-03",
                "2021-01-04",
                "2022-01-05",
            ],
            "date2": [
                2020,
                2020,
                2021,
                2021,
                2022,
            ],
            "date3": [
                "12/01/2020",
                "12/02/2020",
                "18/03/2021",
                "24/04/2021",
                "03/05/2022",
            ],
        }
    )

    # Infer format of a string
    result1 = to_date_column(df.date)
    assert result1.dtype == "datetime64[ns]"
    assert result1.dt.year.to_list() == [2020, 2020, 2021, 2021, 2022]

    # infer format of an integer
    result2 = to_date_column(df.date2)
    assert result2.dtype == "datetime64[ns]"
    assert result2.dt.year.to_list() == [2020, 2020, 2021, 2021, 2022]

    # Infer format of a different string
    result3 = to_date_column(df.date3)

    assert result3.dtype == "datetime64[ns]"
    assert any(result3.dt.year.isna())

    # Specify format
    result4 = to_date_column(df.date3, date_format="%d/%m/%Y")
    assert result4.dtype == "datetime64[ns]"
    assert result4.dt.day.to_list() == [12, 12, 18, 24, 3]


def test_date_to_str():
    # Create a sample dataframe with a date column
    df = pd.DataFrame(
        {
            "date": [
                "2020-01-01",
                "2020-04-02",
                "2021-01-03",
                "2021-08-04",
                "2022-11-05",
            ],
            "date2": [
                "2100-error",
                "2020-01-02",
                "2021-01-03",
                "2021-01-04",
                "2022-01-05",
            ],
            "country": ["DNK", "FRA", "GBR", "USA", "DNK"],
        }
    )

    df = df.assign(date=date_to_str(df.date))

    assert df.date.to_list() == [
        "01 January 2020",
        "02 April 2020",
        "03 January 2021",
        "04 August 2021",
        "05 November 2022",
    ]

    # check invalid returns nan
    df = df.assign(date=date_to_str(df.date2))
    assert any(df.date.isna())


def test_format_number():
    # sample dataframe with numeric columns
    df = pd.DataFrame(
        {
            "a": [123, 2433, 32, 4.6, 53423.3],
            "b": [10000, 20000, 343214532, 4432, 53],
            "c": [0.5, 0.6, 0.7, 0.8, 0.9],
            "d": [12.2 * 1e9, 2.3 * 1e9, 1.4 * 1e9, 2.5 * 1e9, 1.6 * 1e9],
        }
    )

    test = df.assign(a=format_number(df.a, as_units=True, decimals=0))
    assert test.a.to_list() == ["123", "2,433", "32", "5", "53,423"]

    test = df.assign(b=format_number(df.b, as_millions=True, decimals=3))
    assert test.b.to_list() == ["0.010", "0.020", "343.215", "0.004", "0.000"]

    test = df.assign(c=format_number(df.c, as_percentage=True, decimals=1))
    assert test.c.to_list() == ["50.0%", "60.0%", "70.0%", "80.0%", "90.0%"]

    test = df.assign(d=format_number(df.d, as_billions=True, decimals=1))
    assert test.d.to_list() == ["12.2", "2.3", "1.4", "2.5", "1.6"]

    test = df.assign(a=format_number(df.a, other_format="{:.8f}"))
    assert test.a.to_list() == [
        "123.00000000",
        "2433.00000000",
        "32.00000000",
        "4.60000000",
        "53423.30000000",
    ]

    with pytest.raises(ValueError):
        _ = test.assign(a=format_number(test.a))

    with pytest.raises(KeyError):
        _ = test.assign(b=format_number(test.b, as_billions=True, as_percentage=True))

    with pytest.warns(UserWarning):
        _ = test.assign(c=format_number(test.c))

    with pytest.raises(KeyError):
        _ = test.assign(
            b=format_number(test.b, as_billions=True, other_format="{:.8f}")
        )


def test_convert_to_datetime():
    # Single string date cases:
    single_string_date = "2022"
    assert convert_to_datetime(single_string_date) == pd.Timestamp(
        datetime.strptime(single_string_date, "%Y")
    )

    single_string_date2 = "2022-05-13"
    assert convert_to_datetime(single_string_date2) == pd.Timestamp(
        datetime.strptime(single_string_date2, "%Y-%m-%d")
    )

    # Single integer date case:
    single_integer_date = 2022
    assert convert_to_datetime(single_integer_date) == pd.Timestamp(
        datetime.strptime(str(single_integer_date), "%Y")
    )

    # pd.Series case:
    series_dates = pd.Series(["2022", "2023", "2024"])
    expected_results = pd.Series(
        [
            pd.Timestamp(datetime.strptime(date, "%Y"))
            for date in ["2022", "2023", "2024"]
        ]
    )
    pd.testing.assert_series_equal(convert_to_datetime(series_dates), expected_results)

    # Case where pd.Series has NaN
    series_with_nan = pd.Series(["2022", "2023", np.nan, "2024"])
    expected_with_nan = pd.Series(
        [
            (
                pd.Timestamp(datetime.strptime(date, "%Y"))
                if isinstance(date, str)
                else pd.NaT
            )
            for date in ["2022", "2023", np.nan, "2024"]
        ]
    )
    pd.testing.assert_series_equal(
        convert_to_datetime(series_with_nan), expected_with_nan
    )
