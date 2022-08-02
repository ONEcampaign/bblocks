import pytest

from bblocks.dataframe_tools.add import (
    add_population_column,
    __validate_add_column_params,
    add_short_names_column,
)
import pandas as pd


def test___validate_column_params():
    # Create a sample df
    df = pd.DataFrame(
        {
            "iso_code": ["France", "Germany", "Guatemala"],
            "date": [2000, 2001, 2002],
            "value": [100, 120, 230],
        }
    )

    df_test, on_ = __validate_add_column_params(
        df=df.copy(deep=True),
        id_type=None,
        id_column="iso_code",
        date_column=None,
    )

    assert on_ == ["id_"]
    assert df_test.id_.to_list() == ["FRA", "DEU", "GTM"]

    df_test, on_ = __validate_add_column_params(
        df=df.copy(deep=True),
        id_type="short_name",
        id_column="iso_code",
        date_column="date",
    )

    assert on_ == ["id_", "year"]
    assert df_test.year.to_list() == df_test.date.to_list()

    with pytest.raises(ValueError):
        __validate_add_column_params(
            df=df.copy(deep=True),
            id_type=None,
            id_column="nonsense",
            date_column=None,
        )

    with pytest.raises(ValueError):
        __validate_add_column_params(
            df=df.copy(deep=True),
            id_type=None,
            id_column="iso_code",
            date_column="None",
        )

    df = pd.DataFrame(
        {
            "name": ["France", "Germany", "Guatemala"],
            "year": ["13/07/2022", "01/01/2002", "02/03/2005"],
            "value": [100, 120, 230],
        }
    )

    df_test, on_ = __validate_add_column_params(
        df=df.copy(deep=True),
        id_type="short_name",
        id_column="name",
        date_column="year",
    )

    assert df_test.year.to_list() == [2022, 2002, 2005]

    df_errors = pd.DataFrame(
        {
            "name": ["France", "Germany", "Guatemala"],
            "year": ["15/2022", "02.3/2002", "25/2005"],
            "value": [100, 120, 230],
        }
    )
    with pytest.raises(ValueError):
        _, _ = __validate_add_column_params(
            df=df_errors.copy(deep=True),
            id_type="short_name",
            id_column="name",
            date_column="year",
        )

    df_errors = pd.DataFrame(
        {
            "name": ["France", "Germany", "Guatemala"],
            "year": [5, 12.3, 5456.3],
            "value": [100, 120, 230],
        }
    )

    with pytest.raises(ValueError):
        _, _ = __validate_add_column_params(
            df=df_errors.copy(deep=True),
            id_type="short_name",
            id_column="name",
            date_column="year",
        )


def test_add_population_column():
    # Create a sample df
    df = pd.DataFrame(
        {
            "iso_code": ["Germany", "Germany", "Guatemala"],
            "date": [2000, 2020, 2012],
            "value": [100, 120, 230],
        }
    )

    df_test = add_population_column(
        df=df.copy(deep=True),
        id_column="iso_code",
    )

    assert df_test.columns.to_list() == ["iso_code", "date", "value", "population"]

    ger = df_test.loc[df_test.iso_code == "Germany", "population"].to_list()
    assert ger[0] == ger[1]

    df_test_date = add_population_column(
        df=df.copy(deep=True),
        id_column="iso_code",
        date_column="date",
    )

    pop_date = df_test_date.population.to_list()[0:2]
    pop_no_date = df_test.population.to_list()[0:2]

    assert pop_date[0] < pop_no_date[0]
    assert pop_date[1] > pop_no_date[1]


def test_add_short_names_column():
    # Create a sample df
    df = pd.DataFrame(
        {
            "formal_name": [
                "Federal Republic of Germany",
                "United States of America",
                "Republic of " "Guatemala",
            ],
            "col_1": [2000, 2020, 2012],
            "col_2": [100, 120, 230],
        }
    )

    df_test = add_short_names_column(
        df=df.copy(deep=True),
        id_column="formal_name",
        target_column="short_name",
    )

    assert df_test.short_name.to_list() == ["Germany", "United States", "Guatemala"]
