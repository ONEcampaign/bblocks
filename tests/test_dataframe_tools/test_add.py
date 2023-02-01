import pytest

from bblocks.dataframe_tools.add import (
    add_population_column,
    __validate_add_column_params,
    add_short_names_column,
    add_iso_codes_column,
    add_poverty_ratio_column,
    add_population_density_column,
    add_population_share_column,
    add_median_observation,
    add_income_level_column,
    add_gdp_column,
    add_gdp_share_column,
    add_gov_expenditure_column,
    add_gov_exp_share_column,
    add_flourish_geometries,
    add_value_as_share,
)
import pandas as pd
from bblocks import set_bblocks_data_path, config

set_bblocks_data_path(config.BBPaths.tests_data)


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

    assert on_ == ["id_", "merge_year"]
    assert df_test.date.to_list() == df_test.date.to_list()

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

    assert df_test.merge_year.to_list() == [2022, 2002, 2005]

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

    df_test = add_population_column(df=df.copy(deep=True), id_column="iso_code")

    assert df_test.columns.to_list() == ["iso_code", "date", "value", "population"]

    ger = df_test.loc[df_test.iso_code == "Germany", "population"].to_list()
    assert ger[0] == ger[1]

    df_test_date = add_population_column(
        df=df.copy(deep=True), id_column="iso_code", date_column="date"
    )

    pop_date = df_test_date.population.to_list()[0:2]
    pop_no_date = df_test.population.to_list()[0:2]

    assert pop_date[0] < pop_no_date[0]


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


def test_add_iso_codes_column():
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

    df_test = add_iso_codes_column(
        df=df.copy(deep=True),
        id_column="formal_name",
        target_column="iso_code",
    )

    assert df_test.iso_code.to_list() == ["DEU", "USA", "GTM"]


def test_add_poverty_ratio_column():
    # Create a sample df
    df = pd.DataFrame(
        {
            "country_name": ["Sierra Leone", "Sierra Leone", "Guatemala"],
            "date": [2003, 1989, 2014],
        }
    )

    df_test = add_poverty_ratio_column(df=df.copy(deep=True), id_column="country_name")

    assert df_test.columns.to_list() == [
        "country_name",
        "date",
        "poverty_ratio",
    ]

    sle = df_test.loc[df_test.country_name == "Sierra Leone", "poverty_ratio"].to_list()
    assert sle[0] == sle[1]

    df_test_date = add_poverty_ratio_column(
        df=df.copy(deep=True), id_column="country_name", date_column="date"
    )

    pov_date = df_test_date.poverty_ratio.to_list()[0:2]
    pov_no_date = df_test.poverty_ratio.to_list()[0:2]

    assert pov_date[0] > pov_no_date[0]
    assert pov_date[1] > pov_no_date[1]


def test_add_population_density_column():
    # Create a sample df
    df = pd.DataFrame(
        {
            "country_name": ["France", "France", "Guatemala"],
            "date": [1989, 2017, 2014],
        }
    )

    df_test = add_population_density_column(
        df=df.copy(deep=True), id_column="country_name"
    )

    assert df_test.columns.to_list() == [
        "country_name",
        "date",
        "population_density",
    ]

    fra = df_test.loc[df_test.country_name == "France", "population_density"].to_list()
    assert fra[0] == fra[1]

    df_test_date = add_population_density_column(
        df=df.copy(deep=True), id_column="country_name", date_column="date"
    )

    pop_date = df_test_date.population_density.to_list()[0:2]
    pop_no_date = df_test.population_density.to_list()[0:2]

    assert pop_date[0] < pop_no_date[0]
    assert pop_date[1] < pop_no_date[1]


def test_add_population_share_column():
    # Create a sample df
    df = pd.DataFrame(
        {
            "iso_code": ["Germany", "Germany", "Guatemala", "Honduras", "Honduras"],
            "date": [2020, 2000, 2012, 2005, 2020],
            "value": [5 * 1e6, 10 * 1e6, 2 * 1e6, 5 * 1e6, 5 * 1e6],
        }
    )

    df_test = add_population_share_column(df=df.copy(deep=True), id_column="iso_code")

    assert df_test.columns.to_list() == [
        "iso_code",
        "date",
        "value",
        "population_share",
    ]

    ger = df_test.loc[df_test.iso_code == "Germany", "population_share"].to_list()
    assert 2 * round(ger[0]) == round(ger[1])

    df_test_date = add_population_share_column(
        df=df.copy(deep=True), id_column="iso_code", date_column="date"
    )

    pop_date = df_test_date.population_share.to_list()

    assert pop_date[-2] != pop_date[-1]


def test_add_median_observation():
    df = pd.DataFrame(
        {
            "iso_code": [
                "Germany",
                "Germany",
                "Germany",
                "France",
                "France",
                "France",
                "Singapore",
                "Singapore",
                "Singapore",
            ],
            "date": [2020, 2000, 2019, 2020, 2020, 2020, 2019, 2020, 2019],
            "continent": [
                "Europe",
                "Europe",
                "Europe",
                "Europe",
                "Europe",
                "Europe",
                "Asia",
                "Asia",
                "Asia",
            ],
            "value": [1, 3, 5, 5, 7, 9, 9, 11, 13],
            "number2": [40, 20, 30, 100, 60, 80, 140, 120, 100],
        }
    )

    # Test defaults
    result = add_median_observation(
        df=df, value_columns=["value", "number2"], group_by=["iso_code"]
    )

    assert result.loc[result.iso_code == "Germany", "value"][10] == 3.0
    assert result.loc[result.iso_code == "Singapore", "number2"][11] == 120.0

    # Test multiple group groups
    result2 = add_median_observation(
        df=df, value_columns=["value", "number2"], group_by=["iso_code", "continent"]
    )
    assert result2.loc[result.iso_code == "Germany", "value"][10] == 3.0
    assert result2.loc[result.iso_code == "Singapore", "number2"][11] == 120.0

    # test by continent
    result3 = add_median_observation(
        df=df,
        value_columns=["value", "number2"],
        group_by=["continent"],
    )

    assert result3.loc[result3.continent == "Europe", "value"].sum() == 35.0

    # test str value
    results4 = add_median_observation(
        df=df,
        value_columns="value",
        group_by="continent",
    )

    assert results4.loc[results4.continent == "Europe", "value"].sum() == 35.0

    with pytest.raises(ValueError):
        add_median_observation(
            df=df, value_columns=["nonsense"], group_by=["iso_code", "continent"]
        )

    with pytest.raises(ValueError):
        add_median_observation(
            df=df, value_columns=["value"], group_by=["nonsense", "continent"]
        )

    results5 = add_median_observation(df=df, value_columns=["value", "number2"])

    assert results5.loc[results5.iso_code == "France", "value"][9] == 7.0

    resultw = add_median_observation(
        df=df,
        value_columns=["value", "number2"],
        group_by=["iso_code", "continent"],
        append=False,
    )

    assert (
        resultw.loc[resultw.iso_code == "France", "value (median_observation)"].sum()
        / 3
        == 7.0
    )

    resultw2 = add_median_observation(
        df=df,
        value_columns=["value", "number2"],
        group_by=["continent"],
        append=False,
    )

    assert (
        resultw2.loc[resultw2.iso_code == "France", "value (median_observation)"].sum()
        / 3
        == 5.0
    )


def test_add_income_level_column():
    df = pd.DataFrame(
        {
            "country_name": ["Sierra Leone", "France", "Guatemala"],
            "date": [2003, 1989, 2014],
            "value": [1, 2, 3],
        }
    )

    df_test = add_income_level_column(
        df=df,
        id_column="country_name",
    )

    assert df_test.income_level.to_list() == [
        "Low income",
        "High income",
        "Upper middle income",
    ]


def test_add_gdp_column():
    df = pd.DataFrame(
        {
            "country_name": ["Sierra Leone", "France", "Guatemala"],
            "date": [2018, 2021, 2022],
            "value": [100 * 1e6, 2 * 1e9, 300 * 1e6],
        }
    )

    # test include estimates
    df_test = add_gdp_column(
        df=df,
        id_column="country_name",
        date_column="date",
        usd=True,
        include_estimates=True,
    )

    assert len(df_test.loc[df_test.gdp.isna()]) == 0

    df_test_lcu = add_gdp_column(
        df=df,
        id_column="country_name",
        date_column="date",
        usd=False,
        include_estimates=True,
    )

    assert df_test.gdp.values[0] < df_test_lcu.gdp.values[0]

    df_no_estimates = add_gdp_column(
        df=df,
        id_column="country_name",
        date_column="date",
        usd=True,
        include_estimates=False,
    )

    assert len(df_no_estimates.loc[df_no_estimates.gdp.isna()]) > 0


def test_add_gdp_share_column():
    # Create a sample df
    df = pd.DataFrame(
        {
            "iso_code": ["Germany", "Germany", "Guatemala"],
            "date": [2022, 2022, 2012],
            "value": [5 * 1e9, 10 * 1e9, 2 * 1e9],
        }
    )

    df_test = add_gdp_share_column(df=df, id_column="iso_code")

    assert df_test.columns.to_list() == [
        "iso_code",
        "date",
        "value",
        "gdp_share",
    ]

    ger = df_test.loc[df_test.iso_code == "Germany", "gdp_share"].to_list()
    assert round(2 * ger[0], 1) == round(ger[1], 1)

    df_test_date = add_gdp_share_column(
        df=df.copy(deep=True),
        id_column="iso_code",
        date_column="date",
        include_estimates=True,
    )

    _d = df_test_date.loc[lambda d: d.iso_code == "Guatemala"].gdp_share.sum()
    _nd = df_test.loc[lambda d: d.iso_code == "Guatemala"].gdp_share.sum()

    assert _d != _nd

    with pytest.raises(ValueError):
        _ = add_gdp_share_column(
            df=df,
            id_column="country_name",
            date_column="date",
            value_column="nonsense",
            usd=True,
            include_estimates=True,
        )


def test_add_gov_expenditure_column():
    df = pd.DataFrame(
        {
            "country_name": ["Sierra Leone", "France", "Guatemala"],
            "date": [2018, 2021, 2022],
            "value": [100 * 1e6, 2 * 1e9, 300 * 1e6],
        }
    )

    # test include estimates
    df_test = add_gov_expenditure_column(
        df=df,
        id_column="country_name",
        date_column="date",
        usd=True,
        include_estimates=True,
    )

    assert len(df_test.loc[df_test.gov_exp.isna()]) == 0

    df_test_lcu = add_gov_expenditure_column(
        df=df,
        id_column="country_name",
        date_column="date",
        usd=False,
        include_estimates=True,
    )

    assert df_test.gov_exp.values[0] < df_test_lcu.gov_exp.values[0]

    df_no_estimates = add_gov_expenditure_column(
        df=df,
        id_column="country_name",
        date_column="date",
        usd=True,
        include_estimates=False,
    )

    assert len(df_no_estimates.loc[df_no_estimates.gov_exp.isna()]) > 0


def test_add_gov_exp_share_column():
    # Create a sample df
    df = pd.DataFrame(
        {
            "iso_code": ["Germany", "Germany", "Guatemala"],
            "date": [2022, 2022, 2012],
            "value": [5 * 1e9, 10 * 1e9, 2 * 1e9],
        }
    )

    df_test = add_gov_exp_share_column(
        df=df,
        id_column="iso_code",
    )

    assert df_test.columns.to_list() == [
        "iso_code",
        "date",
        "value",
        "gov_exp_share",
    ]

    ger = df_test.loc[df_test.iso_code == "Germany", "gov_exp_share"].to_list()
    assert round(2 * ger[0], 1) == round(ger[1], 1)

    df_test_date = add_gov_exp_share_column(
        df=df.copy(deep=True),
        id_column="iso_code",
        date_column="date",
        include_estimates=True,
    )

    _d = df_test_date.loc[lambda d: d.iso_code == "Guatemala"].gov_exp_share.sum()
    _nd = df_test.loc[lambda d: d.iso_code == "Guatemala"].gov_exp_share.sum()

    assert _d != _nd

    with pytest.raises(ValueError):
        _ = add_gov_exp_share_column(
            df=df,
            id_column="country_name",
            value_column="nonsense",
            date_column="date",
            usd=True,
            include_estimates=True,
        )


def test_add_flourish_geometries():
    # Create a sample df
    df = pd.DataFrame(
        {
            "country_name": ["Sierra Leone", "France", "Guatemala"],
            "date": [2018, 2021, 2022],
            "value": [100 * 1e6, 2 * 1e9, 300 * 1e6],
        }
    )

    df_test = add_flourish_geometries(
        df=df,
        id_column="country_name",
    )

    assert df_test.columns.to_list() == ["country_name", "date", "value", "geometry"]
    assert df_test["geometry"].dropna().shape[0] == df_test.shape[0]


def test_add_value_as_share():
    # Create a sample df
    df = pd.DataFrame(
        {
            "country_name": ["Sierra Leone", "France", "Guatemala"],
            "date": [2018, 2021, 2022],
            "value": [100 * 1e3, 2 * 1e9, 300.343 * 1e5],
            "value2": [100 * 1e6, 2 * 1e9, 300 * 1e6],
        }
    )

    df_test = add_value_as_share(df, "value", "value2")

    assert df_test.columns.to_list() == [
        "country_name",
        "date",
        "value",
        "value2",
        "value_as_share_of_value2",
    ]

    assert df_test["value_as_share_of_value2"].to_list() == [0.1, 100.0, 10.01]

    with pytest.raises(ValueError):
        _ = add_value_as_share(df, "value", "nonsense")

    with pytest.raises(ValueError):
        _ = add_value_as_share(df, "nonsense", "value")

    df_test2 = add_value_as_share(df, "value", "value2", "share", 4)

    assert df_test2["share"].to_list() == [0.1, 100.0, 10.0114]
