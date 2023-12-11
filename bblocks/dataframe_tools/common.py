from functools import partial

import pandas as pd

from bblocks.cleaning_tools.clean import convert_to_datetime
from bblocks.cleaning_tools.filter import filter_latest_by
from bblocks.import_tools.imf import WorldEconomicOutlook
from bblocks.import_tools.world_bank import WorldBankData


def __get_wb_ind(
    ind_code: str, ind_name: str, update: bool, most_recent_only: bool = False, **kwargs
) -> pd.DataFrame:
    """Get a simplified DataFrame for a World Bank indicator"""
    if "mrnev" in kwargs:
        most_recent_only = kwargs["mrnev"]

    wb = WorldBankData().load_data(
        indicator=ind_code, most_recent_only=most_recent_only
    )

    if update:
        wb.update_data(reload_data=True)

    return (
        wb.get_data()
        .assign(year=lambda d: d.date.dt.year)
        .filter(["year", "iso_code", "value"], axis=1)
        .rename(columns={"value": ind_name})
    )


get_population_df = partial(__get_wb_ind, ind_code="SP.POP.TOTL", ind_name="population")

get_poverty_ratio_df = partial(
    __get_wb_ind, ind_code="SI.POV.DDAY", ind_name="poverty_headcount_ratio"
)

get_population_density_df = partial(
    __get_wb_ind, ind_code="EN.POP.DNST", ind_name="population_density"
)


def _get_weo_indicator(
    indicator: str,
    *,
    most_recent_only: bool,
    update: bool,
    include_estimates: bool = True,
) -> pd.DataFrame:
    # Create a World Economic Outlook object
    weo = WorldEconomicOutlook().load_data(indicator=indicator)

    # Update the data if needed
    if update:
        weo.update_data(reload_data=True)

    # Get the _data
    data = weo.get_data(keep_metadata=True).assign(
        year=lambda d: convert_to_datetime(d.year)
    )

    # Filter the _data to keep only non-estimates if needed
    if not include_estimates:
        data = data.loc[lambda d: ~d.estimate]

    # limit most recent to current year
    data = data.loc[lambda d: d.year.dt.year <= pd.Timestamp.now().year]

    # Filter the _data to keep only the most recent _data if needed
    if most_recent_only:
        data = filter_latest_by(
            data, date_column="year", value_columns="value", group_by=["iso_code"]
        )

    return data.assign(year=lambda d: d.year.dt.year).filter(
        ["year", "iso_code", "value"], axis=1
    )


def get_gdp_df(
    *,
    usd: bool = True,
    most_recent_only: bool,
    update_data: bool,
    include_estimates: bool = True,
) -> pd.DataFrame:
    """Get a population DataFrame"""

    # Select the right indicator depending on the USD flag
    indicator = "NGDPD" if usd else "NGDP"

    return _get_weo_indicator(
        indicator,
        most_recent_only=most_recent_only,
        update=update_data,
        include_estimates=include_estimates,
    ).assign(
        value=lambda d: d.value * 1e9,
    )


def get_gov_expenditure_df(
    *,
    usd: bool = True,
    most_recent_only: bool,
    update_data: bool,
    include_estimates: bool = True,
) -> pd.DataFrame:
    """Get a population DataFrame"""

    gov_exp = _get_weo_indicator(
        indicator="GGX_NGDP",
        most_recent_only=most_recent_only,
        update=update_data,
        include_estimates=include_estimates,
    )

    gdp = get_gdp_df(
        usd=usd,
        most_recent_only=most_recent_only,
        update_data=update_data,
        include_estimates=include_estimates,
    )

    return (
        gov_exp.merge(
            gdp, on=["year", "iso_code"], how="left", suffixes=("_gov", "_gdp")
        )
        .assign(value=lambda d: d.value_gov / 100 * d.value_gdp)
        .filter(["year", "iso_code", "value"], axis=1)
    )
