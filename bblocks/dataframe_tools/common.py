import pandas as pd

from bblocks.cleaning_tools.filter import filter_latest_by
from bblocks.import_tools.imf import WorldEconomicOutlook
from bblocks.import_tools.world_bank import WorldBankData


def __get_wb_ind(
    ind_code: str, ind_name: str, update: bool, mrnev: bool, data_path: str = None
) -> pd.DataFrame:
    """Get a simplified DataFrame for a World Bank indicator"""
    return (
        WorldBankData(update_data=update, data_path=data_path)
        .load_indicator(
            ind_code,
            most_recent_only=mrnev,
        )
        .get_data()
        .assign(year=lambda d: d.date.dt.year)
        .filter(["year", "iso_code", "value"], axis=1)
        .rename(columns={"value": ind_name})
    )


def get_population_df(
    *, most_recent_only, update_population_data, data_path: str = None
) -> pd.DataFrame:
    """Get a population DataFrame"""

    return __get_wb_ind(
        ind_code="SP.POP.TOTL",
        ind_name="population",
        update=update_population_data,
        mrnev=most_recent_only,
        data_path=data_path,
    )


def get_poverty_ratio_df(
    *, most_recent_only, update_poverty_data, data_path: str = None
) -> pd.DataFrame:
    """Get a population DataFrame"""

    return __get_wb_ind(
        ind_code="SI.POV.DDAY",
        ind_name="poverty_headcount_ratio",
        update=update_poverty_data,
        mrnev=most_recent_only,
        data_path=data_path,
    )


def get_population_density_df(
    *, most_recent_only, update_population_data, data_path: str = None
) -> pd.DataFrame:
    """Get a population DataFrame"""

    return __get_wb_ind(
        ind_code="EN.POP.DNST",
        ind_name="population_density",
        update=update_population_data,
        mrnev=most_recent_only,
        data_path=data_path,
    )


def _get_weo_indicator(
    indicator: str,
    *,
    most_recent_only: bool,
    update_data: bool,
    include_estimates: bool = True,
    data_path: str = None,
) -> pd.DataFrame:

    # Create a World Economic Outlook object
    weo = WorldEconomicOutlook(update_data=update_data, data_path=data_path)

    # Get the data
    data = (
        weo.load_indicator(indicator)
        .get_data(keep_metadata=True)
        .assign(year=lambda d: pd.to_datetime(d.year, format="%Y"))
    )

    # Filter the data to keep only non-estimates if needed
    if not include_estimates:
        data = data.loc[lambda d: ~d.estimate]

    # limit most recent to current year
    data = data.loc[lambda d: d.year.dt.year <= pd.Timestamp.now().year]

    # Filter the data to keep only the most recent data if needed
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
    update_gdp_data: bool,
    include_estimates: bool = True,
    data_path: str = None,
) -> pd.DataFrame:
    """Get a population DataFrame"""

    # Select the right indicator depending on the USD flag
    indicator = "NGDPD" if usd else "NGDP"

    return _get_weo_indicator(
        indicator,
        most_recent_only=most_recent_only,
        update_data=update_gdp_data,
        include_estimates=include_estimates,
        data_path=data_path,
    ).assign(
        value=lambda d: d.value * 1e9,
    )


def get_gov_expenditure_df(
    *,
    usd: bool = True,
    most_recent_only: bool,
    update_data: bool,
    include_estimates: bool = True,
    data_path: str = None,
) -> pd.DataFrame:
    """Get a population DataFrame"""

    gov_exp = _get_weo_indicator(
        indicator="GGX_NGDP",
        most_recent_only=most_recent_only,
        update_data=update_data,
        include_estimates=include_estimates,
        data_path=data_path,
    )

    gdp = get_gdp_df(
        usd=usd,
        most_recent_only=most_recent_only,
        update_gdp_data=update_data,
        include_estimates=include_estimates,
        data_path=data_path,
    )

    return (
        gov_exp.merge(
            gdp, on=["year", "iso_code"], how="left", suffixes=("_gov", "_gdp")
        )
        .assign(value=lambda d: d.value_gov / 100 * d.value_gdp)
        .filter(["year", "iso_code", "value"], axis=1)
    )
