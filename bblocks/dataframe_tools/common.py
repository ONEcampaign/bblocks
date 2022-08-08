import pandas as pd
from bblocks.import_tools.world_bank import WorldBankData


def __get_wb_ind(
    ind_code: str, ind_name: str, update: bool, mrnev: bool
) -> pd.DataFrame:
    """Get a simplified DataFrame for a World Bank indicator"""
    return (
        WorldBankData(update_data=update)
        .load_indicator(
            ind_code,
            most_recent_only=mrnev,
        )
        .get_data()
        .assign(year=lambda d: d.date.dt.year)
        .filter(["year", "iso_code", "value"], axis=1)
        .rename(columns={"value": ind_name})
    )


def get_population_df(*, most_recent_only, update_population_data) -> pd.DataFrame:
    """Get a population DataFrame"""

    return __get_wb_ind(
        ind_code="SP.POP.TOTL",
        ind_name="population",
        update=update_population_data,
        mrnev=most_recent_only,
    )


def get_poverty_ratio_df(*, most_recent_only, update_poverty_data) -> pd.DataFrame:
    """Get a population DataFrame"""

    return __get_wb_ind(
        ind_code="SI.POV.DDAY",
        ind_name="poverty_headcount_ratio",
        update=update_poverty_data,
        mrnev=most_recent_only,
    )


def get_population_density_df(
    *, most_recent_only, update_population_data
) -> pd.DataFrame:
    """Get a population DataFrame"""

    return __get_wb_ind(
        ind_code="EN.POP.DNST",
        ind_name="population_density",
        update=update_population_data,
        mrnev=most_recent_only,
    )
