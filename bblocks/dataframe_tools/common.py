import pandas as pd
from bblocks.import_tools.world_bank import WorldBankData


def get_population_df(*, most_recent_only, update_population_data) -> pd.DataFrame:
    """Get a population DataFrame"""

    return (
        WorldBankData(update_data=update_population_data)
        .load_indicator(
            "SP.POP.TOTL",
            indicator_name="population",
            most_recent_only=most_recent_only,
        )
        .get_data()
        .assign(year=lambda d: d.date.dt.year)
        .filter(["year", "iso_code", "value"], axis=1)
        .rename(columns={"value": "population"})
    )
