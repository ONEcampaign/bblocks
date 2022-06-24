import os

import pandas as pd
from country_converter import convert

from bblocks.config import PATHS
from bblocks.import_tools.world_bank import WorldBankData
from bblocks.other_tools.common import Dict


def __download_income_levels():
    """Downloads fresh version of income levels from WB"""
    url = "https://databank.worldbank.org/data/download/site-content/CLASS.xlsx"

    df = pd.read_excel(
        url,
        sheet_name="List of economies",
        usecols=["Code", "Income group"],
        na_values=None,
    )

    df = df.dropna(subset=["Income group"])

    df.to_csv(PATHS.imported_data + r"/income_levels.csv", index=False)
    print("Downloaded income levels")


def __get_income_levels() -> dict:
    """Return income level dictionary"""
    file = PATHS.imported_data + r"/income_levels.csv"
    if not os.path.exists(file):
        __download_income_levels()

    return pd.read_csv(file, na_values=None, index_col="Code")["Income group"].to_dict()


__wb = WorldBankData()
(
    __wb.load_indicator("SP.DYN.LE00.IN", most_recent_only=True)
    .load_indicator("EN.POP.DNST", most_recent_only=True)
    .load_indicator("SP.POP.TOTL", most_recent_only=True)
    .load_indicator("SI.POV.DDAY", most_recent_only=True)
)


def update_dictionaries() -> None:
    """Updates dictionaries"""
    __wb.update()
    __download_income_levels()


g20_countries: Dict = Dict(
    {
        x: convert(x, src="ISO3", to="name_short", not_found=None)
        for x in [
            "ARG",
            "AUS",
            "BRA",
            "CAN",
            "CHN",
            "FRA",
            "DEU",
            "IND",
            "IDN",
            "ITA",
            "KOR",
            "JPN",
            "MEX",
            "RUS",
            "SAU",
            "ZAF",
            "TUR",
            "GBR",
            "USA",
        ]
    }
)

eu27 = Dict(
    {
        x: convert(x, src="ISO3", to="name_short")
        for x in [
            "AUT",
            "BEL",
            "BGR",
            "HRV",
            "CZE",
            "DNK",
            "EST",
            "FIN",
            "FRA",
            "DEU",
            "GRC",
            "HUN",
            "IRL",
            "ITA",
            "LVA",
            "LTU",
            "LUX",
            "MLT",
            "NLD",
            "POL",
            "PRT",
            "ROU",
            "SVK",
            "SVN",
            "ESP",
            "SWE",
            "GBR",
        ]
    }
)

g7 = Dict(
    {
        x: convert(x, src="ISO3", to="name_short")
        for x in ["FRA", "DEU", "ITA", "GBR", "USA", "JPN", "CAN"]
    }
)

income_levels = Dict(__get_income_levels())

life_expectancy = Dict(
    __wb.get_data("SP.DYN.LE00.IN").set_index("iso_code")["value"].to_dict()
)

population_density = Dict(
    __wb.get_data("EN.POP.DNST").set_index("iso_code")["value"].to_dict()
)

population = Dict(__wb.get_data("SP.POP.TOTL").set_index("iso_code")["value"].to_dict())
