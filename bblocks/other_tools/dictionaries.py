import os

import pandas as pd
from country_converter import convert

from bblocks import config
from bblocks.import_tools.world_bank import WorldBankData
from bblocks.other_tools.common import Dict


def __download_income_levels():
    """Downloads fresh version of income levels from WB"""
    url = "https://databank.worldbank.org/data/download/site-content/CLASS.xlsx"

    df = pd.read_excel(
        io=url,
        sheet_name="List of economies",
        usecols=["Code", "Income group"],
        na_values=None,
    )

    df = df.dropna(subset=["Income group"])

    df.to_csv(config.BBPaths.raw_data / "income_levels.csv", index=False)
    print("Downloaded income levels")


def __get_income_levels() -> dict:
    """Return income level dictionary"""
    file = config.BBPaths.raw_data / "income_levels.csv"
    if not os.path.exists(file):
        __download_income_levels()

    return pd.read_csv(file, na_values=None, index_col="Code")["Income group"].to_dict()


def __wb() -> WorldBankData:
    return WorldBankData().load_data(
        indicator=["SP.DYN.LE00.IN", "EN.POP.DNST", "SP.POP.TOTL", "SI.POV.DDAY"],
        most_recent_only=True,
    )


def __get_dac_codes() -> dict:
    """Return dac codes dictionary"""
    file = config.BBPaths.import_settings / "oecd_codes.csv"

    return pd.read_csv(file, na_values=None, index_col="code")["iso_code"].to_dict()


def __read_flourish_geometries() -> dict:
    """Reads flourish geometries"""
    file = config.BBPaths.import_settings / "flourish_geometries.csv"
    return pd.read_csv(file, na_values=None, index_col="3-letter ISO code")[
        "geometry"
    ].to_dict()


def update_dictionaries() -> None:
    """Updates dictionaries"""
    __wb.update_data()
    __download_income_levels()


def g20_countries() -> dict:
    return Dict(
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


def eu27() -> dict:
    return Dict(
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


def g7() -> dict:
    return Dict(
        {
            x: convert(x, src="ISO3", to="name_short")
            for x in ["FRA", "DEU", "ITA", "GBR", "USA", "JPN", "CAN"]
        }
    )


def income_levels() -> dict:
    return Dict(__get_income_levels())


def life_expectancy() -> dict:
    return Dict(
        __wb().get_data("SP.DYN.LE00.IN").set_index("iso_code")["value"].to_dict()
    )


def population_density() -> dict:
    return Dict(__wb().get_data("EN.POP.DNST").set_index("iso_code")["value"].to_dict())


def population() -> dict:
    return Dict(__wb().get_data("SP.POP.TOTL").set_index("iso_code")["value"].to_dict())


def dac_codes() -> dict:
    return Dict(__get_dac_codes())


def flourish_geometries() -> dict:
    return Dict(__read_flourish_geometries())
