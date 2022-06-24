from dataclasses import dataclass, field
from country_converter import convert
import pandas as pd

from package.config import PATHS
import os
from package.import_tools.world_bank import WorldBankData


@dataclass(repr=False)
class Dict(dict):
    dictionary: dict = field(default_factory=dict)

    """A wrapper that adds functionality to a standard dictionary"""

    def __post_init__(self):
        self.update(self.dictionary)

    def change_keys(self, from_: str = None, to: str = "ISO3") -> dict:
        _ = {
            convert(names=key, src=from_, to=to, not_found=None): value
            for key, value in self.dictionary.items()
        }
        self.clear()
        self.update(_)
        return self

    def reverse(self) -> dict:
        _ = {value: key for key, value in self.items()}
        self.clear()
        self.update(_)
        return self

    def set_keys_type(self, type_: type) -> dict:
        _ = {type_(key): value for key, value in self.dictionary.items()}
        self.clear()
        self.update(_)
        return self

    def set_values_type(self, type_: type) -> dict:
        _ = {key: type_(value) for key, value in self.dictionary.items()}
        self.clear()
        self.update(_)
        return self


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


g20: Dict = Dict(
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
            "EU",
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

df = pd.DataFrame({"iso_code": ["FRA", "DEU", "ITA"]})
df["population"] = df.iso_code.map(population)
