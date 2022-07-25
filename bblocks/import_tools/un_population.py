from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import KeysView, Optional

import pandas as pd

from bblocks import config
from bblocks.import_tools.common import ImportData
from bblocks.import_tools.unzip import read_zipped_csv

AVAILABLE_INDICATORS = {
    'total_population_5y_medium': 'Population1JanuaryByAge5GroupSex_Medium',
    'total_population_5y_other': 'Population1JanuaryByAge5GroupSex_OtherVariants',
    'total_population_1y_historical_medium': 'Population1JanuaryBySingleAgeSex_Medium_1950-',
    'total_population_1y_projection_medium': 'Population1JanuaryBySingleAgeSex_Medium_',
    'demographic_indicators_medium': 'Demographic_Indicators_Medium',
    'demographic_indicators_other': 'Demographic_Indicators_OtherVariants',
}

BASE_URL: str = "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/"


def _gen_filename(indicator: str, year: int) -> str:
    """Generate the file name for the given year"""

    suffix = ''

    if indicator == "total_population_1y_historical_medium":
        suffix = f"{year - 1}"

    if indicator == "total_population_1y_projection_medium":
        suffix = f"{year}-2100"

    return f"WPP{year}_{AVAILABLE_INDICATORS[indicator]}{suffix}"


def _gen_url(file_name: str) -> str:
    """Generate the url for the given year"""

    return f"{BASE_URL}{file_name}.zip"


def _download_data(indicator: str) -> None:
    """Downloads population data from the UN Population Division"""

    # Loop through recent years to get a working link
    for year in range(datetime.now().year, 2016, -1):
        try:
            print(f"Attempting to download {indicator} ({year})")

            file_name = _gen_filename(indicator=indicator, year=year)
            url = _gen_url(file_name=file_name)

            df = read_zipped_csv(url=url, path=f"{file_name}.csv")

            df.to_csv(f"{config.PATHS.imported_data}/{file_name}.csv", index=False)
            print(f"Downloaded {file_name}")

            return

        except Exception as e:
            print(f"Error downloading {year}: {e}")
            continue


@dataclass
class UNPopulationData(ImportData):
    """An object to help download data from the UN Population Division website.
    In order to use, create an instance of this class.
    Then, call the load_indicator method to load an indicator. This can be done multiple times.
    If the data for an indicator has never been downloaded, it will be downloaded.
    If it has been downloaded, it will be loaded from disk.
    If `update_data` is set to True when creating the object, the data will be updated
    from the UN Population Website for each indicator.
    You can force an update by calling `update` if you want to refresh the data stored on disk.
    You can get a dataframe of the data by calling `get_data`."""

    @property
    def available_indicators(self) -> KeysView:
        return AVAILABLE_INDICATORS.keys()

    def load_indicator(self, indicator: str) -> ImportData:

        file_name = f"{_gen_filename(indicator=indicator, year=datetime.now().year)}.csv"

        if not os.path.exists(f"{config.PATHS.imported_data}/{file_name}") or self.update_data:
            _download_data(indicator=indicator)

        self.indicators[indicator] = pd.read_csv(
            f"{config.PATHS.imported_data}/{file_name}",
            low_memory=False,
        ).assign(indicator=indicator)

        return self

    def update(self) -> ImportData:
        if len(self.indicators) == 0:
            raise RuntimeError("No indicators loaded")

        for indicator_, _ in self.indicators.items():
            _download_data(indicator=indicator_)
            self.load_indicator(indicator=indicator_)

        return self

    def get_data(self, indicators: Optional | str | list = "all") -> pd.DataFrame:
        """
        Get the data as a Pandas DataFrame
        Args:
            indicators: By default, all indicators are returned in a single DataFrame.
                If a list of indicators is passed, only those indicators will be returned.
                A single indicator can be passed as a string as well.

        Returns:
            A Pandas DataFrame with the data for the indicators requested.

        """

        df = pd.DataFrame()

        if indicators != "all" and isinstance(indicators, str):
            indicators = [indicators]

        if isinstance(indicators, list):
            indicators = [self.indicators[_] for _ in indicators if _ in list(self.indicators)]

        elif indicators == "all":
            indicators = self.indicators.values()

        for indicator in indicators:
            df = pd.concat([df, indicator], ignore_index=True)
            print(f"Loaded {indicator}")

        self.data = df
        return self.data
