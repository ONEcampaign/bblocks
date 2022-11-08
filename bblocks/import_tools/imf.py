from __future__ import annotations

import pandas as pd
import requests
from datetime import datetime

import weo.dates
from bs4 import BeautifulSoup
import os
from typing import Optional

from weo import all_releases, download, WEO

from bblocks.cleaning_tools.clean import clean_numeric_series

from bblocks.import_tools.common import ImportData


def _check_weo_parameters(
    latest_y: int | None = None, latest_r: int | None = None
) -> (int, int):
    """Check parameters and return max values or provided input"""
    if latest_y is None:
        latest_y = max(*all_releases())[0]

    # if latest release isn't provided, take max value
    if latest_r is None:
        latest_r = max(*all_releases())[1]

    return latest_y, latest_r


def _update_weo(
    latest_y: int = None,
    latest_r: int = None,
    data_path: str = None,
) -> None:
    """Update data from the World Economic Outlook, using WEO package"""

    if data_path is None:
        raise ValueError("Please specify a data path")

    if data_path[-1] == "/":
        data_path = data_path[:-1]

    latest_y, latest_r = _check_weo_parameters(latest_y, latest_r)

    # Download the file from the IMF website and store in directory
    download(
        latest_y,
        latest_r,
        directory=data_path,
        filename=f"weo{latest_y}_{latest_r}.csv",
    )

    # Validate the file
    if os.path.getsize(f"{data_path}/weo{latest_y}_{latest_r}.csv") < 1000:
        print(
            f"Downloading release {latest_r} of "
            f"{latest_y} failed. Trying previous release"
        )
        os.remove(f"{data_path}/weo{latest_y}_{latest_r}.csv")

        try:
            _update_weo(latest_y, latest_r - 1, data_path=data_path)
        except weo.dates.DateError:
            _update_weo(latest_y - 1, latest_r, data_path=data_path)


class WorldEconomicOutlook(ImportData):
    """World Economic Outlook data"""

    year: Optional[int] = None
    release: Optional[int] = None

    def __load_data(
        self, latest_y: int | None = None, latest_r: int | None = None
    ) -> None:
        """loading WEO as a clean dataframe

        Args:
            latest_y: passed only optional to override the behaviour to get the latest
            release year for the WEO.
            latest_r: passed only optionally to override the behaviour to get the latest
            released value (1 or 2).
        """

        latest_y, latest_r = _check_weo_parameters(latest_y, latest_r)

        names = {
            "ISO": "iso_code",
            "WEO Subject Code": "indicator",
            "Subject Descriptor": "indicator_name",
            "Subject Notes": "indicator_description",
            "Units": "units",
            "Scale": "scale",
            "Estimates Start After": "estimates_start_after",
        }

        to_drop = [
            "WEO Country Code",
            "Country",
            "Country/Series-specific Notes",
        ]

        # If data doesn't exist or update is required, update the data
        if (
            not os.path.exists(f"{self.data_path}/weo{latest_y}_{latest_r}.csv")
            or self.update_data
        ):
            _update_weo(latest_y, latest_r, data_path=self.data_path)

        # Load the data from disk. If it doesn't exist, try the previous one
        try:
            df = WEO(f"{self.data_path}/weo{latest_y}_{latest_r}.csv").df
            self.version = {"year": latest_y, "release": latest_r}
        except FileNotFoundError:
            try:
                df = WEO(f"{self.data_path}/weo{latest_y}_{latest_r-1}.csv").df
                self.version = {"year": latest_y, "release": latest_r - 1}
            except FileNotFoundError:
                df = WEO(f"{self.data_path}/weo{latest_y-1}_{latest_r}.csv").df
                self.version = {"year": latest_y - 1, "release": latest_r}

        # Load data into data object
        self.data = (
            df.drop(to_drop, axis=1)
            .rename(columns=names)
            .melt(id_vars=names.values(), var_name="year", value_name="value")
            .assign(
                year=lambda d: pd.to_datetime(d.year, format="%Y"),
                value=lambda d: clean_numeric_series(d.value),
            )
            .dropna(subset=["value"])
            .reset_index(drop=True)
        )

    def _check_indicators(self, indicators: str | list | None = None) -> None | dict:

        if self.data is None:
            self.__load_data()

        # Create dictionary of available indicators
        indicators_ = (
            self.data.drop_duplicates(subset=["indicator", "indicator_name", "units"])
            .assign(name_units=lambda d: d.indicator_name + " (" + d.units + ")")
            .set_index("indicator")["name_units"]
            .to_dict()
        )

        if indicators is None:
            return indicators_

        if isinstance(indicators, str):
            indicators = [indicators]

        for _ in indicators:
            if _ not in indicators_:
                self.available_indicators()
                raise ValueError(f"Indicator not found: {_}")

    def load_indicator(
        self, indicator_code: str, indicator_name: Optional[str] = None
    ) -> ImportData:
        """Loads a specific indicator from the World Economic Outlook data"""

        if self.data is None:
            self.__load_data()

        # Check if indicator exists
        self._check_indicators(indicators=indicator_code)

        self.indicators[indicator_code] = (
            self.data.loc[lambda d: d.indicator == indicator_code]
            .assign(
                indicator_name=indicator_name
                if indicator_name is not None
                else lambda d: d.indicator_name,
                estimate=lambda d: d.apply(
                    lambda r: True if r.year.year >= r.estimates_start_after else False,
                    axis=1,
                ),
            )
            .drop(columns=["estimates_start_after"])
            .sort_values(["iso_code", "year"])
            .reset_index(drop=True)
        )
        return self

    def update(self, latest_y: int | None = None, latest_r: int | None = None) -> None:
        """Update the stored WEO data, using WEO package.

        Args:
            latest_y: passed only optionally to override the behaviour to get the latest
                release year for the WEO.
            latest_r: passed only optionally to override the behaviour to get the latest
                released value (1 or 2).
        """
        _update_weo(latest_y=latest_y, latest_r=latest_r, data_path=self.data_path)

    def available_indicators(self) -> None:
        """Print the available indicators in the dataset"""

        indicators_ = self._check_indicators(indicators=None)

        available = [f"{code}: {name} \n" for code, name in indicators_.items()]

        print(f"Available indicators:\n{''.join(available)}")

    def get_data(
        self, indicators: str | list = "all", keep_metadata: bool = False
    ) -> pd.DataFrame:

        df = _get_data(obj=self, indicators=indicators)

        if not keep_metadata:
            return df.filter(["iso_code", "name", "indicator", "year", "value"], axis=1)

        return df


