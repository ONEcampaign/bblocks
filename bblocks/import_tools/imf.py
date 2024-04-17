from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from bblocks import config
from bblocks.cleaning_tools.clean import clean_numeric_series, convert_to_datetime
from bblocks.import_tools.common import ImportData
from bblocks.import_tools.imf_weo import WEO
from bblocks.logger import logger


def _check_parameters(latest_y: int | None, latest_r: int | None) -> str | tuple:
    if latest_y is None and latest_r is None:
        release = "latest"

    else:
        release = (latest_y, latest_r)

    return release


def _update_weo(latest_y: int = None, latest_r: int = None) -> None:
    """Update _data from the World Economic Outlook, using WEO package"""

    release = _check_parameters(latest_y, latest_r)

    # Download the file from the IMF website and store in directory
    WEO(release).update_data()


@dataclass
class WorldEconomicOutlook(ImportData):
    """World Economic Outlook _data"""

    year: Optional[int] = None
    release: Optional[int] = None

    def __repr__(self) -> str:
        return f"IMF WEO(year={self.year}, release={self.release})"

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

        names = {
            "ISO": "iso_code",
            "WEO Subject Code": "indicator",
            "Subject Descriptor": "indicator_name",
            # "Country/Series-specific Notes": "indicator_description",
            "Units": "units",
            "Scale": "scale",
            "Estimates Start After": "estimates_start_after",
        }

        to_drop = [
            "WEO Country Code",
            "Country",
            "Country/Series-specific Notes",
        ]

        release = _check_parameters(latest_y, latest_r)
        df = WEO(version=release).load_data().get_old_format_data()

        # Load _data into _data object
        self._raw_data = (
            df.drop(
                columns=to_drop,
            )
            .rename(columns=names)
            .melt(id_vars=names.values(), var_name="year", value_name="value")
            .assign(year=lambda d: convert_to_datetime(d.year))
            .dropna(subset=["value"])
            .reset_index(drop=True)
        )

    def _check_indicators(self, indicators: str | list | None = None) -> None | dict:
        if self._raw_data is None:
            self.__load_data()

        # Create dictionary of available indicators
        indicators_ = (
            self._raw_data.drop_duplicates(
                subset=["indicator", "indicator_name", "units"]
            )
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

    def load_data(self, indicator: str | list) -> ImportData:
        """Loads a specific indicator from the World Economic Outlook _data"""

        def __load_indicator(ind_: str) -> None:
            self._data[ind_] = (
                self._raw_data.query(f"indicator == '{ind_}'")
                .assign(
                    estimate=lambda d: d.apply(
                        lambda r: (
                            True if r.year.year >= r.estimates_start_after else False
                        ),
                        axis=1,
                    ),
                )
                .drop(columns=["estimates_start_after"])
                .sort_values(["iso_code", "year"])
                .reset_index(drop=True)
            )
            logger.info(f"Loaded indicator: {ind_}")

        if isinstance(indicator, str):
            indicator = [indicator]

        # Check if indicator exists
        [self._check_indicators(indicators=i_) for i_ in indicator]

        # Load indicator(s)
        [__load_indicator(ind_) for ind_ in indicator]

        return self

    def update_data(
        self, year: int | None, release: int | None, reload_data: bool = True
    ) -> None:
        """Update the stored WEO _data, using WEO package.

        Args:
        """
        _update_weo(latest_y=year, latest_r=release)

        # Reset the _data
        self._raw_data = None
        self._data = {}

        logger.info("WEO data updated.")

        if reload_data:
            self.load_data(indicator=list(self._data.keys()))

    def available_indicators(self) -> None:
        """Print the available indicators in the dataset"""

        indicators_ = self._check_indicators(indicators=None)

        available = [f"{code}: {name} \n" for code, name in indicators_.items()]

        print(f"Available indicators:\n{''.join(available)}")

    def get_data(
        self, indicators: str | list = "all", keep_metadata: bool = False
    ) -> pd.DataFrame:
        df = super().get_data(indicators=indicators)

        if not keep_metadata:
            return df.filter(["iso_code", "name", "indicator", "year", "value"], axis=1)

        return df
