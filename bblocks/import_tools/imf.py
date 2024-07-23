from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd
import numpy as np
from imf_reader import weo
import os

from imf_reader.weo.reader import gen_latest_version

from bblocks import config
from bblocks.cleaning_tools.clean import convert_to_datetime, convert_id
from bblocks.import_tools.common import ImportData
from bblocks.logger import logger


@dataclass
class WorldEconomicOutlook(ImportData):
    """World Economic Outlook _data"""

    year: Optional[int] = None
    release: Optional[int] = None

    # if year and release are not both None or both not None raise error
    def __post_init__(self) -> None:
        if (self.year is None and self.release is not None) or (
            self.year is not None and self.release is None
        ):
            raise ValueError(
                "Both year and release must be specified or must both be `None`"
            )

    def __repr__(self) -> str:
        return f"IMF WEO(year={self.year}, release={self.release})"

    def __load_data(self) -> None:
        """loading WEO as a clean dataframe

        Args:
            latest_y: passed only optional to override the behaviour to get the latest
            release year for the WEO.
            latest_r: passed only optionally to override the behaviour to get the latest
            released value (1 or 2).
        """

        names = {
            "CONCEPT_CODE": "indicator",
            "CONCEPT_LABEL": "indicator_name",
            "UNIT_LABEL": "units",
            "SCALE_LABEL": "scale",
            "LASTACTUALDATE": "estimates_start_after",
            "OBS_VALUE": "value",
            "TIME_PERIOD": "year",
            "REF_AREA_LABEL": "entity_name",
        }

        # If year and release are not specified, get the latest version
        if self.year is None and self.release is None:
            version = gen_latest_version()
            self.release, self.year = version

        # For compatibility, if the version is provided as int, convert
        if self.release == 1:
            version = ("April", self.year)
        elif self.release == 2:
            version = ("October", self.year)

        # Define the path where the data will be stored (or should be stored)
        path = f"{config.BBPaths.raw_data}/weo_{self.year}_{self.release}.feather"

        # try read from disk
        if os.path.exists(path):
            self._raw_data = pd.read_feather(path)
            return

        # If not found, fetch the data
        df = weo.fetch_data(version=version)

        # Check if the fetched version is the same as the requested version
        fetched_version = weo.fetch_data.last_version_fetched
        if fetched_version != version:
            self.release, self.year = version
            path = f"{config.BBPaths.raw_data}/weo_{self.year}_{self.release}.feather"

        # Load _data into _data object
        self._raw_data = (
            df.loc[:, names.keys()]
            .rename(columns=names)
            .assign(year=lambda d: convert_to_datetime(d.year))
            .assign(iso_code=lambda d: convert_id(d.entity_name, not_found=np.NaN))
            .dropna(subset=["iso_code"])
            .dropna(subset=["value"])
            .reset_index(drop=True)
        )

        # save data to disk`
        self._raw_data.to_feather(path)

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

    def update_data(self, reload_data: bool = True) -> None:
        """Update the stored WEO _data, using WEO package.

        Args:
        """
        # clear cache
        weo.clear_cache()

        # Reset the _data
        self._raw_data = None

        if reload_data:
            indicators_to_load = list(self._data.keys())
            self._data = {}
            self.load_data(indicator=indicators_to_load)

        else:
            self._data = {}

        logger.info("WEO data updated.")

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
