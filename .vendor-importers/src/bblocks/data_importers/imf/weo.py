"""Importer for the WEO database from IMF

The World Economic Outlook (WEO) database provides data on the global economy and its prospects
for countries and regions, over time, and includes estimates and forecasts for the years ahead.
The report and data is published twice a year in April and October.

More information and access to the raw data can be found at: https://www.imf.org/en/Publications/WEO


This importer provides functionality to easily access the latest WEO data (or data from a specific version).

Usage:

First instantiate an importer object:
>>> weo = WEO()

get the latest data:
>>> data = weo.get_data()

To get the data for a specific version, pass the version as an argument:
>>> data = weo.get_data(version=("April", 2023))

The data is cached to avoid downloading it multiple times. To clear the cache, call:
>>> weo.clear_cache()
"""

import pandas as pd
from imf_reader import weo

from bblocks.data_importers.utilities import convert_dtypes
from bblocks.data_importers.config import (
    logger,
    weo_version,
    Fields,
    DataExtractionError,
    DataFormattingError,
)
from bblocks.data_importers.data_validators import DataFrameValidator


class WEO:
    """Importer for the WEO database

    The World Economic Outlook (WEO) database provides data on the global economy and its prospects
    for countries and regions, over time, and includes estimates and forecasts.
    The report and data is published twice a year in April and October.
    See more details at: https://www.imf.org/en/Publications/WEO

    Usage:

    First instantiate an importer object:
    >>> weo = WEO()

    get the latest data:
    >>> data = weo.get_data()

    To get the data for a specific version, pass the version as an argument:
    >>> data = weo.get_data(version=("April", 2023))

    The data is cached to avoid downloading it multiple times. To clear the cache, call:
    >>> weo.clear_cache()
    """

    def __init__(self):
        self._data: dict = {}
        self._latest_version = None

    def __repr__(self) -> str:
        """String representation of the WEO object"""

        imported = list(self._data.keys())
        return f"{self.__class__.__name__}(" f"imported versions = {imported!r}" f")"

    @staticmethod
    def _format_data(df: pd.DataFrame):
        """Format WEO data"""

        return (
            df.pipe(convert_dtypes).rename(
                columns={
                    "OBS_VALUE": Fields.value,
                    "TIME_PERIOD": Fields.year,
                    "REF_AREA_CODE": Fields.entity_code,
                    "REF_AREA_LABEL": Fields.entity_name,
                    "CONCEPT_CODE": Fields.indicator_code,
                    "CONCEPT_LABEL": Fields.indicator_name,
                    "UNIT_LABEL": Fields.unit,
                    "LASTACTUALDATE": "last_actual_date",
                }
            )
            # convert other columns to lowercase
            .rename(columns={col: col.lower() for col in df.columns})
        )

    def _load_data(self, version=None) -> None:
        """Load WEO data to the object for a specific version

        Args:
            version: version of the WEO data to load. If None, the latest version is loaded
        """

        try:
            df = weo.fetch_data(version)  # fetch the data
        except Exception as e:
            raise DataExtractionError(f"Failed to fetch data: {e}")

        try:
            df = self._format_data(df)  # format the data
        except Exception as e:
            raise DataFormattingError(f"Error formatting data: {e}")

        DataFrameValidator().validate(
            df,
            required_cols=[
                Fields.value,
                Fields.year,
                Fields.entity_code,
                Fields.indicator_code,
            ],
        )  # validate the data

        self._data[weo.fetch_data.last_version_fetched] = df

        # if the latest version is loaded, save the version to _latest_version
        if version is None:
            self._latest_version = weo.fetch_data.last_version_fetched

    def get_data(self, version: weo_version = "latest") -> pd.DataFrame:
        """Get the WEO data for a specific version

        Args:
            version: version of the WEO data to get. If "latest", the latest version is returned.
                    If another version is required, pass a tuple with the month and year of the version.
                    WEO releases data in April and October each year.

        Returns:
            The WEO data for the specified version
        """

        if version == "latest":
            if self._latest_version is not None:
                return self._data[self._latest_version]
            else:
                self._load_data()
                return self._data[self._latest_version]

        if version not in self._data:
            self._load_data(version)
            return self._data[version]

        return self._data[version]

    def clear_cache(self):
        """Clear the data cached in the importer"""

        self._latest_version = None  # clear the latest version
        self._data = {}  # clear the data
        logger.info("Cache cleared")
