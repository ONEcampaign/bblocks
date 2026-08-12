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

from bblocks.importers.utilities import convert_dtypes
from bblocks.importers.config import (
    logger,
    weo_version,
    Fields,
    DataExtractionError,
    DataFormattingError,
)
from bblocks.importers.data_validators import DataFrameValidator


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

    def __init__(self, *, legacy_entity_codes: bool = False):
        """Instantiate a WEO importer.

        Args:
            legacy_entity_codes: migration aid for pipelines keyed on the old
                numeric IMF area codes. When True, `entity_code` is populated
                from the legacy numeric code (imf-reader's `REF_AREA_IMF_CODE`,
                also exposed here as `imf_code`) instead of the ISO3 code.
                Default is False, meaning `entity_code` is ISO3. This is
                temporary: imf-reader documents `REF_AREA_IMF_CODE` as a
                compatibility column slated for removal in its 3.0 release,
                at which point this flag will stop working. A small number of
                entities, such as Liechtenstein, never had a legacy numeric
                code. For those, `entity_code` is null under this flag, and a
                warning names the affected entities.
        """
        self._data: dict = {}
        self._latest_version = None
        self._legacy_entity_codes = legacy_entity_codes

    def __repr__(self) -> str:
        """String representation of the WEO object"""

        imported = list(self._data.keys())
        return f"{self.__class__.__name__}(imported versions = {imported!r})"

    def _format_data(self, df: pd.DataFrame):
        """Format WEO data"""

        df = (
            df.pipe(convert_dtypes)
            .rename(
                columns={
                    "OBS_VALUE": Fields.value,
                    "TIME_PERIOD": Fields.year,
                    "REF_AREA_CODE": Fields.entity_code,
                    "REF_AREA_IMF_CODE": Fields.imf_code,
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

        if self._legacy_entity_codes:
            if Fields.imf_code not in df.columns:
                raise ValueError(
                    "legacy_entity_codes=True requires the legacy IMF numeric "
                    f"area code column, but imf-reader did not return one "
                    f"(no '{Fields.imf_code}' column after formatting). This "
                    "flag is a migration aid tied to imf-reader's "
                    "REF_AREA_IMF_CODE column, which its docstring marks as "
                    "slated for removal; if it has been dropped, switch to "
                    "the default ISO3 entity_code."
                )
            df[Fields.entity_code] = df[Fields.imf_code]

            null_entities = df.loc[
                df[Fields.entity_code].isna(), Fields.entity_name
            ].unique()
            if len(null_entities) > 0:
                logger.warning(
                    "legacy_entity_codes=True: no legacy IMF numeric code for "
                    f"{', '.join(sorted(null_entities))}. entity_code is null "
                    "for these rows."
                )

        return df

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
