"""Tools to import IMF World Economic Outlook data."""

from dataclasses import dataclass

import numpy as np
import pandas as pd
from imf_reader.weo import fetch_data

from bblocks.cleaning_tools import clean
from bblocks.config import BBPaths
from bblocks.import_tools.common import ImportData
from bblocks.logger import logger

BASE_URL = "https://www.imf.org/"

COLUMN_MAPPER = {
    "UNIT": "IMF.CL_WEO_UNIT.1.0",
    "CONCEPT": "IMF.CL_WEO_CONCEPT.1.0",
    "REF_AREA": "IMF.CL_WEO_REF_AREA.1.0",
    "FREQ": "IMF.CL_FREQ.1.0",
    "SCALE": "IMF.CL_WEO_SCALE.1.0",
}


@dataclass
class WEO(ImportData):
    """An object to extract WEO data from the IMF website using SDMX
    To use, create an instance of the class setting the version to the desired version,
    or leave blank to get the latest version.
    Call the load_data method to load the data to the object. If the data is already
    downloaded, it will load from disk, otherwise it will download the data.
    To force an update of the data, call the update_data method.
    To retrieve the data, call the get_data method.

    Attributes:
        version: tuple of (year, release) or "latest". Default is "latest"
    """

    version = "latest"

    def __post_init__(self):
        """check that version is valid"""

        if (self.version != "latest") and not (isinstance(self.version, tuple)):
            raise ValueError(
                'Invalid version. Must be a tuple of (year, release) or "latest"'
            )

    @property
    def _path(self):
        """Generate path based on version"""
        return BBPaths.raw_data / f"weo_{self.version[0]}_{self.version[1]}.feather"

    def _download_data(self) -> None:
        """Downloads latest data or data for specified version if not already available in disk

        If version is "latest" it will generate the expected version based on the current date
        and try download the data. If it is not available for the expected version, it will
        roll back the version and try again. If no data is found, it will raise an error.

        If version is a tuple of (year, release), it will try download the data for that version
        """

        if self.version == "latest":
            self.version = None

        # if the path does not exist yet, try download the data

        df = fetch_data(self.version)

        # if data is not None, save to disk
        if df is not None:
            df.to_feather(self._path)
            logger.info(f"Data downloaded to disk for version {self.version}")

    def _clean_data(self) -> None:
        """Clean and format the dataframe"""

        self._raw_data = clean.clean_numeric_series(
            self._raw_data,
            series_columns=[
                "REF_AREA_CODE",
                "LASTACTUALDATE",
                "TIME_PERIOD",
            ],
            to=int,
        )

        self._raw_data.columns = self._raw_data.columns.str.lower()

    def load_data(self, indicators: str | list = "all") -> ImportData:
        """Load data to object

        When called this method will load WEO indicators to the object.
        If the data is not downloaded, it will download the data. If the data is
        already downloaded, it will load the data from disk. If the version is set to
        'latest', it will find the latest version and download the data.

        Args:
            indicators: The indicators to load. If 'all' is passed, all indicators will be loaded. Default is 'all'

        Returns:
            same object with data loaded
        """

        # download data if it doesn't exist
        self._download_data()

        # check if raw data is not loaded to object
        if self._raw_data is None:
            self._raw_data = pd.read_feather(self._path)

        self._clean_data()

        # load indicators
        if indicators == "all":
            indicators = self._raw_data["concept_code"].unique()
        if isinstance(indicators, str):
            indicators = [indicators]

        for indicator in indicators:
            if indicator not in self._raw_data["concept_code"].unique():
                logger.debug(f"Indicator not found in data: {indicator}")
            else:
                self._data[indicator] = self._raw_data[
                    self._raw_data["concept_code"] == indicator
                ].reset_index(drop=True)

        logger.info("Data loaded to object")
        return self

    def update_data(self, reload_data: bool = True) -> ImportData:
        """Update the data

        When called it will update the data used to load the indicators.
        If reload_data is True, it will reload the indicators to the object.

        Args:
            reload_data: If True, reload the indicators after updating the data. Default is True.

        Returns:
            same object with updated data to allow chaining
        """

        self._download_data()

        # load raw data to object
        self._raw_data = pd.read_feather(self._path)

        # reload data if reload_data is True
        if reload_data:
            self.load_data(indicators=list(self._raw_data["concept_code"].unique()))

        logger.info("Data updated successfully")
        return self

    def available_indicators(self) -> dict[str, str]:
        """Returns a DataFrame of available indicators to load to the object"""

        if self._raw_data is None:
            self._download_data()
            self._raw_data = pd.read_feather(self._path)

        return (
            self._raw_data.loc[:, ["concept_code", "concept"]]
            .drop_duplicates()
            .set_index("concept_code")["concept"]
            .to_dict()
        )

    def get_old_format_data(self) -> pd.DataFrame:
        """This function returns the data in the old format that weo-reader returns

        NOTE: This will return all the data in the object in the old format, regardless of the indicators loaded
        Not all columns that existed in weo-reader are returned as they don't exist in the sdmx data files. However,
        this should not cause issues are they are metadata columns not used in analysis.
        """

        logger.warning(
            "This method is a temporary fix used to patch the output format that weo-reader returns. "
            "It will be removed in the future."
        )

        col_mapper = {
            "concept_code": "WEO Subject Code",
            "ref_area_code": "WEO Country Code",
            "lastactualdate": "Estimates Start After",
            "notes": "Country/Series-specific Notes",
            "unit_label": "Units",
            "concept_label": "Subject Descriptor",
            "ref_area_label": "Country",
            "scale_label": "Scale",
        }

        return (
            self.get_data()
            .rename(columns=col_mapper)
            .pivot(index=col_mapper.values(), columns="time_period", values="obs_value")
            .reset_index()
            .assign(ISO=lambda d: clean.convert_id(d.Country, not_found=np.nan))
            .dropna(subset="ISO")
            .reset_index(drop=True)
        )


if __name__ == "__main__":
    weo = WEO()
    weo.available_indicators()
    weo.load_data()
    weo.update_data()