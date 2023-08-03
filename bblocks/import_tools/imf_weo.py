"""Tools to import IMF World Economic Outlook data."""

import xml.etree.ElementTree as ET
from zipfile import ZipFile
from dataclasses import dataclass
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import io
import requests

from bblocks.import_tools.common import ImportData, get_response, unzip
from bblocks.config import BBPaths
from bblocks.logger import logger
from bblocks.cleaning_tools import clean


BASE_URL = "https://www.imf.org/"

COLUMN_MAPPER = {
    "UNIT": "IMF.CL_WEO_UNIT.1.0",
    "CONCEPT": "IMF.CL_WEO_CONCEPT.1.0",
    "REF_AREA": "IMF.CL_WEO_REF_AREA.1.0",
    "FREQ": "IMF.CL_FREQ.1.0",
    "SCALE": "IMF.CL_WEO_SCALE.1.0",
}


def _smdx_query_url(version: tuple[int, int]) -> str:
    """Generate the url for the SDMX query"""

    if version[1] == 1:
        month = "April"
    elif version[1] == 2:
        month = "October"
    else:
        raise ValueError("invalid version. Must be 1 or 2")

    return (
        f"{BASE_URL}/en/Publications/WEO/weo-database/"
        f"{version[0]}/{month}/download-entire-database"
    )


def _parse_sdmx_query_response(content: requests.Response.content) -> str:
    """Parse the response from the SDMX query"""
    soup = BeautifulSoup(content, "html.parser")

    # check is data exists
    if soup.find("a", string="SDMX Data"):
        return soup.find_all("a", string="SDMX Data")[0].get("href")


def get_sdmx_href(version: tuple[int, int]) -> str | None:
    """retrieve the href for the SDMX file

    Args:
        version (tuple[int, int]): the version of the WEO data as a tuple of (year, version)
    """

    url = _smdx_query_url(version)
    response = get_response(url)
    return _parse_sdmx_query_response(response.content)


class Parser:
    """Helper class to parse WEO data

    This class parses the data and schema files into a clean dataframe
    to be used in the main WEO class

    Parameters:
        folder: the folder containing the data and schema files
    """

    def __init__(self, folder: ZipFile):
        self.folder: ZipFile = folder

        self.data_file: ET = None
        self.schema_file: ET = None
        self.data: pd.DataFrame | None = None

    def get_files(self) -> None:
        """Get the root of files"""

        if len(self.folder.namelist()) > 2:
            raise ValueError("More than two files in zip file")

        for file in self.folder.namelist():
            if file.endswith(".xml"):
                self.data_file = ET.parse(self.folder.open(file)).getroot()

            if file.endswith(".xsd"):
                self.schema_file = ET.parse(self.folder.open(file)).getroot()

        if self.data_file is None or self.schema_file is None:
            raise ValueError("No data or schema file found")

    def _extract_data(self) -> None:
        """extract data from data file"""

        rows = []
        for series in self.data_file[1]:
            for obs in series:
                rows.append({**series.attrib, **obs.attrib})

        self.data = pd.DataFrame(rows)

    def _convert_series_codes(self, series: pd.Series, lookup_value: str) -> pd.Series:
        """Converts a series of codes to the corresponding values in the schema file"""

        xpath_expr = f"./{{http://www.w3.org/2001/XMLSchema}}simpleType[@name='{lookup_value}']/*/*"
        query = self.schema_file.findall(xpath_expr)

        lookup_dict = {}
        for elem in query:
            lookup_dict[elem.attrib["value"]] = elem[0][0].text

        return series.map(lookup_dict)

    def _clean_data(self) -> None:
        """Clean and format the dataframe"""

        for col, value in COLUMN_MAPPER.items():
            self.data = self.data.rename(columns={col: f"{col}_CODE"})
            self.data[col] = self._convert_series_codes(self.data[f"{col}_CODE"], value)

        self.data = clean.clean_numeric_series(self.data, series_columns="OBS_VALUE")
        self.data = clean.clean_numeric_series(
            self.data,
            series_columns=[
                "REF_AREA_CODE",
                "LASTACTUALDATE",
                "TIME_PERIOD",
            ],
            to=int,
        )

        self.data.columns = self.data.columns.str.lower()

    def parse_data(self) -> None:
        """Parse the data to a dataframe"""

        logger.info("Parsing data")

        self.get_files()
        self._extract_data()
        self._clean_data()

    def get_data(self) -> pd.DataFrame:
        """Get the data. If the data is not already parsed, parse it first"""

        if self.data is None:
            self.parse_data()

        return self.data


def gen_latest_version() -> tuple[int, int]:
    """Generates the latest expected version based on the current date"""

    current_year = datetime.now().year
    current_month = datetime.now().month

    # if month is less than 4 (April)
    # return the version 2 (October) for the previous year
    if current_month < 4:
        current_year -= 1
        return current_year, 2

    # elif month is less than 10 (October)
    # return current year and version 2 (April)
    elif current_month < 10:
        return current_year, 1

    # else (if month is more than 10 (October)
    # return current month and version 2 (October)
    else:
        return current_year, 2


def roll_back_version(version: tuple[int, int]) -> tuple[int, int]:
    """Roll back version to the previous version"""

    if version[1] == 2:
        return version[0], 1

    elif version[1] == 1:
        return version[0] - 1, 2

    else:
        raise ValueError(
            f"Release must be either 1 or 2. Invalid release: {version[1]}"
        )


def extract_data(version) -> pd.DataFrame | None:
    """Downloads latest data or data for specified version

    Args:
        version (tuple[int, int]): version to download
    """

    logger.info(f"Extracting data for version {version}")

    href = get_sdmx_href(version)

    # if href is not None, get and parse data and save to disk
    if href is not None:
        response = get_response(BASE_URL + href)
        folder = unzip(io.BytesIO(response.content))
        return Parser(folder).get_data()

    # if href is None, log that no data was found
    else:
        logger.debug(f"No data found for version {version}")


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

    version: str | tuple[int, int] = "latest"

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
            # set version to expected latest version
            self.version = gen_latest_version()
            # if the path does not exist yet, try download the data
            if not self._path.exists():
                df = extract_data(self.version)

                # if data is not None, save to disk
                if df is not None:
                    df.to_feather(self._path)
                    logger.info(f"Data downloaded to disk for version {self.version}")
                else:
                    self.version = roll_back_version(self.version)
                    logger.debug(
                        f"Data not available for expected version. "
                        f"Rolling back version to {self.version}"
                    )

        if not self._path.exists():
            df = extract_data(self.version)
            if df is not None:
                df.to_feather(self._path)
                logger.info(f"Data downloaded to disk for version {self.version}")
            else:
                raise ValueError(f"No data found for version {self.version}")

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
        if self.version == "latest":
            # set version to expected latest version
            self.version = gen_latest_version()
            # try download the data
            df = extract_data(self.version)
            # if data is not None, save to disk, otherwise roll back version
            if df is not None:
                df.to_feather(self._path)
                logger.info(f"Data downloaded to disk for version {self.version}")

                return self

            else:
                self.version = roll_back_version(self.version)
                logger.info(
                    f"Data not available for expected version. "
                    f"Rolling back version to {self.version}"
                )

        # exctract data for a specific version or if version was rolled back
        df = extract_data(self.version)
        # if data is not None, save to disk otherwise raise error
        if df is not None:
            df.to_feather(self._path)
            logger.info(f"Data downloaded to disk for version {self.version}")
        else:
            raise ValueError(f"No data found for version {self.version}")

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
