""" """

import xml.etree.ElementTree as ET
from zipfile import ZipFile
from dataclasses import dataclass
import pandas as pd
from datetime import datetime
import calendar
from bs4 import BeautifulSoup
import io

from bblocks.import_tools.common import ImportData, get_response
from bblocks.config import BBPaths
from bblocks.logger import logger

BASE_URL = "https://www.imf.org/"

COLUMN_MAPPER = {
    "UNIT": "IMF.CL_WEO_UNIT.1.0",
    "CONCEPT": "IMF.CL_WEO_CONCEPT.1.0",
    "REF_AREA": "IMF.CL_WEO_REF_AREA.1.0",
    "FREQ": "IMF.CL_FREQ.1.0",
    "SCALE": "IMF.CL_WEO_SCALE.1.0",
}


def get_smdx_href(version: tuple[int, int]) -> str | None:
    """retrieve the href for the SDMX file"""

    if version[1] == 1:
        month = 'April'
    elif version[1] == 2:
        month = 'October'
    else:
        raise ValueError('invalid version. Must be 1 or 2')

    url = (
        f"{BASE_URL}/en/Publications/WEO/weo-database/"
        f"{version[0]}/{month}/download-entire-database"
    )

    response = get_response(url)
    soup = BeautifulSoup(response.content, "html.parser")

    # check is data exists
    if soup.find("a", string="SDMX Data"):
        return soup.find_all("a", string="SDMX Data")[0].get("href")


def get_folder(href: str) -> ZipFile:
    """Extract the folder containing data and schema"""

    # TODO add to common with error handling (remove unzip)

    try:
        response = get_response(BASE_URL + href)
        return ZipFile(io.BytesIO(response.content))
    except ConnectionError:
        raise ConnectionError("invalid url")


@dataclass
class Parser:
    """Helper class to download WEO"""

    folder: ZipFile

    data_file = None
    schema_file = None
    data = None

    def get_files(self):
        """Get the root of files"""

        if len(self.folder.namelist()) > 2:
            raise ValueError("More than one file in zip file")

        for file in self.folder.namelist():
            if file.endswith(".xml"):
                self.data_file = ET.parse(self.folder.open(file)).getroot()

            if file.endswith(".xsd"):
                self.schema_file = ET.parse(self.folder.open(file)).getroot()

        if self.data_file is None or self.schema_file is None:
            raise ValueError("No data or schema file found")

    def extract_data(self):
        """extract data from data file"""

        rows = []
        for series in self.data_file[1].findall("./"):
            for obs in series.findall("./"):
                rows.append({**series.attrib, **obs.attrib})

        self.data = pd.DataFrame(rows)

    def _convert_series_codes(self, series: pd.Series, lookup_value: str):
        """ """

        query = self.schema_file.findall(
            "./{http://www.w3.org/2001/XMLSchema}simpleType[@name="
            + f'"{lookup_value}"'
            + "]/"
        )[0].findall("./")

        return series.map({elem.attrib["value"]: elem[0][0].text
                           for elem in query})

    def clean_data(self):
        """Format the dataframe"""

        for col, value in COLUMN_MAPPER.items():
            self.data = self.data.rename(columns={col: f"{col}_CODE"})
            self.data[col] = self._convert_series_codes(self.data[f"{col}_CODE"], value)

        for col in ['REF_AREA_CODE', 'LASTACTUALDATE', 'TIME_PERIOD', 'OBS_VALUE']:
            self.data[col] = pd.to_numeric(self.data[col], errors='coerce')

    def parse_data(self) -> pd.DataFrame:
        """Parse the data to a dataframe"""

        self.get_files()
        self.extract_data()
        self.clean_data()

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
    # return current month and version 2 (October

    else:
        return current_year, 2


def roll_back_version(version: tuple[int, int]) -> tuple[int, int]:
    """Roll back version to the previous version"""

    if version[1] == 2:
        return version[0], 1

    elif version[1] == 1:
        return version[0] - 1, 2

    else:
        raise ValueError(f'Release must be either 1 or 2. Invalid release: {version[1]}')


@dataclass
class WEO(ImportData):
    """
    """

    version: tuple[int, int] = 'latest'

    @property
    def _path(self):
        """Generate path based on version"""
        return BBPaths.raw_data / f"weo_{self.version[0]}_{self.version[1]}.csv"

    def _download_latest_data(self) -> None:
        """Get data for latest release, and replace release with latest"""

        href = None  # set href to none
        # search for the latest, until data is found
        while href is None:
            href = get_smdx_href(self.version)
            if href is None:
                # roll back version
                self.version = roll_back_version(self.version)

        # parse data and save to disk
        folder = get_folder(href)
        Parser(folder).parse_data().to_csv(self._path, index=False)

    def _download_data(self) -> None:
        """Get data for the version, raise error is no data for version is found"""

        href = get_smdx_href(self.version)
        if href is None:
            raise ValueError(f'No data found for version:'
                             f' {self.version[0]}, {self.version[1]}')

        # parse data and save to disk
        folder = get_folder(href)
        Parser(folder).parse_data().to_csv(self._path, index=False)

    def load_data(self, indicators: str | list = 'all') -> ImportData:
        """ """

        # check if latest is requested
        if self.version == 'latest':
            self.version = gen_latest_version()  # set latest expected version
            if not self._path.exists():  # check if not downloaded
                self._download_latest_data()  # find latest and download data

        else:
            if not self._path.exists():  # check if not downloaded
                self._download_data()  # download data

        # check if raw data is not loaded to object
        if self._raw_data is None:
            self._raw_data = pd.read_csv(self._path)

        # load indicators
        if indicators == 'all':
            indicators = self._raw_data['CONCEPT_CODE'].unique()
        if isinstance(indicators, str):
            indicators = [indicators]

        self._data.update(
            {indicator: (self._raw_data[self._raw_data['CONCEPT_CODE'] == indicator]
                         .reset_index(drop=True)
                         )
             for indicator in indicators
             }
        )
        logger.info("Data loaded successfully")
        return self

    def update_data(self, reload_data: bool = True):
        """ """
        if len(self._data) == 0:
            raise RuntimeError('No indicators loaded')

        self._download_data()
        self._raw_data = pd.read_csv(self._path)
        if reload_data:
            self.load_data(indicators=self._raw_data['CONCEPT_CODE'])
