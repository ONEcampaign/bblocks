""" """

import xml.etree.ElementTree as ET
from zipfile import ZipFile
from dataclasses import dataclass
import pandas as pd
import datetime

from bblocks.import_tools.common import ImportData
from bblocks.config import BBPaths

BASR_URL = "https://www.imf.org/"

COLUMN_MAPPER = {
    "UNIT": "IMF.CL_WEO_UNIT.1.0",
    "CONCEPT": "IMF.CL_WEO_CONCEPT.1.0",
    "REF_AREA": "IMF.CL_WEO_REF_AREA.1.0",
    "FREQ": "IMF.CL_FREQ.1.0",
    "SCALE": "IMF.CL_WEO_SCALE.1.0",
}


@dataclass
class Downloader:
    """Helper class to download WEO"""

    folder: ZipFile
    data_file = None
    schema_file = None
    data = None

    def get_files(self):
        """Get the root of files
        """

        if len(self.folder.namelist()) > 2:
            raise ValueError("More than one file in zip file")

        for file in self.folder.namelist():
            if file.endswith(".xml"):
                self.data_file = ET.parse(self.folder.open(file)).getroot()

            if file.endswith(".xsd"):
                self.schema_file = ET.parse(self.folder.open(file)).getroot()

        if self.data_file is None or self.schema_file is None:
            raise ValueError("No data or schema file found")

    def parse_data(self):
        """Parse data file"""

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

        for col in ['REF_AREA_CODE', 'LASTACTUALDATE', 'TIME_PERIOD','OBS_VALUE']:
            self.data[col] = pd.to_numeric(self.data[col], errors = 'coerce')

    def get_data(self) -> pd.DataFrame:
        """Pipeline"""

        # webscraping steps TODO

        self.get_files()
        self.parse_data()
        self.clean_data()

        return self.data


@dataclass
class WEO(ImportData):
    """

    Attributes:
        year: Report year
        release: Report release, 1 for April, 2 for October



    """

    year: int = None
    release: int = None

    folder = None # to replace with web scraping

    super().__init__()

    def __post_init__(self):

        current_year = datetime.datetime.now().year
        current_month = datetime.datetime.now().month

        # check version, if latest, set latest
        if self.year is None:
            self.year = current_year

            if self.release is None:
                month = datetime.datetime.now().month
                if month < 4:
                    self.year -= 1
                    self.release = 2

        else:
            if self.year < current_year
                self.release = 2
            elif self.year == current_year




    def load_data(self, indicators: str | list = 'all'):
        """ """

        path = BBPaths.raw_data / f"WEO_{self.year}_{self.release}.csv"
        if not path.exists():
            Downloader(self.folder).get_data().to_csv(path)


    def update_data(self):
        """ """
        raise RuntimeError("No indicators loaded")
        pass

    def get_data(self, indicators: str | list = "all", only_data = False, only_estimates: bool = False):
        """ """

        pass






