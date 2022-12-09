"""Tools to extract data from the World Economic Outlook database."""

import xml.etree.ElementTree

import pandas as pd
import requests
import xml.etree.ElementTree as ET
from zipfile import ZipFile
import io
from bs4 import BeautifulSoup
from datetime import datetime
import country_converter as coco

BASE = 'https://www.imf.org/'


def _convert_month(month: int) -> str:
    """Converts a month number to a string"""
    if month == 1:
        return 'April'
    elif month == 2:
        return 'October'
    else:
        raise ValueError('invalid month. Must be 1 or 2, or "April" or "October"')


def validate_date(year: int, month: str | int) -> tuple[int, str]:
    """Check if the date is valid, and return the year as an integer and
    the month as a string either 'April' or 'October' (for weo releases)
    """

    if isinstance(month, int):
        month = _convert_month(month)
    elif month not in ['April', 'October']:
        raise ValueError('invalid month. Must be 1 or 2, or "April" or "October"')

    if year < 2017:
        raise ValueError('invalid year. Must be 2017 or later')

    return year, month


def latest_version():
    """Return the latest version date of the WEO data"""
    year = datetime.now().year
    month = datetime.now().month

    if month < 4:
        year -= 1
        return year, 'October'

    elif month < 10:
        return year, 'April'

    else:
        return year, 'October'


def get_root(folder: ZipFile, file: str) -> xml.etree.ElementTree.Element:
    """Get the root of the xml file

    Args:
        folder (ZipFile): ZipFile object containing the WEO data and schema files
        file (str): name of the file to be parsed

    Returns:
        xml.etree.ElementTree.Element: root of the xml file
    """

    tree = ET.parse(folder.open(file))
    return tree.getroot()


def convert_series_codes(root: xml.etree.ElementTree.Element, series: pd.Series, lookup_value: str) -> pd.Series:
    """Converts a series of codes to a series of labels

    Args:
        root (xml.etree.ElementTree.Element): root of the xml file
        series (pd.Series): series of codes
        lookup_value (str): value to look up in the xml file

    Returns:
        pd.Series: series of labels
    """

    query = root.findall('./{http://www.w3.org/2001/XMLSchema}simpleType[@name='
                         + f'"{lookup_value}"'
                         + ']/')[0].findall('./')
    return series.map({elem.attrib['value']: elem[0][0].text for elem in query})


class WEODownloader:
    """Object to download and parse the WEO data

    to use, instantiate the object and declare the weo version to download.
    If no version is provided, the latest version will be used.
    To get the data call the `get_data` method. This will return extract the data
    from weo, assign the data to the `data` attribute and return the a dataframe.
    """

    def __init__(self, version: tuple[int, str] | None = None):

        if version is not None:
            self.version = validate_date(*version) #check if version is valid
            self._latest = False
        else:
            self.version = latest_version()
            self._latest = True

        self.href = None
        self.folder = None
        self.data_file = None
        self.schema_file = None
        self.data = None

        self.column_mapper = {'UNIT': 'IMF.CL_WEO_UNIT.1.0',
                              'CONCEPT': 'IMF.CL_WEO_CONCEPT.1.0',
                              'REF_AREA': 'IMF.CL_WEO_REF_AREA.1.0',
                              'FREQ': 'IMF.CL_FREQ.1.0',
                              'SCALE': 'IMF.CL_WEO_SCALE.1.0'
                              }

    def roll_back_version(self):
        """Roll back the month and year to a previous version"""

        if self.version[1] == 'October':
            self.version = (self.version[0], 'April')
        else:
            self.version = (self.version[0] - 1, 'October')

    def _parse_href(self) -> str | bool:
        """Retrieve the href of the SDMX file"""

        url = (f'{BASE}/en/Publications/WEO/weo-database/'
               f'{self.version[0]}/{self.version[1]}/download-entire-database')
        try:
            content = requests.get(url).content
            soup = BeautifulSoup(content, 'html.parser')
            if soup.find('a', text='SDMX Data'):
                return soup.find_all('a', text='SDMX Data')[0].get('href')
            else:
                return False
        except ConnectionError:
            raise ConnectionError("invalid url")

    def get_href(self):
        """Retrieve the href of the SDMX file"""

        self.href = self._parse_href()

        if self._latest:
            # check if version hasn't been released yet
            if self.href is False:
                # if version hasn't been released, roll back to previous version and try again
                self.roll_back_version()
                href = self._parse_href()

                # if previous version hasn't been released, raise error
                if href is False:
                    raise ValueError('No valid version found')
        else:
            if self.href is False:
                raise ValueError('Version is not available')

    def get_folder(self):
        """Get the folder containing the data and schema files"""

        try:
            resp = requests.get(BASE + self.href)
            self.folder = ZipFile(io.BytesIO(resp.content))
        except ConnectionError:
            raise ConnectionError("invalid url")

    def get_file_names(self):
        """Get the names of the data and schema files in the folder
        (these may change naming convention in future releases)
        """

        if len(self.folder.namelist()) > 2:
            raise ValueError('More than one file in zip file')
        for file in self.folder.namelist():
            if file.endswith('.xml'):
                self.data_file = file
            if file.endswith('.xsd'):
                self.schema_file = file
        if self.data_file is None or self.schema_file is None:
            raise ValueError('No data or schema file found')

    def parse_data(self):
        """Parse the data file"""

        root = get_root(self.folder, self.data_file)

        rows = []
        for series in root[1].findall('./'):
            for obs in series.findall('./'):
                rows.append({**series.attrib, **obs.attrib})

        self.data = pd.DataFrame(rows)

    def convert_columns(self):
        """Convert the columns which contain codes to labels"""

        schema_root = get_root(self.folder, self.schema_file)
        for col, value in self.column_mapper.items():
            self.data = self.data.rename(columns={col: f'{col}_CODE'})
            self.data[col] = convert_series_codes(schema_root, self.data[f'{col}_CODE'], value)

    def get_data(self):
        """Extract weo data"""

        self.get_href()
        self.get_folder()
        self.get_file_names()
        self.parse_data()
        self.convert_columns()

        return self.data

    def make_weo_format(self):
        """Temporary function to make the data in the same format as the weo package data"""

        col_mapper = {'REF_AREA_CODE': 'WEO Country Code',
                      'CONCEPT_CODE': 'WEO Subject Code',
                      'REF_AREA': 'Country',
                      'CONCEPT': 'Subject Descriptor',
                      'UNIT': 'Units',
                      'SCALE': 'Scale',
                      'LASTACTUALDATE': 'Estimates Start After'}

        return (self.data.rename(columns=col_mapper)
                .loc[:, list(col_mapper.values()) + ['TIME_PERIOD', 'OBS_VALUE']]
                .pivot(index=col_mapper.values(), columns='TIME_PERIOD', values='OBS_VALUE')
                .reset_index()
                .assign(ISO=lambda d: coco.convert(d.Country, to='ISO3', not_found=None))
                )

