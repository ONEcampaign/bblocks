"""Tools to extract data from the World Economic Outlook database."""
import xml.etree.ElementTree

import pandas as pd
import requests
import xml.etree.ElementTree as ET
from zipfile import ZipFile
import io
from bs4 import BeautifulSoup

BASE = 'https://www.imf.org/'


def convert_month(month: int) -> str:
    """Converts a month number to a string"""
    if month == 1:
        return 'April'
    elif month == 2:
        return 'October'
    else:
        raise ValueError('invalid month. Must be 1 or 2, or "April" or "October"')


def validate_date(year: int, month: str | int) -> tuple[int, str]:
    """ """

    if isinstance(month, int):
        month = convert_month(month)
    elif month not in ['April', 'October']:
        raise ValueError('invalid month. Must be 1 or 2, or "April" or "October"')

    if year < 2017:
        raise ValueError('invalid year. Must be 2017 or later')

    return year, month


def get_sdmx_href(year: int, month: str):
    """Retrieve the href of the SDMX file for a given year and month."""

    url = f'https://www.imf.org/en/Publications/WEO/weo-database/{year}/{month}/download-entire-database'
    try:
        content = requests.get(url).content
        soup = BeautifulSoup(content, 'html.parser')
        return soup.find_all('a', text='SDMX Data')[0].get('href')
    except ConnectionError:
        raise ConnectionError("invalid url")


def get_folder(url: str) -> ZipFile:
    """Request a url and unzip the file"""

    try:
        resp = requests.get(url)
        return ZipFile(io.BytesIO(resp.content))
    except ConnectionError:
        raise ConnectionError("invalid url")


def get_root(folder):
    """Get the root of the xml file"""

    tree = ET.parse(folder.open(folder.namelist()[0]))
    return tree.getroot()


def parse_xml(root) -> pd.DataFrame:
    """Parse the xml file"""
    rows = []
    for series in root[1].findall('./'):
        for obs in series.findall('./'):
            rows.append({**series.attrib, **obs.attrib})

    return pd.DataFrame(rows)


def get_schema_root(folder):
    """Get the root of the xml file"""

    tree = ET.parse(folder.open(folder.namelist()[1]))
    return tree.getroot()


def _get_mapper(root: xml.etree.ElementTree.Element, lookup_value: str):
    """get mapper for a column"""

    query = root.findall('./{http://www.w3.org/2001/XMLSchema}simpleType[@name='
                         + f'"{lookup_value}"'
                         + ']/')[0].findall('./')
    return {elem.attrib['value']: elem[0][0].text for elem in query}


def convert_series_codes(root: xml.etree.ElementTree.Element,
                         series: pd.Series,
                         lookup_value: str) -> pd.Series:
    """Convert codes to values in a series"""

    mapper = _get_mapper(root, lookup_value)
    return series.map(mapper)


def convert_codes(root: xml.etree.ElementTree.Element, df: pd.DataFrame) -> pd.DataFrame:
    """" """

    column_mapper = {'UNIT': 'IMF.CL_WEO_UNIT.1.0',
                     'CONCEPT': 'IMF.CL_WEO_CONCEPT.1.0',
                     'REF_AREA': 'IMF.CL_WEO_REF_AREA.1.0',
                     'FREQ': 'IMF.CL_FREQ.1.0',
                     'SCALE': 'IMF.CL_WEO_SCALE.1.0'

                     }

    for col, value in column_mapper.items():
        df = df.rename(columns = {col: f'{col}_CODE'})
        df[col] = convert_series_codes(root, df[f'{col}_CODE'], value)

    return df


def extract_weo(year: int, month: str) -> ZipFile:
    """Retrieve WEO data for a given year and month"""

    href = get_sdmx_href(year, month)
    return get_folder(BASE + href)


def read_weo(folder):
    """ """

    root = get_root(folder)
    return parse_xml(root)


def get_weo(year: int, month: str):
    """ """

    folder = extract_weo(year, month)
    df = read_weo(folder)
    schema_root = get_schema_root(folder)
    return convert_codes(schema_root, df)


