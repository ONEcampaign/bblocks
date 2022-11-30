"""Tools to extract data from the World Economic Outlook database."""

import pandas as pd
import requests
import xml.etree.ElementTree as ET
from zipfile import ZipFile
import io
from bs4 import BeautifulSoup


BASE = 'https://www.imf.org/'


def convert_month(month: int | str) -> str:
    """ """
    pass


def validate_date(year: int, month: str):
    """ """
    pass


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
    rows = []
    for series in root[1].findall('./'):
        for obs in series.findall('./'):
            rows.append({**series.attrib, **obs.attrib})

    return pd.DataFrame(rows)


def get_weo(year: int, month: str) -> pd.DataFrame:
    """Retrieve WEO data for a given year and month"""

    href = get_sdmx_href(year, month)
    folder = get_folder(BASE + href)
    root = get_root(folder)
    return parse_xml(root)


