""" """

import pandas as pd
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import os
from bblocks.config import PATHS

from bblocks.cleaning_tools.common import clean_number


def _parse_sdr_links(url: str, concat_url: str) -> dict:
    """Function to parse SDR tables from html

    When called it finds the SDR tables and finds a relevant
    date and link

    Args:
        url (str): URL to parse
        concat_url: base url onto which a href is attached

    Returns:
        A dictionary with dates as keys and URLs as values
    """

    try:
        content = requests.get(url).content
    except ConnectionError:
        raise ConnectionError(f'Could not read page: {url}')

    soup = BeautifulSoup(content, 'html.parser')
    table = soup.find_all('table')[4]
    links = table.find_all('a')

    return {link.string: f'{concat_url}{link.get("href")}' for link in links}


def _get_tsv_url(url: str) -> str:
    """retrieves the URL for the SDR TSV file

    Args:
        url (str): URL to parse

    Returns:
        A link to the TSV file
    """

    try:
        content = requests.get(url).content
    except ConnectionError:
        raise ConnectionError(f'could not read page: {url}')
    soup = BeautifulSoup(content, "html.parser")
    href = soup.find_all("a", string="TSV")[0].get("href")

    return f"https://www.imf.org/external/np/fin/tad/{href}"


def _read_sdr_tsv(url: str) -> pd.DataFrame:
    """Reads a TSV file with SDR data from the web to a pandas dataframe

    Args:
        url (str): link to the TSV file

    Returns:
        A pandas dataframe with SDR data
    """

    df = pd.read_csv(url, delimiter="/t", engine="python").loc[3:]
    df = df["SDR Allocations and Holdings"].str.split("\t", expand=True)
    df.columns = ["members", "sdr_holdings", "sdr_allocations"]

    df['sdr_holdings'] = df['sdr_holdings'].apply(clean_number)
    df['sdr_allocations'] = df['sdr_allocations'].apply(clean_number)

    return df.reset_index(drop=True)


class SDR:
    """An object to download the latest SDR holdings and allocations.

    Parameters:
        update_data (bool): set to `True` to update the data, otherwise
                            the data will be searched and read in the disk if it exists.
                            If the data does not exist in disk, it will be updated.
                            default = `False`
    """

    update_data: bool = False

    def __init__(self, update_data=None):
        self.data = None
        self.date = None
        self.update_data = update_data

        if self.update_data:
            self.update()
        if not os.path.exists(f"{PATHS.imported_data}/{self.file_name}"):
            self.update()
        else:
            self.data = pd.read_csv(f"{PATHS.imported_data}/{self.file_name}")
            self.date = self.data['date'].unique()[0]

    def update(self) -> None:
        """Update the data saved on disk

        When called it will scrape the IMF SDR site and save the
        latest SDR values in disk
        """

        base = 'https://www.imf.org/external/np/fin/tad/'

        # check latest year
        years = _parse_sdr_links(url='https://www.imf.org/external/np/fin/tad/extsdr1.aspx', concat_url=base)
        latest_year_link = list(years.values())[0]

        # check latest date
        dates = _parse_sdr_links(latest_year_link, base)
        latest_date_link = list(dates.values())[0]
        latest_date = datetime.strptime(list(dates.keys())[0], "%B %d, %Y").strftime("%d %B %Y")  # assign latest date

        # find tsv file
        tsv_link = _get_tsv_url(latest_date_link)

        # read tsv file
        df = _read_sdr_tsv(tsv_link)
        (df.assign(date=latest_date).to_csv(f"{PATHS.imported_data}/{self.file_name}", index=False))

        self.data = pd.read_csv(f"{PATHS.imported_data}/{self.file_name}")
        self.date = self.data['date'].unique()[0]

    @property
    def file_name(self):
        """Returns the name of the stored file"""
        return f"SDR.csv"
