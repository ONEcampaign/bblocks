""" """
from __future__ import annotations

import pandas as pd
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import os
from typing import Optional
from dataclasses import field
import warnings

from bblocks.config import PATHS
from bblocks.cleaning_tools.clean import clean_numeric_series
from bblocks.import_tools.common import ImportData


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
    df.columns = ["member", "holdings", "allocations"]

    return (df.melt(id_vars='member', value_vars=["holdings", "allocations"])
            .pipe(clean_numeric_series, series_columns="value")
            .rename(columns={"variable": "indicator"})
            .reset_index(drop=True)
            )


def _check_indicators(indicators: str | list) -> list:
    """ """

    accepted_indicators = ['allocations', 'holdings']
    if isinstance(indicators, str):
        if indicators not in accepted_indicators:
            raise ValueError(f"{indicators} is not a valid indicator")
        return [indicators]
    elif isinstance(indicators, list):
        for indicator in indicators:
            if indicator not in accepted_indicators:
                raise ValueError(f"{indicator} is not a valid indicator")
        return indicators
    elif indicators is None:
        return ['allocations', 'holdings']


class SDR(ImportData):
    """ """

    indicators: list = field(default_factory=list)

    def load_indicator(self, indicators: Optional | str = None) -> ImportData:
        """ """

        # make sure indicators are valid
        indicator_list = _check_indicators(indicators)

        if not os.path.exists(f"{PATHS.imported_data}/{self.file_name}") or self.update_data:
            self.update()

        df = pd.read_csv(f"{PATHS.imported_data}/{self.file_name}")
        self.data = df.loc[df['indicator'].isin(indicator_list)].reset_index(drop=True)
        self.indicators = indicator_list

        return self

    def update(self) -> ImportData:
        """ """

        base = 'https://www.imf.org/external/np/fin/tad/'
        # check latest year
        years = _parse_sdr_links(url='https://www.imf.org/external/np/fin/tad/extsdr1.aspx', concat_url=base)
        latest_year_link = list(years.values())[0]

        # check latest date
        dates = _parse_sdr_links(latest_year_link, base)
        latest_date_link = list(dates.values())[0]
        latest_date = datetime.strptime(list(dates.keys())[0], "%B %d, %Y").strftime(
            "%d %B %Y")  # assign latest date

        # find tsv file
        tsv_link = _get_tsv_url(latest_date_link)

        # read tsv file
        df = _read_sdr_tsv(tsv_link)

        (df.assign(date=latest_date)
         .to_csv(f"{PATHS.imported_data}/{self.file_name}", index=False))

        return self

    def get_data(self,
                 indicators: Optional | str = None,
                 members: Optional | str | list = None
                 ) -> pd.DataFrame:
        """ """

        df = self.data

        indicator_list = _check_indicators(indicators)
        df = df.loc[df['indicator'].isin(indicator_list)].reset_index(drop=True)

        if members is not None:
            if isinstance(members, str):
                members = [members]

            df = df[df['member'].isin(members)]
            if len(df) == 0:
                raise ValueError(f"No members found")
            else:
                for member in members:
                    if member not in df['member'].unique():
                        warnings.warn(f"member not found: {member}")

        return df

    @property
    def file_name(self):
        """Returns the name of the stored file"""
        return f"SDR.csv"
