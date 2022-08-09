from __future__ import annotations

import pandas as pd
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import os
from typing import Optional
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
        raise ConnectionError(f"Could not read page: {url}")

    soup = BeautifulSoup(content, "html.parser")
    table = soup.find_all("table")[4]
    links = table.find_all("a")

    return {link.string: f'{concat_url}{link.get("href")}' for link in links}


def _get_tsv_url(url: str) -> str:
    """retrieves the URL for the SDR TSV file

    Args:
        url: URL to parse

    Returns:
        A link to the TSV file
    """

    try:
        content = requests.get(url).content
    except ConnectionError:
        raise ConnectionError(f"could not read page: {url}")
    soup = BeautifulSoup(content, "html.parser")
    href = soup.find_all("a", string="TSV")[0].get("href")

    return f"https://www.imf.org/external/np/fin/tad/{href}"


def _read_sdr_tsv(url: str) -> pd.DataFrame:
    """Reads a TSV file with SDR data from the web to a pandas dataframe

    Args:
        url: link to the TSV file

    Returns:
        A pandas dataframe with SDR data
    """

    df = pd.read_csv(url, delimiter="/t", engine="python").loc[3:]
    df = df["SDR Allocations and Holdings"].str.split("\t", expand=True)
    df.columns = ["member", "holdings", "allocations"]

    return (
        df.melt(id_vars="member", value_vars=["holdings", "allocations"])
        .pipe(clean_numeric_series, series_columns="value")
        .rename(columns={"variable": "indicator"})
        .reset_index(drop=True)
    )


def _check_indicators(indicator: str) -> list:
    """Checks if the indicator is in the list of SDR accepted indicators

    Args:
        indicator: indicator to check. If indicator is None, all accepted indicators
            are returned ['holdings', 'allocations']

    Returns:
        A list of accepted indicators
    """

    accepted_indicators = ["allocations", "holdings"]

    if indicator == "all":
        return accepted_indicators

    elif not isinstance(indicator, str):
        raise ValueError(f"{indicator} is not a valid indicator type")

    if indicator not in accepted_indicators:
        raise ValueError(f"{indicator} is not a valid indicator")

    return [indicator]


class SDR(ImportData):
    """An object to extract SDR data

    An object to help extract and store the latest Special Drawing Rights (SDR)
    data from the IMF website: https://www.imf.org/external/np/fin/tad/extsdr1.aspx

    In order to use, create an instance of this class.
    Then, call the load_indicator method to load an indicator. Optionally specify
    the indicator to extract (holdings or allocations).

    If the data for an indicator has never been downloaded, it will be downloaded.
    If it has been downloaded, it will be loaded from disk.
    If `update_data` is set to True when creating the object, the data will be updated each time
    `load_indicators is called`. You can force an update by calling `update` if you
    want to refresh the data stored on disk.
    You can get a dataframe of the data by calling `get_data`.

    The data consists of `member` (including countries, prescribed holders e.g. central banks,
    and other holders - "General Resource Account"), `indicator` (holdings or allocations), `date`
    (the date of the SDR announcement), and `value`.
    """

    def __load(self, indicator_list: list) -> None:
        """Loads indicators and data from disk

        Args:
            indicator_list: list of indicators to load
        """
        # load data from disk
        df = pd.read_csv(f"{PATHS.imported_data}/{self.file_name}")

        # filter data on indicator
        self.data = df.loc[df["indicator"].isin(indicator_list)].reset_index(drop=True)

        self.indicators = {
            indicator: self.data.loc[self.data["indicator"] == indicator].reset_index(
                drop=True
            )
            for indicator in indicator_list
        }
        print(f"Successfully loaded {indicator_list} SDR data")

    def load_indicator(self, indicator: str = "all") -> ImportData:
        """Load SDR data. Optionally specify the indicator to load (holdings or allocations).

        Args:
            indicator: indicator to load, either 'holdings' or 'allocations'. The default
                is 'all' which loads both.

        Returns:
            The same object to allow chaining
        """

        # make sure indicators are valid
        indicator_list = _check_indicators(indicator)

        if (
            not os.path.exists(f"{PATHS.imported_data}/{self.file_name}")
            or self.update_data
        ):
            self.update()

        else:
            self.__load(indicator_list)

        return self

    def update(self, reload_data: bool = False) -> ImportData:
        """Update the data saved on disk

        When called it extracts the SDR data from the IMF website and saves it to disk.
        Optionally specify whether to reload the data to the object

        Args:
            reload_data: If True, the data will be reloaded to the object

        Returns:
            The same object to allow chaining
        """

        base_url = "https://www.imf.org/external/np/fin/tad/"

        links_url = "https://www.imf.org/external/np/fin/tad/extsdr1.aspx"

        # check latest year
        years = _parse_sdr_links(url=links_url, concat_url=base_url)
        latest_year_link = list(years.values())[0]

        # check latest date
        dates = _parse_sdr_links(latest_year_link, base_url)
        latest_date_link = list(dates.values())[0]

        # Convert latest date to formatted string
        latest_date = datetime.strptime(list(dates.keys())[0], "%B %d, %Y").strftime(
            "%d %B %Y"
        )
        # find tsv file
        tsv_link = _get_tsv_url(latest_date_link)

        # read tsv file
        df = _read_sdr_tsv(tsv_link)

        (
            df.assign(date=latest_date).to_csv(
                f"{PATHS.imported_data}/{self.file_name}", index=False
            )
        )

        if reload_data:
            self.__load(list(self.indicators.keys()))
        else:
            print(
                "Successfully updated SDR data to disk. "
                "Run `load_indicator` to load the data"
            )

        return self

    def get_data(
        self,
        indicators: str = "all",
        members: Optional[str | list] = "all",
    ) -> pd.DataFrame:
        """Get the data as a Pandas DataFrame

        Args:
            indicators: indicator to get, either 'holdings' or 'allocations'.
                If 'all', both indicators will be returned

            members: member to get (includes countries, prescribed holders, and others).
                If 'all', all members will be returned

        Returns:
            A pandas dataframe with the SDR data
        """
        df = pd.DataFrame()

        if indicators != "all" and isinstance(indicators, str):
            indicators = [indicators]

        if isinstance(indicators, list):
            indicators = [
                self.indicators[_] for _ in indicators if _ in list(self.indicators)
            ]

        elif indicators == "all":
            indicators = self.indicators.values()

        for indicator in indicators:
            df = pd.concat([df, indicator], ignore_index=True)

        if isinstance(members, str) and members != "all":
            members = [members]
            df = df[df["member"].isin(members)]

        elif isinstance(members, list):
            for member in members:
                if member not in df["member"].unique():
                    warnings.warn(f"member not found: {member}")
                    print(f"Available members:\n {df.member.unique()}")

        if len(df) == 0:
            raise ValueError(f"No members found")

        return df

    @property
    def file_name(self):
        """Returns the name of the stored file"""
        return f"SDR.csv"
