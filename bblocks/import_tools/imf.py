from __future__ import annotations

import pandas as pd
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import os
from typing import Optional
import warnings

from weo import all_releases, download, WEO

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


def _check_sdr_indicators(indicator: str | list) -> list:
    """Checks if the indicator is in the list of SDR accepted indicators

    Args:
        indicator: indicator to check. If indicator is 'all', all accepted indicators
            are returned ['holdings', 'allocations']

    Returns:
        A list of accepted indicators
    """

    accepted_indicators = ["allocations", "holdings"]

    if indicator == "all":
        return accepted_indicators

    else:
        if isinstance(indicator, str):
            indicator = [indicator]

        for i in indicator:
            if i not in accepted_indicators:
                raise ValueError(f"{i} is not a valid indicator.")

        return indicator


def _get_data(obj: ImportData, indicators: str | list) -> pd.DataFrame:
    """Unpack dictionary of indicators into a dataframe"""
    df = pd.DataFrame()

    if indicators == "all":
        indicators = obj.indicators.values()

    if isinstance(indicators, str):
        indicators = [indicators]

    if isinstance(indicators, list):

        for _ in indicators:
            if _ not in obj.indicators:
                raise ValueError(f"{_} has not been loaded or is an invalid indicator.")

        indicators = [
            obj.indicators[_] for _ in indicators if _ in list(obj.indicators)
        ]

    for _ in indicators:
        df = pd.concat([df, _], ignore_index=True)

    return df


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

    def load_indicator(self, indicator: Optional[str | list] = "all") -> ImportData:
        """Load SDR data. Optionally specify the indicator to load (holdings or allocations).

        Args:
            indicator: indicator to load, either 'holdings' or 'allocations'. The default
                is 'all' which loads both.

        Returns:
            The same object to allow chaining
        """

        # make sure indicators are valid
        indicator_list = _check_sdr_indicators(indicator)

        if (
                not os.path.exists(f"{PATHS.imported_data}/{self.file_name}")
                or (self.update_data and self.data is None)
        ):
            self.update(reload_data=False)

        if self.data is None:
            self.data = pd.read_csv(f"{PATHS.imported_data}/{self.file_name}")

        for i in indicator_list:
            self.indicators[i] = self.data[self.data["indicator"] == i].reset_index(drop=True)

        return self

    def update(self, reload_data: bool = True) -> ImportData:
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
            self.load_indicator(list(self.indicators))
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
        df = _get_data(obj=self, indicators=indicators)

        if isinstance(members, str) and members != "all":
            if members not in self.members:
                raise ValueError(f"member not found: {members}.\nPlease call `obj.member` to see available members.")

            df = df[df["member"] == members]

        elif isinstance(members, list):
            for member in members:
                if member not in df["member"].unique():
                    warnings.warn(f"member not found: {member}.\nPlease call `obj.member` to see available members.")
            df = df[df["member"].isin(members)]

        if len(df) == 0:
            raise ValueError(f"No members found")

        return df.reset_index(drop=True)

    @property
    def file_name(self):
        """Returns the name of the stored file"""
        return f"SDR.csv"

    @property
    def members(self):
        """Returns a list of all members"""
        return self.data["member"].unique()


def _check_weo_parameters(
        latest_y: int | None = None, latest_r: int | None = None
) -> (int, int):
    """Check parameters and return max values or provided input"""
    if latest_y is None:
        latest_y = max(*all_releases())[0]

    # if latest release isn't provided, take max value
    if latest_r is None:
        latest_r = max(*all_releases())[1]

    return latest_y, latest_r


def _update_weo(latest_y: int = None, latest_r: int = None) -> None:
    """Update data from the World Economic Outlook, using WEO package"""

    latest_y, latest_r = _check_weo_parameters(latest_y, latest_r)

    # Download the file from the IMF website and store in directory
    download(
        latest_y,
        latest_r,
        directory=PATHS.imported_data,
        filename=f"weo{latest_y}_{latest_r}.csv",
    )


class WorldEconomicOutlook(ImportData):
    """World Economic Outlook data"""

    def __load_data(
            self, latest_y: int | None = None, latest_r: int | None = None
    ) -> None:
        """loading WEO as a clean dataframe

        Args:
            latest_y: passed only optional to override the behaviour to get the latest
            release year for the WEO.
            latest_r: passed only optionally to override the behaviour to get the latest
            released value (1 or 2).
        """

        latest_y, latest_r = _check_weo_parameters(latest_y, latest_r)

        names = {
            "ISO": "iso_code",
            "WEO Subject Code": "indicator",
            "Subject Descriptor": "indicator_name",
            "Subject Notes": "indicator_description",
            "Units": "units",
            "Scale": "scale",
            "Estimates Start After": "estimates_start_after",
        }

        to_drop = [
            "WEO Country Code",
            "Country",
            "Country/Series-specific Notes",
        ]

        # If data doesn't exist or update is required, update the data
        if (
                not os.path.exists(f"{PATHS.imported_data}/weo{latest_y}_{latest_r}.csv")
                or self.update_data
        ):
            _update_weo(latest_y, latest_r)

        # Load the data from disk
        df = WEO(PATHS.imported_data + rf"/weo{latest_y}_{latest_r}.csv").df

        # Load data into data object
        self.data = (
            df.drop(to_drop, axis=1)
                .rename(columns=names)
                .melt(id_vars=names.values(), var_name="year", value_name="value")
                .assign(
                year=lambda d: pd.to_datetime(d.year, format="%Y"),
                value=lambda d: clean_numeric_series(d.value),
            )
                .dropna(subset=["value"])
                .reset_index(drop=True)
        )

    def _check_indicators(self, indicators: str | list | None = None) -> None | dict:

        if self.data is None:
            self.__load_data()

        # Create dictionary of available indicators
        indicators_ = (
            self.data.drop_duplicates(subset=["indicator", "indicator_name", "units"])
                .assign(name_units=lambda d: d.indicator_name + " (" + d.units + ")")
                .set_index("indicator")["name_units"]
                .to_dict()
        )

        if indicators is None:
            return indicators_

        if isinstance(indicators, str):
            indicators = [indicators]

        for _ in indicators:
            if _ not in indicators_:
                self.available_indicators()
                raise ValueError(f"Indicator not found: {_}")

    def load_indicator(
            self, indicator_code: str, indicator_name: Optional[str] = None
    ) -> ImportData:
        """Loads a specific indicator from the World Economic Outlook data"""

        if self.data is None:
            self.__load_data()

        # Check if indicator exists
        self._check_indicators(indicators=indicator_code)

        self.indicators[indicator_code] = (
            self.data.loc[lambda d: d.indicator == indicator_code]
                .assign(
                indicator_name=indicator_name
                if indicator_name is not None
                else lambda d: d.indicator_name,
                estimate=lambda d: d.apply(
                    lambda r: True if r.year.year >= r.estimates_start_after else False,
                    axis=1,
                ),
            )
                .drop(columns=["estimates_start_after"])
                .sort_values(["iso_code", "year"])
                .reset_index(drop=True)
        )
        return self

    def update(self, latest_y: int | None = None, latest_r: int | None = None) -> None:
        """Update the stored WEO data, using WEO package.

        Args:
            latest_y: passed only optionally to override the behaviour to get the latest
                release year for the WEO.
            latest_r: passed only optionally to override the behaviour to get the latest
                released value (1 or 2).
        """
        _update_weo(latest_y=latest_y, latest_r=latest_r)

    def available_indicators(self) -> None:
        """Print the available indicators in the dataset"""

        indicators_ = self._check_indicators(indicators=None)

        available = [f"{code}: {name} \n" for code, name in indicators_.items()]

        print(f"Available indicators:\n{''.join(available)}")

    def get_data(
            self, indicators: str | list = "all", keep_metadata: bool = False
    ) -> pd.DataFrame:

        df = _get_data(obj=self, indicators=indicators)

        if not keep_metadata:
            return df.filter(["iso_code", "name", "indicator", "year", "value"], axis=1)

        return df
