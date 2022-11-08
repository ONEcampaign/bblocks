from __future__ import annotations

import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
import calendar
from typing import Optional

from bblocks.cleaning_tools.clean import clean_numeric_series
from bblocks.import_tools.common import ImportData
from bblocks.config import PATHS

BASE_URL = "https://www.imf.org/external/np/fin/tad/"
MAIN_PAGE_URL = "https://www.imf.org/external/np/fin/tad/extsdr1.aspx"
EXCHANGE_URL = "https://www.imf.org/external/np/fin/data/rms_sdrv.aspx"


def create_tsv_link(year: int, month: int, day: int) -> str:
    """Generate a TSV download link for a specific date"""

    flag = "&tsvflag=Y"
    return f"{BASE_URL}extsdr2.aspx?date1key={year}-{month}-{day}{flag}"


def get_response(url: str) -> requests.models.Response:
    """Request a response from a url"""
    try:
        response = requests.get(url)
    except ConnectionError:
        raise ConnectionError("Could not connect to the IMF SDR website")

    if response.status_code != 200:
        raise ConnectionError("Could not connect to the IMF SDR website")
    else:
        return response


def parse_sdr_links(response: bytes) -> dict:
    """Function to parse SDR tables. returns a dictionary of dates and links"""

    soup = BeautifulSoup(response, "html.parser")
    table = soup.find_all("table")[4]
    links = table.find_all("a")

    return {link.string: f'{BASE_URL}{link.get("href")}' for link in links}


def get_latest_date() -> str:
    """ """

    year_response = get_response(MAIN_PAGE_URL)
    year_links = parse_sdr_links(year_response.content)
    year = list(year_links.keys())[0]
    month_response = get_response(year_links[year])
    month_links = parse_sdr_links(month_response.content)
    month = list(month_links.keys())[0]

    return datetime.strptime(month, "%B %d, %Y").strftime("%Y-%m-%d")


def clean_df(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """clean the SDR dataframe"""

    df = df.loc[3:]["SDR Allocations and Holdings"].str.split("\t", expand=True)
    df.columns = ["entity", "holdings", "allocations"]

    return (
        df.melt(id_vars="entity", value_vars=["holdings", "allocations"])
        .pipe(clean_numeric_series, series_columns="value")
        .rename(columns={"variable": "indicator"})
        .reset_index(drop=True)
        .assign(date=date)
        .assign(date=lambda d: pd.to_datetime(d.date))
    )


def read_tsv(url: str) -> pd.DataFrame:
    """Read a tsv file from a url and return a dataframe"""

    try:
        return pd.read_csv(url, delimiter="/t", engine="python")

    except pd.errors.ParserError:
        raise ValueError("SDR data not available for this date")


def get_data(date: str) -> pd.DataFrame:
    """Get the SDR data for a specific date"""

    tsv_link = f"{BASE_URL}extsdr2.aspx?date1key={date}&tsvflag=Y"
    df = read_tsv(tsv_link).pipe(clean_df, date)

    return df


def format_date(date: list | tuple) -> str:
    """Format a date string containing year and month, and adds the last day for that month-year"""

    if len(date) != 2:
        raise ValueError("Date must be a list or tuple containing year and month")

    last_day = calendar.monthrange(date[0], date[1])[1]
    return f"{date[0]}-{date[1]}-{last_day}"


def check_if_not_downloaded(date: str) -> bool:
    """Checks if data is already downloaded for an indicator and area grouping
    Returns:
        True if data is not downloaded, False if data is downloaded
    """
    if os.path.exists(f"{PATHS.imported_data}/SDR_{date}.csv"):
        return False
    else:
        return True


def __get_rate(table: BeautifulSoup, currency: str):
    """Returns currency value from SDR exchange rate table"""

    exchange_dict = {"USD": "U.S.$1.00 = SDR", "SDR": "SDR1 = US$"}

    rows = table.find_all("td")

    for i in range(len(rows)):
        if exchange_dict[currency] in rows[i].text:
            return float(rows[i + 1].text.strip().split("\r\n")[0])
    else:
        raise ValueError("Could not find exchange rate")


def __get_exchange_date(table: BeautifulSoup) -> str:
    """Returns date of exchange rate table"""

    date = table.find_all("th")[0].text.strip()
    date = datetime.strptime(date, "%A, %B %d, %Y").strftime("%Y-%m-%d")
    return date


def parse_exchange(response: bytes, currency: str):
    """Parse the exchange rate response"""

    soup = BeautifulSoup(response, "html.parser")
    table = soup.find_all("table")[5]

    date = __get_exchange_date(table)
    rate = __get_rate(table, currency)

    return date, rate


def get_latest_exchange_rate(
    currency: str = "USD", only_value: bool = False
) -> float | dict:
    """Get the latest exchange rate for a specific currency

    Args:
        currency: Currency to get exchange rate for. Default is USD. Choose from 'USD' or 'SDR'
        only_value: If False, a dictionary containing the date and exchange rate is returned.
                    If True, only the exchange rate is returned. Default is False.

    Returns:
        A dictionary containing the date and exchange rate, or only the exchange rate
    """

    if currency not in ["USD", "SDR"]:
        raise ValueError("Invalid currency. Currency must be `USD` or `SDR`")

    response = get_response(EXCHANGE_URL)
    date, rate = parse_exchange(response.content, currency)

    if only_value:
        return rate
    else:
        return {"date": date, "rate": rate}


class SDR(ImportData):
    """An object to import SDR data

    An object to help extract and store the latest Special Drawing Rights (SDR)
    data from the IMF website: https://www.imf.org/external/np/fin/tad/extsdr1.aspx

    In order to use, create an instance of this class.
    Then, call the `load_indicator` method to load SDR data for a specific date. If no date is provided,
    the latest date will be found and loaded.
    Call `latest_date` to get the latest date available.
    If the data for a specific date has never been downloaded, it will be downloaded. If it has been downloaded,
    it will be loaded from disk. If `update_data` is set to True when creating the object, the data will be updated each time
    `load_indicators is called`. You can force an update by calling `update` if you
    want to refresh the data stored on disk and in the object.
    Call `get_data` to get the data as a DataFrame.

    """

    __latest_date: str = None

    @property
    def latest_date(self):
        """return the latest date of the data"""
        if self.__latest_date is None:
            self.__latest_date = get_latest_date()
        return self.__latest_date

    def load_indicator(self, date: tuple | list = "latest") -> ImportData:
        """Load the SDR data for a specific date

        Args:
            date: a list or set containing the year and month for which to load the data. If no date is provided,
            the latest date will be found and loaded.

        Returns:
            the same object to allow chaining
        """

        if date == "latest":
            date = get_latest_date()
            self.__latest_date = date
        else:
            date = format_date(date)

        if check_if_not_downloaded(date) or self.update_data:
            df = get_data(date)
            df.to_csv(f"{PATHS.imported_data}/SDR_{date}.csv", index=False)

        self.indicators[date]: pd.DataFrame = pd.read_csv(
            f"{PATHS.imported_data}/SDR_{date}.csv", parse_dates=["date"]
        )
        return self

    def update(self, reload: bool = True) -> ImportData:
        """Update the data stored on disk and in the object

        Args:
            reload: if True, the updated data will be reloaded to the object.
                    If False, the data will be only be updated on disk.

        Returns:
            the same object to allow chaining
        """

        for date in self.indicators:
            df = get_data(date)
            df.to_csv(f"{PATHS.imported_data}/SDR_{date}.csv", index=False)
            if reload:
                self.indicators[date] = df

        return self

    def get_data(self, date: str = None, indicator: str = None) -> pd.DataFrame:
        """Get SDR data as a DataFrame

        Args:
            date: the date of the data to be retrieved. If no date is provided, data for all dates will be retrieved
                  If 'latest' is provided, all the data will be returned.

            indicator: the indicator to be retrieved, choose from ['holdings', 'allocations].
                       If no indicator is provided, all the indicators will be returned.

        Returns:
            a DataFrame containing the SDR data
        """

        if date == "latest":
            df = self.indicators[self.latest_date]
        elif date is None:
            df = pd.concat(self.indicators.values())
        else:
            raise ValueError("Date must be 'latest' or None")

        if indicator is not None:
            if indicator not in ["holdings", "allocations"]:
                raise ValueError("Indicator must be 'holdings' or 'allocations'")
            df = df.loc[df.indicator == indicator]

        return df
