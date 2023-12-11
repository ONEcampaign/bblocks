from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
import calendar

from bblocks.cleaning_tools.clean import clean_numeric_series, convert_to_datetime
from bblocks.import_tools.common import ImportData
from bblocks.config import BBPaths
from bblocks.logger import logger

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
    """Get the latest available date"""

    year_response = get_response(MAIN_PAGE_URL)
    year_links = parse_sdr_links(year_response.content)
    year = list(year_links.keys())[0]
    month_response = get_response(year_links[year])
    month_links = parse_sdr_links(month_response.content)
    month = list(month_links.keys())[0]

    return datetime.strptime(month, "%B %d, %Y").strftime("%Y-%m-%d")


def clean_df(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """Clean the SDR dataframe"""

    df = df.loc[3:]["SDR Allocations and Holdings"].str.split("\t", expand=True)
    df.columns = ["entity", "holdings", "allocations"]

    return (
        df.melt(id_vars="entity", value_vars=["holdings", "allocations"])
        .pipe(clean_numeric_series, series_columns="value", to=float)
        .rename(columns={"variable": "indicator"})
        .reset_index(drop=True)
        .assign(date=date)
        .assign(date=lambda d: convert_to_datetime(d.date))
    )


def read_tsv(url: str) -> pd.DataFrame:
    """Read a tsv file from a url and return a dataframe"""

    try:
        return pd.read_csv(url, delimiter="/t", engine="python")

    except pd.errors.ParserError:
        raise ValueError("SDR _data not available for this date")


def get_data(date: str) -> pd.DataFrame:
    """Get the SDR _data for a specific date"""

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
    """Checks if _data is already downloaded for an indicator and area grouping
    Returns:
        True if _data is not downloaded, False if _data is downloaded
    """
    if os.path.exists(f"{BBPaths.raw_data}/SDR_{date}.csv"):
        return False
    else:
        return True


def __get_rate(table: BeautifulSoup, currency: str) -> float:
    """Returns currency value from SDR exchange rate table"""

    exchange_dict = {"USD": "U.S.$1.00 = SDR", "SDR": "SDR1 = US$"}

    rows = table.find_all("td")

    for i in range(len(rows)):
        if exchange_dict[currency] in rows[i].text:
            return float(rows[i + 1].text.strip().split("\n")[0])
    else:
        raise ValueError("Could not find exchange rate")


def __get_exchange_date(table: BeautifulSoup) -> str:
    """Returns date of exchange rate table"""

    date = table.find_all("th")[0].text.strip()
    date = datetime.strptime(date, "%A, %B %d, %Y").strftime("%Y-%m-%d")
    return date


def parse_exchange(response: bytes, currency: str) -> tuple[str, float]:
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


@dataclass()
class SDR(ImportData):
    """An object to import SDR data

    An object to help extract and store the latest Special Drawing Rights (SDR)
    data from the IMF website: https://www.imf.org/external/np/fin/tad/extsdr1.aspx

    In order to use, create an instance of this class.
    Then, call the `load_data` method to load SDR _data for a specific date. If no date is provided,
    the latest date will be found and loaded.
    Call `latest_date` to get the latest date available.
    If the data for a specific date has never been downloaded, it will be downloaded. If it has been downloaded,
    it will be loaded from disk. You can force an update by calling `update_data` if you
    want to refresh the data stored on disk and in the object.
    Call `get_data` to get the data as a DataFrame.

    """

    __latest_date: str = None

    def __repr__(self):
        if self.__latest_date is None:
            return f"SDR object with no data loaded"
        return f"SDR data for {self.__latest_date}"

    def latest_date(self):
        """Return the latest date of the _data"""
        if self.__latest_date is None:
            self.__latest_date = get_latest_date()
        return self.__latest_date

    def load_data(self, date: str | tuple = "latest") -> ImportData:
        """Load the SDR _data for a specific date

        Returns:
            the same object to allow chaining
        """

        if date == "latest":
            date = get_latest_date()
            self.__latest_date = date
        else:
            date = format_date(date)

        if check_if_not_downloaded(date):
            df = get_data(date)
            df.to_csv(f"{BBPaths.imported_data}/SDR_{date}.csv", index=False)

        logger.info(f"Loading SDR data for {date}")
        self._data[date]: pd.DataFrame = pd.read_csv(
            f"{BBPaths.imported_data}/SDR_{date}.csv", parse_dates=["date"]
        )
        return self

    def update_data(self, reload: bool) -> ImportData:
        """Update the data stored on disk and in the object

        Returns:
            the same object to allow chaining
        """

        for date in self._data:
            df = get_data(date)
            df.to_csv(f"{BBPaths.imported_data}/SDR_{date}.csv", index=False)
            if reload:
                self._data[date] = df

        return self

    def get_data(self, date: str = None, indicator: str = None) -> pd.DataFrame:
        """Get SDR _data as a DataFrame

        Args:
            date: the date of the _data to be retrieved. If no date is provided, _data for all dates will be retrieved
                  If 'latest' is provided, all the _data will be returned.

            indicator: the indicator to be retrieved, choose from ['holdings', 'allocations].
                       If no indicator is provided, all the indicators will be returned.

        Returns:
            a DataFrame containing the SDR _data
        """
        if date == "latest":
            df = super().get_data(indicators="latest")
        elif date is None:
            df = super().get_data()
        else:
            raise ValueError("Date must be 'latest' or None")

        if indicator is not None:
            if indicator not in ["holdings", "allocations"]:
                raise ValueError("Indicator must be 'holdings' or 'allocations'")
            df = df.loc[df.indicator == indicator]

        return df.reset_index(drop=True)
