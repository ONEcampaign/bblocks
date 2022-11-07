import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
import calendar

from bblocks.cleaning_tools.clean import clean_numeric_series
from bblocks.import_tools.common import ImportData
from bblocks.config import PATHS

BASE_URL = 'https://www.imf.org/external/np/fin/tad/'
MAIN_PAGE_URL = 'https://www.imf.org/external/np/fin/tad/extsdr1.aspx'


def create_tsv_link(year: int, month: int, day: int) -> str:
    """Generate a TSV download link for a specific date"""

    flag = '&tsvflag=Y'
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


def parse_sdr_links(response: requests.models.Response) -> dict:
    """Function to parse SDR tables. returns a dictionary of dates and links"""

    soup = BeautifulSoup(response.content, "html.parser")
    table = soup.find_all("table")[4]
    links = table.find_all("a")

    return {link.string: f'{BASE_URL}{link.get("href")}' for link in links}


def get_latest_date() -> str:
    """ """

    year_response = get_response(MAIN_PAGE_URL)
    year_links = parse_sdr_links(year_response)
    year = list(year_links.keys())[0]
    month_response = get_response(year_links[year])
    month_links = parse_sdr_links(month_response)
    month = list(month_links.keys())[0]

    return datetime.strptime(month, '%B %d, %Y').strftime('%Y-%m-%d')


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """clean the SDR dataframe
    """

    df = (df.loc[3:]
          ["SDR Allocations and Holdings"]
          .str.split("\t", expand=True)
          )
    df.columns = ["member", "holdings", "allocations"]

    return (df
            .melt(id_vars="member", value_vars=["holdings", "allocations"])
            .pipe(clean_numeric_series, series_columns="value")
            .rename(columns={"variable": "indicator"})
            .reset_index(drop=True)
    )


def read_tsv(url: str) -> pd.DataFrame:
    """Read a tsv file from a url"""

    try:
        return pd.read_csv(url, delimiter="/t", engine="python")

    except pd.errors.ParserError:
        raise ValueError("SDR data not available for this date")


def get_data(date: str) -> pd.DataFrame:
    """Get the SDR data for a specific date"""

    tsv_link = f"{BASE_URL}extsdr2.aspx?date1key={date}&tsvflag=Y"
    df = (read_tsv(tsv_link)
          .pipe(clean_df)
          )

    return df


def format_date(date: list | set) -> str:
    """Format a date string containing year and month, and adds the last day for that month-year"""

    if len(date) != 2:
        raise ValueError("Date must be a list or set containing year and month")

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




class SDR(ImportData):
    """ """

    __latest_date: str = None

    @property
    def latest_date(self):
        """return the latest date of the data"""
        if self.__latest_date is None:
            self.__latest_date = get_latest_date()
        return self.__latest_date


    def load_indicator(self, date: set | list = 'latest') -> ImportData:
        """ """

        if date == 'latest':
            date = get_latest_date()
            self.__latest_date = date
        else:
            date = format_date(date)

        if check_if_not_downloaded(date) or self.update_data:
            df = get_data(date)
            df.to_csv(f"{PATHS.imported_data}/SDR_{date}.csv", index=False)

        self.indicators[date]: pd.DataFrame = pd.read_csv(f"{PATHS.imported_data}/SDR_{date}.csv")
        return self

    def update(self, reload: bool = True) -> ImportData:
        """ """

        for date in self.indicators:
            df = get_data(date)
            df.to_csv(f"{PATHS.imported_data}/SDR_{date}.csv", index=False)
            if reload:
                self.indicators[date] = df

        return self

    def get_data(self, date: list | set = 'latest'):
















