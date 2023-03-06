"""Extract ILO data"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.error import HTTPError

from dataclasses import dataclass

from bblocks.import_tools.common import ImportData
from bblocks.config import BBPaths
from bblocks.logger import logger


BASE_URL = "https://www.ilo.org"


def _get_glossaries_links() -> dict[str, str]:
    """Get reference dictionaries for codes and names of indicators"""
    d = {}

    response = requests.get(
        f"{BASE_URL}/ilostat-files/WEB_bulk_download/html/bulk_dic.html"
    )
    soup = BeautifulSoup(response.text, "html.parser")

    # loop through links and get the ones that end with _en.csv
    for a in soup.find_all("a"):
        if f"_en.csv" in a.text:
            # update dictionary with name and link
            d.update({a.text.split("_")[0]: f"{BASE_URL}/{a.get('href')}"})

    return d


def get_glossaries() -> dict[str, dict[str, str]]:
    """Get reference dictionaries for codes and names of indicators

    Returns:
        dict: Dictionary with the names as keys and a dictionary of codes and names as values
    """
    d = {}
    glossaries_links = _get_glossaries_links()  # get names and links to glossaries

    # loop through links and get the data
    for name, link in glossaries_links.items():
        d.update({name: pd.read_csv(link, index_col=0).iloc[:, 0].to_dict()})

    d.update({"obs_status": d.pop("obs")})  # rename obs to obs_status

    return d


def extract_data(indicator_code: str) -> pd.DataFrame:
    """Extract data from ILO website

    Args:
        indicator_code (str): Indicator code to extract

    Returns:
        pd.DataFrame: Dataframe with the data
    """

    url = (
        f"{BASE_URL}/ilostat-files/WEB_bulk_download/indicator/{indicator_code}.csv.gz"
    )

    try:
        return pd.read_csv(url, compression="gzip")
    except HTTPError:
        logger.error(f"Indicator not available: {indicator_code}")
        raise ValueError(f"Indicator not available: {indicator_code}")


def clean_df(df: pd.DataFrame, glossaries: dict[str:dict]) -> pd.DataFrame:
    """Clean a dataframe

    Args:
        df (pd.DataFrame): Dataframe to clean
        glossaries (dict): Dictionary with the names as keys and a dictionary of codes and names as values

    Returns:
        pd.DataFrame: Cleaned dataframe
    """

    # loop through glossaries and rename columns
    for key in glossaries:
        if key in df.columns:
            df = df.rename(columns={key: f"{key}_code"})  # rename columns to *_code
            df[key] = df[f"{key}_code"].map(
                glossaries[key]
            )  # add new column with glossary names

    return df


def download_data(indicator: str, path: str, glossaries: dict) -> None:
    """Pipeline to download an indicator and save it to disk.

    Args:
        indicator: Indicator code to download
        path: Path to save the data to
        glossaries: dictionary to map codes to names
    """

    extract_data(indicator).pipe(clean_df, glossaries).to_csv(path, index=False)


@dataclass
class ILO(ImportData):
    """An object to import data from the ILO website

    To use this object, first create an instance of it and then call the `load_data` method
    to load an indicator to the object. Use the `get_data` method to get the data from the object.
    To update the data saved on disk and in the object, call the `update_data` method.
    The attribute `available_indicators` contains a dataframe with information on
    all the available indicators from the ILO.
    """

    _glossaries: dict = None  # dictionary used to map codes to names
    _available_indicators: pd.DataFrame = (
        None  # dataframe with information on all available indicators
    )

    def available_indicators(self) -> pd.DataFrame:
        """Return a dataframe with information on all the available indicators from the ILO"""

        if self._available_indicators is None:
            self._available_indicators = pd.read_csv(
                f"{BASE_URL}//ilostat-files/WEB_bulk_download/indicator/table_of_contents_en.csv"
            )
            logger.debug("Loaded available indicators to object")
        return self._available_indicators

    def _load_glossaries(self) -> None:
        """Load the glossaries to the object"""

        self._glossaries = get_glossaries()
        logger.info("Loaded glossaries to object")

    def load_data(self, indicator: str | list) -> ImportData:
        """Load an ILO indicator to the object

        This method with check if the indicator is already saved to disk. If not, it
        will downlaod it to disk and load it to the object. Use the `get_data` method
        to get the data from the object after it is loaded.

        Args:
            indicator (str | list): Indicator code or list of indicator codes to load

        Returns:
            ImportData: The object with the data loaded
        """

        if isinstance(indicator, str):
            indicator = [indicator]

        for ind in indicator:  # loop through indicators

            path = BBPaths.raw_data / f"{ind}.csv"

            # download data if not saved to disk
            if not path.exists():

                # download glossaries if not loaded to object
                if self._glossaries is None:
                    self._load_glossaries()
                    logger.info("Loaded glossaries to object")

                # download data
                download_data(ind, path, self._glossaries)

            # load data to object
            self._data[ind] = pd.read_csv(path)
            logger.info(f"Loaded indicator to object: {ind}")

        return self

    def update_data(self, reload_data: bool = True) -> ImportData:
        """Update the data saved on disk

        When called it will go through all the indicators loaded to the object
        and download the data again. If reload_data is True, it will also reload the data
        to the object.

        Args:
            reload_data: If True, it will reload the data to the object. Defaults to True.

        Returns:
            ImportData: The object with the updated data

        """

        if len(self._data) == 0:
            raise RuntimeError("No indicators loaded")

        # download glossaries if not loaded to object
        if self._glossaries is None:
            self._load_glossaries()

        for ind in self._data:  # loop through loaded indicators

            # download data
            download_data(ind, BBPaths.raw_data / f"{ind}.csv", self._glossaries)

            # reload data to object if reload_data is True
            if reload_data:
                self._data[ind] = pd.read_csv(BBPaths.raw_data / f"{ind}.csv")
                logger.info(f"Reloaded indicator to object: {ind}")

        logger.info("Data successfully updated")

        return self
