"""Extract ILO data"""

import pandas as pd
import requests
from bs4 import BeautifulSoup

from dataclasses import dataclass
from bblocks.import_tools.common import ImportData
from bblocks.config import BBPaths


BASE_URL = "https://www.ilo.org"


def _get_glossaries_links(lang: str = 'en') -> dict[str, str]:
    """Get reference dictionaries for codes and names of indicators

    Args:
        lang (str, optional): Language of the glossaries. Defaults to 'en'.
        Choose from en (English), fr (French), es (Spanish)

    Returns:
        dict: Dictionary with the names as keys and a dictionary of codes and names as values
    """
    d = {}

    response = requests.get(f'{BASE_URL}/ilostat-files/WEB_bulk_download/html/bulk_dic.html')
    soup = BeautifulSoup(response.text, 'html.parser')

    for a in soup.find_all('a'):
        if f'_{lang}.csv' in a.text:
            d.update({a.text.split('_')[0]: f"{BASE_URL}/{a.get('href')}"})

    if len(d) == 0:
        raise ValueError(f'Language {lang} not available. Choose from en, fr, es')

    return d


def get_glossaries(lang: str = 'en') -> dict[str, dict[str, str]]:
    """Get reference dictionaries for codes and names of indicators

    Args:
        lang (str, optional): Language of the glossaries. Defaults to 'en'.
        Choose from en (English), fr (French), es (Spanish)

    Returns:
        dict: Dictionary with the names as keys and a dictionary of codes and names as values
    """
    d = {}
    glossaries_links = _get_glossaries_links(lang)

    for name, link in glossaries_links.items():
        d.update({name: pd.read_csv(link, index_col=0).iloc[:, 0].to_dict()})

    d.update({'obs_status': d.pop('obs')})

    return d


def extract_data(indicator_code: str) -> pd.DataFrame:
    """Extract data from ILO website

    Args:
        indicator_code (str): Indicator code to extract

    Returns:
        pd.DataFrame: Dataframe with the data

    Exceptions:
        requests.exceptions.HTTPError: If the indicator is not valid

    """

    url = f"{BASE_URL}/ilostat-files/WEB_bulk_download/indicator/{indicator_code}.csv.gz"

    try:
        return pd.read_csv(url, compression='gzip')
    except requests.exceptions.HTTPError:
        raise requests.exceptions.HTTPError(f'Indicator {indicator_code} not available')


def clean_df(df, glossaries) -> pd.DataFrame:
    """Clean a dataframe

    Args:
        df (pd.DataFrame): Dataframe to clean
        glossaries (dict): Dictionary with the names as keys and a dictionary of codes and names as values

    Returns:
        pd.DataFrame: Cleaned dataframe

    """

    # df = df.drop(columns=['note_classif', 'note_source']) # drop unwanted columns
    # loop through glossaries and rename columns
    for key in glossaries:
        if key in df.columns:
            df = (df.rename(columns = {key: f'{key}_code'})) # rename columns to *_code
            df[key] = df[f'{key}_code'].map(glossaries[key]) # add new column with glossary names

    return df


@dataclass
class ILO(ImportData):
    """An object to import data from the ILO website"""

    _glossaries: dict = None

    def _load_glossaries(self):
        """Load the glossaries to the object"""

        self._glossaries = get_glossaries()

    def load_data(self, indicator: str | list) -> ImportData:
        """Load the data saved on disk"""

        if isinstance(indicator, str):
            indicator = [indicator]

        for ind in indicator: # loop through indicators

            path = BBPaths.raw_data / f"{indicator}.csv"

            # download data if not saved to disk
            if not path.exists():

                # download glossaries if not loaded to object
                if self._glossaries is None:
                    self._load_glossaries()

                # download data
                (extract_data(ind)
                 .pipe(clean_df, self._glossaries)
                 .to_csv(path, index=False)

                 )

            # load data to object
            self._data[ind] = pd.read_csv(path)
            return self

    def update_data(self, reload_data: bool = True) -> ImportData:
        """ """

        if len(self._data) == 0:
            raise RuntimeError("No indicators loaded")

        # download glossaries if not loaded to object
        if self._glossaries is None:
            self._load_glossaries()

        for ind in self._data: # loop through loaded indicators

            # extract data
            (extract_data(ind)
             .pipe(clean_df, self._glossaries)
             .to_csv(BBPaths.raw_data / f"{ind}.csv", index=False)
             )

            # reload data to object if reload_data is True
            if reload_data:
                self._data[ind] = pd.read_csv(BBPaths.raw_data / f"{ind}.csv")

        return self


        

