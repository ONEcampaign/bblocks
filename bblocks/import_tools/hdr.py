"""Tools to import UNDP Human Development Report data."""

import pandas as pd
import requests
import io
from bs4 import BeautifulSoup

BASE_URL = 'https://hdr.undp.org/data-center/documentation-and-downloads'


def _parse_html(soup: BeautifulSoup) -> tuple[str, str]:
    """Parses html for data and metadata links"""

    data_section = soup.find_all('div', {'class': 'section data-links-files'})
    links = data_section[1].find_all('a')

    return links[0].get('href'), links[1].get('href')


def get_data_links() -> dict[str, str]:
    """returns links for data and metadata"""

    try:
        response = requests.get(BASE_URL)
        soup = BeautifulSoup(response.content, 'html.parser')
        data_url, metadata_url = _parse_html(soup)
        return {'data_url': data_url, 'metadata_url': metadata_url}

    except ConnectionError:
        raise ConnectionError(f'Could not read data from {BASE_URL}')


def read_data(url: str) -> pd.DataFrame:
    """Read UNDP Human Development Report data from a URL.

    Returns a dataframe with either the data or metadata. The function checks
    the content type of the response and returns the appropriate dataframe.

    Args:
        url (str): URL to read data from.
    """

    try:
        response = requests.get(url)
        if response.status_code != 200:
            raise ValueError(f'Could not read data from {url}')

        # check content type, use read_csv for csv and read_excel for xlsx
        if response.headers['Content-Type'] == 'text/csv':
            return pd.read_csv(io.BytesIO(response.content))

        if response.headers[
            'Content-Type'] == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
            return pd.read_excel(response.content, sheet_name="codebook")

        return pd.read_csv(io.BytesIO(response.content))

    except ConnectionError:
        raise ValueError(f'Could not read data from {url}')


def create_code_dict(metadata_df: pd.DataFrame) -> dict[str, str]:
    """Create a dictionary of variable codes and names.

    This function takes the metadata dataframe and returns a dictionary
    to be used to map variable codes to variable names.

    Args:
        metadata_df (pd.DataFrame): Metadata dataframe.
    """

    return (metadata_df
            .dropna()
            .loc[:, ['Full name', 'Short name']]
            .set_index('Short name')
            .loc[:, 'Full name']
            .to_dict()
            )


def format_data(data_df: pd.DataFrame, code_dict: dict[str, str]) -> pd.DataFrame:
    """Format HDR data.

    This function takes the raw dataframe and:
    - melts the data to long format
    - creates a year columnand new variable column by splitting on the last underscore
        (variables contain year such as "hdi_rank_2019")
    - creates a variable_name column by mapping the variable to the code_dict

    Args:
        data_df (pd.DataFrame): Raw dataframe to be formatted.
        code_dict (dict): Dictionary of variable codes and names.
    """

    return (data_df
            .melt(id_vars=['iso3', 'country', 'hdicode', 'region'])
            .assign(year=lambda d: d['variable'].str.rsplit("_", n=1).str[-1].astype(int),
                    variable=lambda d: d['variable'].str.rsplit("_", n=1).str[0],
                    variable_name=lambda d: d['variable'].map(code_dict))
            )


def get_hdr_data() -> pd.DataFrame:
    """Get UNDP Human Development Report data.

    This function returns the formatted HDR data as a dataframe.
    """

    links = get_data_links()  # get links to data and metadata

    # read metadata and create dictionary
    code_dict = (read_data(links['metadata_url'])
                 .pipe(create_code_dict)
                 )

    # read data and format it
    data_df = read_data(links['data_url']).pipe(format_data, code_dict)

    return data_df








