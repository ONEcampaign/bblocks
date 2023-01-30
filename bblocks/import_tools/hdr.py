"""Tools to import UNDP Human Development Report data.

`get_hdr_data` returns a dataframe with all HDR data.
Use the `HDR` object to extract and interact with the data
"""

import pandas as pd
import requests
import io
from bs4 import BeautifulSoup
import os

from bblocks.import_tools.common import ImportData


BASE_URL: str = 'https://hdr.undp.org/data-center/documentation-and-downloads'


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


# dictionary of composite indices with their variables
hdr_indicators: dict[str, list] = {'hdi': ['hdi_rank', 'hdi', 'le', 'eys', 'mys', 'gnipc'],
                                   'gdi': ['gdi_group', 'gdi', 'hdi_f', 'le_f', 'eys_f', 'mys_f',
                                           'gni_pc_f',
                                           'hdi_m', 'le_m', 'eys_m', 'mys_m', 'gni_pc_m'],
                                   'ihdi': ['ihdi', 'coef_ineq', 'loss', 'ineq_le', 'ineq_edu',
                                            'ineq_inc'],
                                   'gii': ['gii_rank', 'gii', 'mmr', 'abr', 'se_f', 'se_m', 'pr_f',
                                           'pr_m', 'lfpr_f', 'lfpr_m'],
                                   'phdi': ['rankdiff_hdi_phdi', 'phdi', 'diff_hdi_phdi',
                                            'co2_prod', 'mf']
                                   }


class HDR(ImportData):
    """Import UNDP Human Development Report data.

    To use, instantiate the class. Call the `load_indicator` method to load the data
    to the object. If the data has already been downloaded, it will be read from disk, otherwise
    it will be downloaded from the UNDP website. Optionally set the attribute `update_data` to
    True to force the data to be downloaded. To force an update of the data, call the `update`
    method. To get the data, call the `get_data` method. To see a list of avilable composite indices,
    call the `available_composite_indices` method. To see a list of available indicators, call the
    `available_indicators` method.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.indicators = hdr_indicators  # set indicators

    def available_composite_indices(self) -> list[str]:
        """See available composite indices"""

        return list(self.indicators.keys())

    def available_indicators(self, composite_index: str = None) -> list[str]:
        """See available indicators

        Args:
            composite_index (str): Composite index to see available indicators for. If None, all
        """

        if composite_index is None:
            return [val for sublist in hdr_indicators.values() for val in sublist]

        if composite_index not in self.indicators:
            raise ValueError(f'Composite index {composite_index} not found')

        return self.indicators[composite_index]

    def load_indicator(self) -> ImportData:
        """Load HDR data to the object

        If the data has not been downloaded or if `update_data` is True, the data will be
        downloaded from the UNDP website. Otherwise, the data will be read from disk.

        Returns:
            Same object to allow chaining
        """

        # check is data does not exist or if `update_data` is True. Update if condition passes.
        if not os.path.exists(f"{self.data_path}/HDR.csv") or self.update_data:
            self.update()

        self.data = pd.read_csv(f"{self.data_path}/HDR.csv")
        return self

    def update(self) -> None:
        """Force an update to the HDR data in the object and on disk."""

        hdr_data = get_hdr_data()
        self.data = hdr_data
        hdr_data.to_csv(f"{self.data_path}/HDR.csv", index=False)

    def get_data(self, composite_index: str = None, indicator: str = None) -> pd.DataFrame:
        """Get HDR data as a pandas dataframe

        Specify either a composite index or an indicator. If both are specified, an error will be
        raised. If neither are specified, all data will be returned.

        Args:
            composite_index (str): Composite index to get data for.
            indicator (str): Indicator to get data for.

        Returns:
            pd.DataFrame: Dataframe with HDR data.


        """

        # check if composite index and indicator is both specified
        if composite_index is not None and indicator is not None:
            raise ValueError("Please specify either index or indicator, not both.")

        # return composite index data
        if composite_index is not None:
            if composite_index not in self.indicators.keys():
                raise ValueError(f"Composite index {composite_index} not found.")
            return (self.data[self.data['variable'].isin(self.indicators[composite_index])]
                    .reset_index(drop=True)
                    )

        # return single indicator data
        if indicator is not None:
            if indicator not in self.data['variable'].unique():
                raise ValueError(f"Indicator {indicator} not found.")
            return (self.data[self.data['variable'] == indicator]
                    .reset_index(drop=True)
                    )

        # return all data
        return self.data
