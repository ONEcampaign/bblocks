"""Tools to extract data from WHO"""

import pandas as pd
import requests
import numpy as np
import os

from bblocks.import_tools.common import ImportData

GHED_URL = "https://apps.who.int/nha/database/Home/IndicatorsDownload/en"


def extract_ghed_data() -> bytes:
    """Extract GHED dataset"""

    try:
        return requests.get(GHED_URL).content

    except ConnectionError:
        raise ConnectionError("Could not connect to WHO GHED database")


def _clean_ghed_codes(df: pd.DataFrame) -> pd.DataFrame:
    """Clean GHED codes"""

    return (df.rename(columns={'Indicator short code': 'indicator_code',
                               'Indicator name': 'indicator_name',
                               'Category 1': 'category_1',
                               'Category 2': 'category_2',
                               'Indicator units': 'indicator_units',
                               'Indicator currency': 'indicator_currency',
                               })
            .replace({'-': np.nan}))


def _clean_ghed_data(df: pd.DataFrame) -> pd.DataFrame:
    """ Clean GHED data dataframe"""

    return (df.melt(id_vars=['country', 'country code', 'region (WHO)', 'income group', 'year'],
                    var_name='indicator_code')
            .rename(columns={'country': 'country_name',
                             'country code': 'country_code',
                             'region (WHO)': 'region',
                             'income group': 'income_group',
                             }))


def _clean_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Clean GHED metadata"""

    return (df.drop(columns=['country', 'region (WHO)', 'Income group', 'Indicator name'])
            .rename(columns={'country code': 'country_code',
                             'Indicator short code': 'indicator_code',
                             'Sources': 'source',
                             'Comments': 'comments',
                             'Methods of estimation': 'methods_of_estimation',
                             'Data type': 'data_type',
                             'Footnote': 'footnote'

                             })
            )


def download_ghed(path: str) -> None:
    """Download GHED dataset to disk"""

    ghed_content = extract_ghed_data()

    # data
    data = pd.read_excel(ghed_content, sheet_name="Data").pipe(_clean_ghed_data)
    codes = pd.read_excel(ghed_content, sheet_name="Codebook").pipe(_clean_ghed_codes)
    pd.merge(data, codes, on='indicator_code', how='left').to_feather(os.path.join(path, 'ghed_data.feather'))

    # metadata
    (pd.read_excel(ghed_content, sheet_name="Metadata")
     .pipe(_clean_metadata).to_feather(os.path.join(path, 'ghed_metadata.feather'))
     )


class GHED(ImportData):
    """An object to extract GHED data

    To use, create an instance of the class and call the load_indicator method.
    If the data is already downloaded, it will be loaded from disk. If not, it will be downloaded.
    If `update_data` is set to True, the data will be downloaded regardless of whether it is already on disk.
    To force an update, call the update method.
    To get the data, call the get_data method.
    To get the metadata, call the get_metadata method.
    """

    metadata: pd.DataFrame = None

    def load_indicator(self) -> ImportData:
        """Load GHED data

        Returns:
            The same object to allow chaining
        """

        if not os.path.exists(f'{self.data_path}\ghed_data.feather') or self.update_data:
            download_ghed(self.data_path)

        self.data = pd.read_feather(f'{self.data_path}\ghed_data.feather')
        self.metadata = pd.read_feather(f'{self.data_path}\ghed_metadata.feather')

        return self

    def update(self, reload_data: bool = True) -> ImportData:
        """Update GHED data

        Args:
            reload_data: Whether to reload the data to the object after updating it. Default is True.

        Returns:
            The same object to allow chaining
        """

        download_ghed(self.data_path)
        if reload_data:
            self.data = pd.read_feather(f'{self.data_path}\ghed_data.feather')
            self.metadata = pd.read_feather(f'{self.data_path}\ghed_metadata.feather')
        return self

    def get_data(self) -> pd.DataFrame:
        """Get GHED data as a pandas dataframe

        Returns:
            A pandas dataframe with the data
        """
        return self.data

    def get_metadata(self) -> pd.DataFrame:
        """Get GHED metadata as a pandas dataframe

        Returns:
            A pandas dataframe with the metadata
        """

        return self.metadata
