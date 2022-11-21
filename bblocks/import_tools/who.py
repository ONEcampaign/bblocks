"""Tools to extract data from WHO"""

import pandas as pd
import requests
import numpy as np
import os

from bblocks.import_tools.common import ImportData

GHED_URL = "https://apps.who.int/nha/database/Home/IndicatorsDownload/en"


def extract_ghed() -> dict[str:pd.DataFrame]:
    """Extract GHED dataset"""

    try:
        req = requests.get(GHED_URL)
        data = pd.read_excel(req.content, sheet_name="Data")
        code_book = pd.read_excel(req.content, sheet_name="Codebook")
        metadata = pd.read_excel(req.content, sheet_name="Metadata")

        return {'data': data, 'code_book': code_book, 'metadata': metadata}
    except ConnectionError:
        raise ConnectionError("Could not connect to WHO GHED database")


def _clean_codes(df: pd.DataFrame) -> pd.DataFrame:
    """Clean codes"""

    return (df.rename(columns={'Indicator short code': 'indicator_code',
                               'Indicator name': 'indicator_name',
                               'Category 1': 'category_1',
                               'Category 2': 'category_2',
                               'Indicator units': 'indicator_units',
                               'Indicator currency': 'indicator_currency',
                               })
            .replace({'-': np.nan}))


def _clean_data(df: pd.DataFrame) -> pd.DataFrame:
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

    ghed_data = extract_ghed()
    df = _clean_data(ghed_data['data'])
    labels = _clean_codes(ghed_data['code_book'])
    metadata = _clean_metadata(ghed_data['metadata'])

    df = pd.merge(df, labels, on='indicator_code', how='left')  # add lables to data
    df = pd.merge(df, metadata, on=['country_code', 'indicator_code'], how='left')  # add metadata to data

    df.to_csv(f'{path}_ghed.csv', index=False)


class GHED(ImportData):
    """An object to extract GHED data

    To use, create an instance of the class and call the load_indicator method.
    If the data is already downloaded, it will be loaded from disk. If not, it will be downloaded.
    If `update_data` is set to True, the data will be downloaded regardless of whether it is already on disk.
    To force an update, call the update method.
    To get the data, call the get_data method. If `include_metadata` is set to True, the metadata will be included.
    """

    def load_indicator(self) -> ImportData:
        """Load GHED data

        Returns:
            The same object to allow chaining
        """

        if not os.path.exists(f'{self.data_path}_ghed.csv') or self.update_data:
            download_ghed(self.data_path)

        self.data = pd.read_csv(f'{self.data_path}_ghed.csv')
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
            self.data = pd.read_csv(f'{self.data_path}_ghed.csv')
        return self

    def get_data(self, include_metadata=False):
        """Get GHED data as a pandas dataframe

        Args:
            include_metadata: Whether to include the metadata in the dataframe. Default is False.

        Returns:
            A pandas dataframe with the data
        """

        if include_metadata:
            return self.data
        else:
            return self.data[['country_name', 'country_code', 'region', 'income_group', 'year',
                              'indicator_code', 'indicator_name', 'indicator_units', 'indicator_currency', 'value']]
