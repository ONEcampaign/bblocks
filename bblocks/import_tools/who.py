"""Tools to extract _data from WHO"""

import numpy as np
import pandas as pd
import requests

from bblocks.config import BBPaths
from bblocks.import_tools.common import ImportData

GHED_URL: str = "https://apps.who.int/nha/database/Home/IndicatorsDownload/en"


def extract_ghed_data() -> bytes:
    """Extract GHED dataset"""

    try:
        return requests.get(GHED_URL).content

    except ConnectionError:
        raise ConnectionError("Could not connect to WHO GHED database")


def _clean_ghed_codes(df: pd.DataFrame) -> pd.DataFrame:
    """Clean GHED codes"""

    return df.rename(
        columns={
            "variable code": "indicator_code",
            "Indicator short code": "indicator_code",
            "variable name": "indicator_name",
            "Indicator name": "indicator_name",
            "Category 1": "category_1",
            "Category 2": "category_2",
            "Indicator units": "indicator_units",
            "Indicator currency": "indicator_currency",
        }
    ).replace({"-": np.nan})


def _clean_ghed_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean GHED _data dataframe"""

    return df.rename(
        columns={
            "country": "country_name",
            "code": "country_code",
            "country code": "country_code",
            "income group": "income_group",
            "region (WHO)": "region",
            "region": "region",
            "income": "income_group",
        }
    ).melt(
        id_vars=["country_name", "country_code", "region", "income_group", "year"],
        var_name="indicator_code",
    )


def _clean_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Clean GHED metadata"""
    to_drop = [
        "country",
        "region (WHO)",
        "Income group",
        "Variable name",
        "Indicator name",
    ]
    keep = [col for col in df.columns if col not in to_drop]

    return df.filter(keep, axis=1).rename(
        columns={
            "code": "country_code",
            "country code": "country_code",
            "Variable code": "indicator_code",
            "Indicator short code": "indicator_code",
            "Sources": "source",
            "Comments": "comments",
            "Methods of estimation": "methods_of_estimation",
            "Data type": "data_type",
            "Footnote": "footnote",
        }
    )


def download_ghed() -> None:
    """Download GHED dataset to disk"""

    ghed_content = extract_ghed_data()

    # _data
    data = pd.read_excel(ghed_content, sheet_name="Data").pipe(_clean_ghed_data)
    codes = pd.read_excel(ghed_content, sheet_name="Codebook").pipe(_clean_ghed_codes)
    pd.merge(data, codes, on="indicator_code", how="left").to_feather(
        BBPaths.raw_data / "ghed_data.feather"
    )

    # metadata
    (
        pd.read_excel(ghed_content, sheet_name="Metadata")
        .pipe(_clean_metadata)
        .to_feather(BBPaths.raw_data / "ghed_metadata.feather")
    )


class GHED(ImportData):
    """An object to extract GHED _data

    To use, create an instance of the class and call the load_indicator method.
    If the _data is already downloaded, it will be loaded from disk. If not, it will be downloaded.
    If `update_data` is set to True, the _data will be downloaded regardless of whether it is already on disk.
    To force an update, call the update method.
    To get the _data, call the get_data method.
    To get the metadata, call the get_metadata method.
    """

    _metadata: pd.DataFrame = None

    def load_data(self) -> ImportData:
        """Load GHED data

        Returns:
            The same object to allow chaining
        """

        if not (BBPaths.raw_data / "ghed_data.feather").exists():
            download_ghed()

        self._raw_data = pd.read_feather(BBPaths.raw_data / "ghed_data.feather")
        self._metadata = pd.read_feather(BBPaths.raw_data / "ghed_metadata.feather")

        # Load all data
        self._data["all"] = self._raw_data

        return self

    def update_data(self, reload_data: bool) -> ImportData:
        """Update GHED _data

        Returns:
            The same object to allow chaining
        """

        download_ghed()

        if reload_data:
            self._raw_data = pd.read_feather(BBPaths.raw_data / "ghed_data.feather")
            self._metadata = pd.read_feather(BBPaths.raw_data / "ghed_metadata.feather")

            # Load all data
            self._data["all"] = self._raw_data

        return self

    def get_metadata(self) -> pd.DataFrame:
        """Get GHED metadata as a pandas dataframe

        Returns:
            A pandas dataframe with the metadata
        """

        return self._metadata
