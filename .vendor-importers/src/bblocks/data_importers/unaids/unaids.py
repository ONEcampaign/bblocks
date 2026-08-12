"""UNAIDS data importer.

This module provides a class to import data from the UNAIDS website.
Data is fetched from the UNAIDS website: https://aidsinfo.unaids.org/
Bulk download urls are available in the Fact Sheets section of the website.

NOTE: the AidsInfo website does not have a valid SSL certificate, so we disable the SSL verification by default.
"""

import requests
import io
from zipfile import ZipFile
import pandas as pd
from typing import Literal
import urllib3

from bblocks.data_importers.config import logger, DataExtractionError, Fields
from bblocks.data_importers.utilities import convert_dtypes
from bblocks.data_importers.data_validators import DataFrameValidator

# Disable warnings - UNAIDS does not have a valid SSL certificate
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

YEAR = 2024  # The year of the last data update - used in the URLs.

URLS = {
    "Estimates": f"https://aidsinfo.unaids.org/documents/Estimates_{YEAR}_en.zip",
    "Laws and Policies": f"https://aidsinfo.unaids.org/documents/NCPI_{YEAR}_en.zip",
    "Key Populations": f"https://aidsinfo.unaids.org/documents/KPAtlasDB_{YEAR}_en.zip",
    "GAM": f"https://aidsinfo.unaids.org/documents/GAM_{YEAR}_en.zip",
}


def get_response(url: str, verify: bool = False) -> requests.Response:
    """Gets a response from the given URL.

    Args:
        url: The URL to get the response from.
        verify: Whether to verify the SSL certificate. Default is False (UNAIDS does not have a valid SSL certificate).

    Returns:
        A requests.Response object.

    Raises:
        requests.exceptions.RequestException: If the request fails.
    """

    logger.debug(f"Request URL: {url}")

    response = requests.get(url, verify=verify)
    response.raise_for_status()

    return response


def read_csv_from_zip_response(response: requests.Response) -> pd.DataFrame:
    """Reads a CSV file from a zip response.

    Args:
        response: The requests.Response object containing the zip file.

    Returns:
        A pandas DataFrame containing the CSV data.

    Raises:
        ValueError: If the zip file contains multiple files or no files.
    """

    logger.debug("Reading CSV file from zip response.")

    with ZipFile(io.BytesIO(response.content)) as zfile:

        files = zfile.namelist()

        # check the length of the files - must only contain 1 file
        if len(files) > 1:
            raise DataExtractionError("Multiple files found in the zipfile")
        if len(files) == 0:
            raise DataExtractionError("No files found in the zipfile")

        with zfile.open(files[0]) as csvfile:
            return pd.read_csv(csvfile)


def format_data(df: pd.DataFrame) -> pd.DataFrame:
    """Formats the data from UNAIDS.

    - Renames the columns to match the Fields class.
    - Converts the dtypes to pyarrow.

    Args:
        df: The DataFrame to format.

    Returns:
        A pandas DataFrame with the formatted data.

    """

    cols = {
        "Indicator": Fields.indicator_name,
        "Unit": Fields.unit,
        "Subgroup": "subgroup",
        "Area": Fields.entity_name,
        "Area ID": Fields.entity_code,
        "Time Period": Fields.year,
        "Source": Fields.source,
        "Data value": Fields.value,
        "Formatted": "value_formatted",
        "Footnote": Fields.footnote,
    }

    # rename the columns
    df.rename(columns=cols, inplace=True)
    # convert the dtypes
    df = convert_dtypes(df)

    return df


class UNAIDS:
    """Import for UNAIDS data

    UNAIDS in a joint United Nations initiative with UNICEF and WHO provides
    key data on HIV/AIDS. The data is available in the UNAIDS website
    at AidsInfo: https://aidsinfo.unaids.org/

    Data is available in the following datasets:
    - Estimates: HIV estimated indicatiors
    - Laws and Policies: Laws and policies related to HIV
    - Key Populations: Key populations data
    - GAM: Global AIDS Monitoring data

    The data is generally updated once a year.

    Usage:

    To use this importer, first instantiate an object:
    >>> unaids = UNAIDS()

    To get the data, call the get_data() method:
    >>> data = unaids.get_data()

    By default it will return the Estimates dataset. You can specify the
    dataset you want to get by passing the dataset name as an argument:
    >>> data = unaids.get_data(dataset="Laws and Policies")
    >>> data = unaids.get_data(dataset="Key Populations")
    >>> data = unaids.get_data(dataset="GAM")

    The data is cached to avoid downloading it multiple times. To clear the
    cache, call the clear_cache() method:
    >>> unaids.clear_cache()

    Currently the AidsInfo website does not have a valid SSL certificate, so we disable the SSL verification
    by default. A user can enable veriication by passing the verify=True when instantiating the class.
    >>> unaids = UNAIDS(verify=True)

    Please verify your security preferences before requesting data.

    Developer Notes:
    The data is requested from the UNAIDS website where download urls are subject to change. Please
    open an issue if you find any issues getting the data.
    """

    def __init__(self, verify_ssl: bool = False):

        self._data: dict = {
            "Estimates": None,
            "Laws and Policies": None,
            "Key Populations": None,
            "GAM": None,
        }

        self.verify_ssl = verify_ssl
        if self.verify_ssl:
            logger.warning(
                "SSL verification is enabled. This may cause issues requesting data from UNAIDS."
            )
            logger.warning(
                " If you encounter SSL errors, please disable SSL verification by setting verify_ssl=False."
            )

        else:
            logger.warning(
                "SSL verification is disabled. This is required to request data from UNAIDS."
                " Please verify your security preferences before requesting data."
            )

    def __repr__(self) -> str:
        """String representation of the UNAIDS object."""

        imported = [name for name, df in self._data.items() if df is not None]
        return (
            f"{self.__class__.__name__}("
            f"verify_ssl={self.verify_ssl!r}, "
            f"imported datasets = {imported!r}"
            f")"
        )

    def _load_data(
        self,
        dataset: Literal["Estimates", "Laws and Policies", "Key Populations", "GAM"],
    ) -> None:
        """Load the data from UNAIDS to the object

        Args:
            dataset: The dataset to load. By default, it is "Estimates".
                Options are:
                - "Estimates": HIV estimated indicatiors
                - "Laws and Policies": Laws and policies related to HIV
                - "Key Populations": Key populations data
                - "GAM": Global AIDS Monitoring data
                For more information, see the UNAIDS website.
        """

        logger.info(f"Loading dataset: {dataset}")

        # pipeline to get the data
        response = get_response(
            URLS[dataset], verify=self.verify_ssl
        )  # get the response from the URL
        df = read_csv_from_zip_response(
            response
        )  # read the CSV file from the zip response
        df = format_data(df)  # format the data

        # validate the data
        DataFrameValidator().validate(
            df,
            required_cols=[
                Fields.year,
                Fields.entity_code,
                Fields.entity_name,
                Fields.indicator_name,
                Fields.value,
            ],
        )
        self._data[dataset] = df

    def get_data(
        self,
        dataset: Literal[
            "Estimates", "Laws and Policies", "Key Populations", "GAM"
        ] = "Estimates",
    ) -> pd.DataFrame:
        """Get UNAIDS data for a specific dataset

        Args:
            dataset: The dataset to get. By default, it is "Estimates".
                Options are:
                - "Estimates": HIV estimated indicatiors
                - "Laws and Policies": Laws and policies related to HIV
                - "Key Populations": Key populations data
                - "GAM": Global AIDS Monitoring data
                For more information, see the UNAIDS website.

        Returns:
            A pandas DataFrame containing the data.
        """

        if dataset not in self._data:
            raise ValueError(
                f"Invalid dataset: {dataset}. Available datasets are: {list(self._data.keys())}"
            )

        if self._data[dataset] is None:
            self._load_data(dataset)

        return self._data[dataset]

    def clear_cache(self) -> None:
        """Clear the cache of the data"""

        self._data = {key: None for key in self._data}
        logger.info("Cache cleared.")
