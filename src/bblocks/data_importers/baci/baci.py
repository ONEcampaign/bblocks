"""BACI importer

This module provides an importer for the CEPII-BACI dataset, which contains
annual data on bilateral trade flows at the product level, classified using the
Harmonized System (HS). The BACI dataset harmonizes and reconciles trade data
from the United Nations COMTRADE database.
"""

import pandas as pd
from diskcache import Cache
from platformdirs import user_cache_dir
import atexit
import requests
from bs4 import BeautifulSoup

from bblocks.data_importers.config import Fields, logger, DataExtractionError
from bblocks.data_importers.data_validators import DataFrameValidator
from bblocks.data_importers.utilities import convert_dtypes
from bblocks.data_importers.baci.data_manager import BaciDataManager


_CACHE_EXPIRY_SECONDS: int = 48 * 60 * 60  # cache expiry after 48 hours
_CACHE_DIR = user_cache_dir("bblocks/baci")
_DATA_CACHE = Cache(_CACHE_DIR, size_limit=1e12)
_DATA_CACHE.stats(enable=True)  # Enable hit/miss tracking

# Ensure cache is properly closed on exit to persist WAL data to disk
atexit.register(_DATA_CACHE.close)


# URL to the BACI data page
URL: str = "https://www.cepii.fr/DATA_DOWNLOAD/baci/doc/baci_webpage.html"


def _get_soup() -> BeautifulSoup:
    """Request the CEPII BACI page and return a BeautifulSoup object."""

    try:
        logger.debug(f"Fetching soup for BACI page")
        response = requests.get(URL)
        response.raise_for_status()

        return BeautifulSoup(response.content, "html.parser")

    except requests.RequestException as e:
        raise DataExtractionError(f"Failed to fetch BACI page: {e}")


def _parse_data_links(soup: BeautifulSoup) -> dict[str, str]:
    """Parse the BACI data links from the BeautifulSoup object.

    Finds the section with download links id="download-links" and extracts all links
    that start with "HS".

    Args:
        soup: BeautifulSoup object of the BACI page

    Returns:
        A dictionary mapping the HS version to the download link.
    """

    try:
        logger.debug(f"Parsing BACI data links")

        # find the section with download links
        section = soup.find("section", {"id": "download-links"})

        data_links_dict = {
            a.text: a["href"] for a in section.find_all("a") if a.text.startswith("HS")
        }

        if not data_links_dict:
            raise DataExtractionError("No BACI data links found")

        return data_links_dict

    except Exception as e:
        raise DataExtractionError(f"Failed to parse BACI data links: {e}")


@_DATA_CACHE.memoize(expire=_CACHE_EXPIRY_SECONDS)
def extract_data_links() -> dict[str, str]:
    """Extract the BACI data links from the CEPII BACI page.

    HS versions and data links are cached to improve performance and reduce redundant requests.
    Cache will persist for 48 hours.

    Returns:
        A dictionary mapping the HS version to the download link.
    """

    logger.info(f"Extracting BACI data links")

    soup = _get_soup()
    return _parse_data_links(soup)


def _add_product_labels(
    df: pd.DataFrame, data_manager: BaciDataManager
) -> pd.DataFrame:
    """Add product labels to the data DataFrame

    Returns:
        DataFrame with product labels added
    """

    return df.merge(
        data_manager.product_codes,
        how="left",
        on=Fields.product_code,
        validate="many_to_one",
    )


def _add_country_labels(
    df: pd.DataFrame, data_manager: BaciDataManager
) -> pd.DataFrame:
    """Add country labels to the data DataFrame including country name and ISO3 code

    Returns:
        DataFrame with country labels added
    """

    return df.merge(
        data_manager.country_codes.rename(
            columns={
                Fields.country_code: Fields.exporter_code,
                Fields.country_name: Fields.exporter_name,
                Fields.iso3_code: Fields.exporter_iso3_code,
            }
        ),
        how="left",
        on=Fields.exporter_code,
        validate="many_to_one",
    ).merge(
        data_manager.country_codes.rename(
            columns={
                Fields.country_code: Fields.importer_code,
                Fields.country_name: Fields.importer_name,
                Fields.iso3_code: Fields.importer_iso3_code,
            }
        ),
        how="left",
        on=Fields.importer_code,
        validate="many_to_one",
    )


def _validate_hs_version(hs_version: str) -> str:
    """Validate a user provided HS version.

    String cleaning - remove leading/trailing whitespace and convert to uppercase.
    Validation - check if the HS version is available in the BACI dataset.

    Raises:
        ValueError: If the HS version is not available/valid.

    Returns:
        The cleaned HS version string.
    """

    hs_version_cleaned = hs_version.strip().upper()
    available_versions = extract_data_links().keys()

    if hs_version_cleaned not in available_versions:
        raise ValueError(
            f"HS version {hs_version} not available. "
            f"Available versions: {list(available_versions)}"
        )

    return hs_version_cleaned


@_DATA_CACHE.memoize(expire=_CACHE_EXPIRY_SECONDS)
def extract_data(hs_version: str) -> BaciDataManager:
    """Helper function to load data for a specific HS version

    Data is cached to improve performance and reduce redundant downloads.
    Cache will persist for 48 hours.

    Args:
        hs_version: The HS version to extract data for (e.g., "HS22")

    Returns:
        A dictionary containing:
            - data: DataFrame with trade data
            - country_codes: DataFrame with country codes
            - product_codes: DataFrame with product codes
            - metadata: Dictionary with metadata
    """

    # get available data links
    data_links = extract_data_links()

    # Extract and parse data

    logger.info(f"Extracting BACI data for HS version {hs_version}")
    data_manager = BaciDataManager(hs_version=hs_version, url=data_links[hs_version])
    data_manager.extract()  # extract and read data

    # validation checks
    logger.debug("Validating BACI data")
    validator = DataFrameValidator()
    validator.validate(
        data_manager.data,
        required_cols=[
            Fields.year,
            Fields.exporter_code,
            Fields.importer_code,
            Fields.product_code,
            Fields.value,
            Fields.quantity,
        ],
    )

    validator.validate(
        data_manager.product_codes,
        required_cols=[Fields.product_code, Fields.product_description],
    )
    validator.validate(
        data_manager.country_codes,
        required_cols=[Fields.country_code, Fields.country_name, Fields.iso3_code],
    )

    # validate metadata
    if not data_manager.metadata:
        raise DataExtractionError("No metadata found after parsing")

    return data_manager


class BACI:
    """importer for CEPII-BACI data

    BACI provides annual data on bilateral trade flows at the product level,
    with products classified using the Harmonized System (HS). BACI harmonizes and
    reconciles trade data from the United Nations COMTRADE database.

    Visit the BACI website for more information: https://www.cepii.fr/DATA_DOWNLOAD/baci/doc/baci_webpage.html

    Usage:

    Instantiate the BACI importer:
    >>> baci = BACI()

    Get available HS versions:
    >>> baci.available_hs_versions()

    Get trade data:
    >>> df = baci.get_data()

    By default, the HS22 version is used. You can specify a different HS version:
    >>> df_hs12 = baci.get_data(hs_version="HS12")

    You can also choose to include product and country labels:
    >>> df_with_labels = baci.get_data(include_product_labels=True, include_country_labels=True)

    Note: Including labels increases memory usage. It may be preferable to join labels separately.
    You can retrieve product and country codes separately:
    >>> product_codes = baci.get_product_codes()
    >>> country_codes = baci.get_country_codes()

    You can equally specify the HS version when retrieving codes:
    >>> product_codes_hs12 = baci.get_product_codes(hs_version="HS12")
    >>> country_codes_hs12 = baci.get_country_codes(hs_version="HS12")

    Get metadata:
    >>> metadata = baci.get_metadata()

    Get metadata for a specific HS version:
    >>> metadata_hs12 = baci.get_metadata(hs_version="HS12")

    Data is cached by default to improve performance and to reduce redundant downloads.
    Cached data will persist for 48 hours. You can clear the cache if needed:
    >>> baci.clear_cache()
    """

    def __init__(self):

        self._hs_versions: dict | None = None
        self._data: dict[str, BaciDataManager] = dict()

    def _load_data(self, hs_version: str) -> None:
        """Load data to object. This method checks if data for the specified HS version
        is already loaded. If not, it extracts and loads the data.

        Args:
            hs_version: The HS version to load data for (e.g., "HS22")
        """

        hs_version = _validate_hs_version(hs_version)  # validate HS version

        if hs_version not in self._data:
            self._data[hs_version] = extract_data(hs_version)
            logger.info(f"Successfully loaded BACI data for HS version {hs_version}")

    def available_hs_versions(self) -> list[str]:
        """Get a list of available HS versions in the BACI dataset

        Returns:
            A list of available HS versions as strings.
        """

        if self._hs_versions is None:
            self._hs_versions = extract_data_links()

        return list(self._hs_versions.keys())

    def get_data(
        self,
        hs_version: str = "HS22",
        include_product_labels: bool = False,
        include_country_labels: bool = False,
    ) -> pd.DataFrame:
        """Get the BACI trade data DataFrame for the specified HS version

        Args:
            hs_version: The HS version to get data for (default is "HS22"). To see available versions, use `available_hs_versions()`.
            include_product_labels: Whether to include product labels in the DataFrame (default is False).
            include_country_labels: Whether to include country labels in the DataFrame (default is False).

        Returns:
            A DataFrame containing the BACI trade data for the specified HS version.
        """
        hs_version = _validate_hs_version(hs_version)
        self._load_data(hs_version)

        df = self._data[hs_version].data

        if include_product_labels:
            df = _add_product_labels(df, self._data[hs_version])

        if include_country_labels:
            df = _add_country_labels(df, self._data[hs_version])

        return df

    def get_country_codes(self, hs_version: str = "HS22") -> pd.DataFrame:
        """Get the country codes for the specified HS version, including country names and ISO3 codes.

        Args:
            hs_version: The HS version to get data for (default is "HS22"). To see available versions, use `available_hs_versions()`.

        Returns:
            A DataFrame containing the country codes for the specified HS version.
        """

        hs_version = _validate_hs_version(hs_version)
        self._load_data(hs_version)

        return self._data[hs_version].country_codes

    def get_product_codes(self, hs_version: str = "HS22") -> pd.DataFrame:
        """Get the product codes for the specified HS version, including product descriptions.

        Args:
            hs_version: The HS version to get data for (default is "HS22"). To see available versions, use `available_hs_versions()`.

        Returns:
            A DataFrame containing the product codes for the specified HS version.
        """

        hs_version = _validate_hs_version(hs_version)
        self._load_data(hs_version)

        return self._data[hs_version].product_codes

    def get_metadata(self, hs_version: str = "HS22") -> dict:
        """Get metadata for the specified HS version

        Args:
            hs_version: The HS version to get data for (default is "HS22"). To see available versions, use `available_hs_versions()`.

        Returns:
            A dictionary containing metadata for the specified HS version.
        """

        hs_version = _validate_hs_version(hs_version)
        self._load_data(hs_version)

        return self._data[hs_version].metadata

    def clear_cache(self) -> None:
        """Clear the cached World Bank data."""

        _DATA_CACHE.clear()
        self._hs_versions = None
        self._data = dict()

        logger.info("BACI cache cleared.")
