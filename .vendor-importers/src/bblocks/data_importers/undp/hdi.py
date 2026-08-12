"""Human Development Index (HDI) data importer.

The Human Development Index (HDI) is a summary measure of average achievement in key dimensions of human development

More information and access to the raw data can be found at: https://hdr.undp.org/

This importer provides functionality to easily access the latest HDI data.

Usage:
First instantiate an importer object:
>>> hdi = HumanDevelopmentIndex()

get the latest data:
>>> data = hdi.get_data()

To get the metadata, call:
>>> metadata = hdi.get_metadata()

The data is cached to avoid downloading it multiple times. To clear the cache, call:
>>> hdi.clear_cache()
"""

import pandas as pd
import requests
import io
import numpy as np

from bblocks.data_importers.config import logger, DataExtractionError, Fields
from bblocks.data_importers.data_validators import DataFrameValidator
from bblocks.data_importers.utilities import convert_dtypes


DATA_URL = "https://hdr.undp.org/sites/default/files/2023-24_HDR/HDR23-24_Composite_indices_complete_time_series.csv"  # HDI data URL TODO: add functionality to dinamically get the latest URL as url link will likely change in the future
METADATA_URL = "https://hdr.undp.org/sites/default/files/2023-24_HDR/HDR23-24_Composite_indices_metadata.xlsx"  # HDI metadata URL TODO: add functionality to dinamically get the latest URL as url link will likely change in the future
DATA_ENCODING = "latin1"  # Encoding used by the HDI data


def _request_hdi_data(url: str, *, timeout: int) -> requests.Response:
    """Request the HDI data from the URL.

    Args:
        url: URL to request the HDI data from.
        timeout: Timeout for the request in seconds

    Returns:
        Response object containing the HDI data.
    """

    logger.debug("Requesting HDI data")

    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response

    except Exception as e:
        raise DataExtractionError(f"Error requesting HDI data: {e}") from e


def read_hdi_data(*, encoding: str = DATA_ENCODING, timeout: int = 30) -> pd.DataFrame:
    """Read the HDI data from the response.
    Args:
        encoding: Encoding used by the HDI data.
        timeout: Timeout for the request in seconds

    Returns:
        The raw HDI data DataFrame.
    """

    logger.debug("Reading HDI data")

    try:
        response = _request_hdi_data(DATA_URL, timeout=timeout)
        data = pd.read_csv(io.BytesIO(response.content), encoding=encoding)
        return data

    except (pd.errors.ParserError, ValueError) as e:
        raise DataExtractionError(f"Error reading HDI data: {e}") from e


def read_hdi_metadata(*, timeout: int = 30) -> pd.DataFrame:
    """Read the HDI metadata from the response.

    Args:
        timeout: Timeout for the request in seconds

    Returns:
        The raw HDI metadata DataFrame
    """

    logger.debug("Reading HDI metadata")

    try:
        response = _request_hdi_data(METADATA_URL, timeout=timeout)
        metadata = pd.read_excel(io.BytesIO(response.content))
        return metadata

    except (pd.errors.ParserError, ValueError) as e:
        raise DataExtractionError(f"Error reading HDI metadata: {e}") from e


def clean_metadata(metadata_df: pd.DataFrame) -> pd.DataFrame:
    """Clean the HDI metadata DataFrame.

    - Drop irrelevant columns (where "Time series" is NaN)
    - Rename columns to match the Fields class
    - Convert the "Time series" column to string
    - Convert the DataFrame to the correct dtypes

    Args:
        metadata_df: The HDI metadata DataFrame.

    Returns:
        The cleaned HDI metadata DataFrame.
    """

    return (
        metadata_df.dropna(subset="Time series")
        .rename(
            columns={
                "Full name": Fields.indicator_name,
                "Short name": Fields.indicator_code,
                "Time series": Fields.time_range,
                "Note": Fields.notes,
            }
        )
        .assign(time_range=lambda d: d.time_range.astype(str))
        .pipe(convert_dtypes)
    )


def clean_data(data_df: pd.DataFrame, metadata_df: pd.DataFrame) -> pd.DataFrame:
    """Clean the HDI data DataFrame.

    - Rename columns to match the Fields class
    - Melt the DataFrame to long format
    - Split the indicator code and year into separate columns
    - Map the indicator code to the indicator name
    - Convert the DataFrame to the correct dtypes

    Args:
        data_df: The HDI data DataFrame.
        metadata_df: The HDI metadata DataFrame.

    Returns:
        The cleaned HDI data DataFrame.
    """

    return (
        data_df.rename(
            columns={
                "iso3": Fields.entity_code,
                "country": Fields.entity_name,
                "region": Fields.region_code,
                "hdicode": "hdi_group",
            }
        )
        .melt(
            id_vars=[
                Fields.entity_code,
                Fields.entity_name,
                Fields.region_code,
                "hdi_group",
            ],
            var_name=Fields.indicator_code,
            value_name=Fields.value,
        )
        .assign(
            split=lambda d: d.indicator_code.apply(
                lambda x: x.rsplit("_", 1) if "_" in x else [x, np.nan]
            )
        )
        .assign(
            indicator_code=lambda x: x["split"].str[0],
            year=lambda x: x["split"].str[1].astype("int", errors="ignore"),
        )
        .drop(columns=["split"])
        .assign(
            indicator_name=lambda d: d.indicator_code.map(
                metadata_df.set_index("indicator_code")["indicator_name"].to_dict()
            )
        )
        .pipe(convert_dtypes)
    )


class HumanDevelopmentIndex:
    """A class to import Human Development Index (HDI) data from UNDP.

    This class provides methods to access HDI data and metadata.
    The Human Development Index (HDI) is a summary measure of average achievement in key dimensions of human development
    More information and access to the raw data can be found at: https://hdr.undp.org/

    Attributes:
        timeout: Timeout for the request in seconds (Optional). By default, it is set to 30 seconds.

    Usage:

    First instantiate an importer object:
    >>> hdi = HumanDevelopmentIndex()

    get the latest data:
    >>> data = hdi.get_data()

    To get the metadata, call:
    >>> metadata = hdi.get_metadata()

    The data is cached to avoid downloading it multiple times. To clear the cache, call:
    >>> hdi.clear_cache()
    """

    def __init__(self, *, timeout: int = 30):
        self._timeout = timeout  # Timeout for the request in seconds
        self._data_df: pd.DataFrame | None = None
        self._metadata_df: pd.DataFrame | None = None

    def __repr__(self) -> str:
        """String representation of the HumanDevelopmentIndex object"""

        imported = True if self._data_df is not None else False
        return (
            f"{self.__class__.__name__}("
            f"timeout={self._timeout}, "
            f"data imported = {imported!r}"
            f")"
        )

    def _extract_metadata(self):
        """Extract HDI metadata"""

        logger.info("Extracting HDI metadata")

        metadata_df = read_hdi_metadata(
            timeout=self._timeout
        )  # Read the HDI metadata from the source
        metadata_df = clean_metadata(metadata_df)  # Clean the HDI metadata
        DataFrameValidator().validate(
            metadata_df, ["indicator_name", "indicator_code"]
        )  # Validate the HDI metadata
        self._metadata_df = metadata_df  # Save the HDI metadata to the object

    def _extract_data(self) -> None:
        """Extract HDI data"""

        logger.info("Extracting HDI data")

        df = read_hdi_data(timeout=self._timeout)  # Read the HDI data from the source

        # Check if the HDI metadata is already extracted, if not then extract it
        if self._metadata_df is None:
            self._extract_metadata()

        df = clean_data(df, self._metadata_df)  # Clean the HDI data
        DataFrameValidator().validate(
            df,
            [
                "indicator_code",
                "indicator_name",
                "year",
                "value",
                "entity_code",
                "entity_name",
            ],
        )  # Validate the HDI data
        self._data_df = df  # Save the HDI data to the object

    def get_metadata(self) -> pd.DataFrame:
        """Get the HDI metadata

        This method will return the HDI metadata DataFrame with indicator names, codes, and time ranges and any notes

        Returns:
            The HDI metadata DataFrame
        """

        if self._metadata_df is None:
            self._extract_metadata()
        return self._metadata_df

    def get_data(self) -> pd.DataFrame:
        """Get the HDI data

        This method will return the HDI data DataFrame

        Returns:
            The HDI data DataFrame.
        """

        if self._data_df is None:
            self._extract_data()
        return self._data_df

    def clear_cache(self) -> None:
        """Clear the cached data and metadata."""

        self._data_df = None
        self._metadata_df = None

        logger.info("Cache cleared")
