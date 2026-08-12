"""Importer for the GHED database from WHO

The Global Health Expenditure Database (GHED) provides comparable data on health expenditure
across various countries and years. See more details and access the raw data at: https://apps.who.int/nha/database

This importer provides functionality to easily get data and metadata from the GHED database.

Usage:

First instantiate an importer object:
>>> ghed = GHED()

get the data and metadata:
>>> data = ghed.get_data()
>>> metadata = ghed.get_metadata()

Export the raw data to disk:
>>> ghed.export_raw_data(directory = "some_path")

Clear the cached data:
>>> ghed.clear_cache()
This will force the importer to download the data again the next time you call `get_data` or `get_metadata`.

Optionally you can pass a path to a local file containing the data to avoid downloading the data from the WHO.
>>> ghed = GHED(data_file="path_to_file")
If you do this, the importer will read the data from the file instead of downloading it from the WHO.
"""

import pandas as pd
import requests
import io
import numpy as np
from requests.exceptions import RequestException
from pathlib import Path
from os import PathLike

from bblocks.data_importers.config import (
    logger,
    DataExtractionError,
    DataFormattingError,
    Fields,
)
from bblocks.data_importers.utilities import convert_dtypes
from bblocks.data_importers.data_validators import DataFrameValidator

URL: str = "https://apps.who.int/nha/database/Home/IndicatorsDownload/en"


class GHED:
    """Importer for the GHED database from WHO.

    The Global Health Expenditure Database (GHED) provides comparable data on health expenditure
    across various countries and years. See more details and access the raw data at: https://apps.who.int/nha/database

    This importer provides functionality to easily get data and metadata from the GHED database.

    Usage:

    First instantiate an importer object:
    >>> ghed = GHED()

    get the data and metadata:
    >>> data = ghed.get_data()
    >>> metadata = ghed.get_metadata()
    >>> indicators = ghed.get_indicators() # Get available indicators

    Export the raw data to disk:
    >>> ghed.export_raw_data(directory = "path/to/directory")

    You can clear the cached data using the `clear_cache method`. Once the cache is cleared, next time you call
    `get_data` or `get_metadata`, the importer will download the data again.
    >>> ghed.clear_cache()


    Optionally you can pass a path to a local file containing the data to avoid downloading the data from the WHO.
    >>> ghed = GHED(data_file="user/path/to/file.xlsx")
    If you do this, the importer will read the data from the file instead of downloading it from the WHO.
    """

    def __init__(self, data_file: PathLike | Path | None = None):
        self._raw_data: io.BytesIO | None = None
        self._data: pd.DataFrame | None = None
        self._indicators: pd.DataFrame | None = None
        self._metadata: pd.DataFrame | None = None

        self._data_file = Path(data_file) if data_file else None

        # if the data file is passed and the filed does not exist, raise an error
        if self._data_file and not self._data_file.exists():
            raise FileNotFoundError(
                f"The file path `{self._data_file}` does not exist. Please provide a valid file "
                f"path."
            )

    def __repr__(self) -> str:
        """String representation of the GHED importer"""

        loaded = True if self._data is not None else False

        return f"GHED(data_file={self._data_file}, " f"data loaded = {loaded}" f")"

    @staticmethod
    def _extract_raw_data() -> io.BytesIO:
        """Extract the raw data from the GHED database

        Returns:
            A BytesIO object containing the raw data from the GHED database
        """

        try:
            response = requests.get(URL)
            response.raise_for_status()  # Raises an error for HTTP codes 4xx/5xx
            return io.BytesIO(response.content)

        except RequestException as e:
            raise DataExtractionError(f"Error extracting data: {e}")

    @staticmethod
    def _read_local_data(path: Path) -> io.BytesIO:
        """Read the data from a local file

        Args:
            path: Path to the file to read

        Returns:
            A BytesIO object containing the data from the file
        """

        try:
            with path.open("rb") as f:
                return io.BytesIO(f.read())
        except Exception as e:
            raise DataExtractionError(
                f"Error reading data from file {path}: {e}"
            ) from e

    def _format_main_data(self) -> pd.DataFrame:
        """
        Format the main data from the GHED database

        Returns:
            A DataFrame with the formatted data
        """
        return (
            pd.read_excel(self._raw_data, sheet_name="Data", dtype_backend="pyarrow")
            .drop(columns=["region", "income"])
            .melt(id_vars=["location", "code", "year"], var_name="indicator_code")
            .rename(columns={"location": Fields.country_name, "code": Fields.iso3_code})
            .pipe(convert_dtypes)
        )

    def _format_codes(self) -> pd.DataFrame:
        """
        Format the codes from the GHED database

        Returns:
            A DataFrame with the formatted codes
        """

        try:
            return (
                pd.read_excel(
                    self._raw_data, sheet_name="Codebook", dtype_backend="pyarrow"
                )
                .rename(
                    columns={
                        "variable code": Fields.indicator_code,
                        "variable name": Fields.indicator_name,
                        "long code (GHED data explorer)": "indicator_long_code",
                        "category 1": "category_1",
                        "category 2": "category_2",
                        "Method of measurement (INDICATORS category1)": "measurement_method",
                    }
                )
                # .loc[:, [Fields.indicator_code, Fields.indicator_name, "unit", "currency"]]
                .replace("-", np.nan)
                .loc[lambda d: d.indicator_long_code.notna()]
                .reset_index(drop=True)
                .pipe(convert_dtypes)
            )

        except (ValueError, KeyError) as e:
            raise DataFormattingError(f"Error formatting indicators metadata: {e}")

    def _format_data(self) -> pd.DataFrame:
        """Format the raw data

        Returns:
            A DataFrame with the formatted data
        """

        try:
            data_df = self._format_main_data()

            return pd.merge(
                data_df,
                self._indicators.loc[
                    :,
                    [Fields.indicator_code, Fields.indicator_name, "unit", "currency"],
                ],
                on="indicator_code",
                how="left",
            )

        except (ValueError, KeyError) as e:
            raise DataFormattingError(f"Error formatting data: {e}")

    def _format_metadata(self) -> pd.DataFrame:
        """Format the metadata

        Returns:
            A DataFrame with the formatted metadata
        """

        cols = {
            "location": Fields.country_name,
            "code": Fields.iso3_code,
            "variable name": Fields.indicator_name,
            "variable code": Fields.indicator_code,
            "Sources": "sources",
            "Comments": "comments",
            "Data type": "data_type",
            "Methods of estimation": "methods_of_estimation",
            "Countries and territories footnote": "country_footnote",
        }

        try:
            return (
                pd.read_excel(
                    self._raw_data, sheet_name="Metadata", dtype_backend="pyarrow"
                )
                .rename(columns=cols)
                .loc[:, cols.values()]
                .pipe(convert_dtypes)
            )

        except (ValueError, KeyError) as e:
            raise DataFormattingError(f"Error formatting metadata: {e}")

    def _load_data(self) -> None:
        """Load the data from the GHED database or the local file to the object"""

        if self._data_file:
            logger.info(f"Importing data from local file: {self._data_file}")
            self._raw_data = self._read_local_data(self._data_file)
        else:
            logger.info("Importing data from GHED database")
            self._raw_data = self._extract_raw_data()

        # Format the indicators
        self._indicators = self._format_codes()

        # Format the main data
        df = self._format_data()
        DataFrameValidator().validate(
            df,
            required_cols=[
                Fields.country_name,
                Fields.iso3_code,
                Fields.year,
                Fields.indicator_code,
                Fields.indicator_name,
                Fields.value,
            ],
        )
        self._data = df

        # Format the metadata
        self._metadata = self._format_metadata()

        logger.info("Data imported successfully")

    def get_data(self) -> pd.DataFrame:
        """Get the GHED data

        Returns:
            A DataFrame with the formatted GHED data
        """

        if self._data is None:
            self._load_data()

        return self._data

    def get_metadata(self) -> pd.DataFrame:
        """Get the GHED metadata

        Returns:
            A DataFrame with the GHED metadata including sources, footnotes, comments, etc.
            for each indicator-country pair
        """

        if self._metadata is None:
            self._load_data()

        return self._metadata

    def get_indicators(self) -> pd.DataFrame:
        """Get available GHED indicators

        Returns:
            A DataFrame with the available indicators in the GHED database
        """

        if self._indicators is None:
            self._load_data()

        return self._indicators

    def clear_cache(self) -> None:
        """Clear the data cached in the importer"""

        self._raw_data = None
        self._data = None
        self._metadata = None
        logger.info("Cache cleared")

    def export_raw_data(
        self, directory: PathLike | Path, file_name="ghed", overwrite=False
    ) -> None:
        """Export the raw data to disk.

        This method saves the raw data to disk in the specified path as an Excel file.

        Args:
            directory: Path to the directory where the data will be saved
            file_name: Name of the file to save the data
            overwrite: Whether to overwrite the file if it already exists. Default is False
        """

        directory = Path(directory)
        if not directory.exists():
            raise FileNotFoundError(
                f"The directory does not exist. Please provide a valid path."
            )
        file_path = directory / f"{file_name}.xlsx"
        if file_path.exists() and not overwrite:
            raise FileExistsError(
                f"The file already exists. Set overwrite=True to overwrite."
            )

        if self._raw_data is None:
            self._load_data()

        with open(file_path, "wb") as file:
            file.write(self._raw_data.getvalue())
        logger.info(f"Data exported to {file_path}")
