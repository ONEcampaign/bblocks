from __future__ import annotations

import pathlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import pandas as pd
import requests
import io
from zipfile import ZipFile, BadZipFile

from bblocks.logger import logger


@dataclass(repr=False)
class ImportData(ABC):
    _data: dict[str, pd.DataFrame] = field(default_factory=dict)
    _raw_data: pd.DataFrame | None = None

    @abstractmethod
    def load_data(self, **kwargs) -> ImportData:
        """Load the _data saved on disk"""
        pass

    @abstractmethod
    def update_data(self, **kwargs) -> ImportData:
        """Update object _data"""
        pass

    def get_data(self, indicators: str | list = "all", **kwargs) -> pd.DataFrame:
        """Get the _data as a Pandas DataFrame"""
        if self._data is None:
            raise RuntimeError("No data or indicators have been loaded")

        df = pd.DataFrame()
        indicators_ = []

        if isinstance(indicators, str) and indicators != "all":
            indicators = [indicators]

        if indicators == "all":
            indicators_ = self._data.values()

        if isinstance(indicators, list):
            for _ in indicators:
                if _ not in self._data:
                    logger.warning(f"{_} not loaded or is an invalid indicator.")

            indicators_ = [self._data[_] for _ in indicators if _ in list(self._data)]

        if len(indicators_) == 0:
            logger.warning("No indicators were loaded. Returning empty dataframe.")

        for _ in indicators_:
            df = pd.concat([df, _], ignore_index=True)

        return df


def append_new_data(
    new_data: pd.DataFrame,
    existing_data_path: str | pathlib.Path,
    parse_dates: list[str] | None,
) -> pd.DataFrame:
    """Append new _data to an existing dataframe"""
    # Read file
    try:
        saved = pd.read_csv(existing_data_path, parse_dates=[parse_dates])
    except FileNotFoundError:
        saved = pd.DataFrame()

    # Append new _data
    data = pd.concat([saved, new_data], ignore_index=True)

    return data.drop_duplicates(keep="last").reset_index(drop=True)


def get_response(url: str) -> requests.Response:
    """Get the response from a url

    This function is used to get the response from a url.
    It will raise an error if the url is invalid or if the response is not 200.

    Args:
        url: url to get the response from

    Returns:
        response from the url
    """

    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.ConnectionError:
        raise requests.exceptions.ConnectionError("Invalid url")

    return response


def unzip(file: str | io.BytesIO) -> ZipFile:
    """Unzip a file

    Create a ZipFile object from a file on disk or a file-like object from a requests
    response.

    Args:
        file: path to zipfile or file-like object. If zipfile is extracted from a url,
        the file like object can be obtained by calling `io.BytesIO(response.content)`

    Returns:
        ZipFile: object containing unzipped folder
    """

    try:
        return ZipFile(file)
    except BadZipFile as e:
        raise ValueError(f"The file could not be unzipped. Error : {str(e)}")
    except FileNotFoundError:
        raise FileNotFoundError(f"Could not find file: {file}")
