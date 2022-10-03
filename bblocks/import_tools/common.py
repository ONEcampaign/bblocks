from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import pandas as pd

from bblocks.config import PATHS


@dataclass
class ImportData(ABC):
    indicators: dict = field(default_factory=dict)
    data: pd.DataFrame = None
    update_data: bool = False
    data_path: str = None

    def __post_init__(self):
        if self.data_path is None:
            self.data_path = f"{PATHS.imported_data}"

        if self.data_path[-1] == "/":
            self.data_path = self.data_path[:-1]

    @abstractmethod
    def load_indicator(self, **kwargs) -> ImportData:
        """Load the data saved on disk"""
        pass

    @abstractmethod
    def update(self, **kwargs) -> ImportData:
        """Update object data"""
        pass

    @abstractmethod
    def get_data(self, **kwargs) -> pd.DataFrame:
        """Get the data as a Pandas DataFrame"""
        pass


def append_new_data(
    new_data: pd.DataFrame, existing_data_path: str, parse_dates: list[str] | None
) -> pd.DataFrame:
    """Append new data to an existing dataframe"""
    # Read file
    try:
        saved = pd.read_csv(existing_data_path, parse_dates=parse_dates)
    except FileNotFoundError:
        saved = pd.DataFrame()

    # Append new data
    data = pd.concat([saved, new_data], ignore_index=True)

    return data.drop_duplicates(keep="last").reset_index(drop=True)
