from __future__ import annotations

from abc import abstractmethod, ABC
from dataclasses import dataclass

import pandas as pd


@dataclass
class ImportData(ABC):
    data: pd.DataFrame = None

    @abstractmethod
    def update(self, **kwargs) -> ImportData:
        """Update object data"""
        pass

    @abstractmethod
    def load_data(self, **kwargs) -> ImportData:
        """Load the data saved on disk"""
        pass

    @abstractmethod
    def get_data(self, **kwargs) -> pd.DataFrame:
        """Get the data as a Pandas DataFrame"""
        pass
