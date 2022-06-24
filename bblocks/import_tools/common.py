from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import pandas as pd


@dataclass
class ImportData(ABC):
    indicators: dict = field(default_factory=dict)
    data: pd.DataFrame = None
    update_data: bool = False

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
