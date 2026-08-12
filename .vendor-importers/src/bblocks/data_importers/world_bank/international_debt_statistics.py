"""International Debt Statistics Data Importer.

This module contains functionality to fetch data from the International Debt Statistics (IDS)
database provided by the World Bank.

"""

import pandas as pd

from bblocks.data_importers.world_bank.world_bank import WorldBank, get_wb_databases


class InternationalDebtStatistics(WorldBank):
    """International Debt Statistics (IDS) Data Importer.

    This class provides access to data from the World Bank's International Debt Statistics database.
    It extends the WorldBank base class to specialize in IDS-specific data retrieval and metadata access.

    Usage:

    Instantiate the class. Note you do not need to provide a database ID, as it is set to IDS by default.
    >>> ids_importer = InternationalDebtStatistics()

    Get data in the same way as the WorldBank class:
    >>> data = ids_importer.get_data(indicators="DT.DOD.BLAT.CD", start_year=2000)

    It is advised to provide parameters to `get_data` to limit the size of data fetched.

    Some convenience functionality exists for commonly needed operations.
    Get the metadata for debt stock indicators:
    >>> debt_stock_meta = ids_importer.debt_stock_indicators

    Get the metadata for debt service indicators:
    >>> debt_service_meta = ids_importer.debt_service_indicators

    See the last updated date of the IDS database:
    >>> print(ids_importer.last_updated)

    All other functionality is inherited from the WorldBank base class, including methods to
    get available indicators, economies, and more. See the WorldBank class documentation for details.
    """

    def __init__(self):
        super().__init__(db=6)  # IDS database ID is 6

    def __repr__(self):
        return f"InternationalDebtStatistics(db={self._db})"

    @property
    def last_updated(self):
        """Last updated date of the IDS database."""

        return (
            get_wb_databases().loc[lambda d: d.id == self._db, "last_updated"].values[0]
        )

    @property
    def debt_stock_indicators(self) -> pd.DataFrame:
        """Get the metadata for PPG debt stock indicators."""

        inds = [
            "DT.DOD.BLAT.CD",
            "DT.DOD.MLAT.CD",
            "DT.DOD.PBND.CD",
            "DT.DOD.PCBK.CD",
            "DT.DOD.PROP.CD",
        ]

        return self.get_indicator_metadata(inds)

    @property
    def debt_service_indicators(self) -> pd.DataFrame:
        """Get the metadata for PPG debt service indicators."""

        inds = [
            "DT.AMT.BLAT.CD",
            "DT.AMT.MLAT.CD",
            "DT.AMT.PBND.CD",
            "DT.AMT.PCBK.CD",
            "DT.AMT.PROP.CD",
            "DT.INT.BLAT.CD",
            "DT.INT.MLAT.CD",
            "DT.INT.PBND.CD",
            "DT.INT.PCBK.CD",
            "DT.INT.PROP.CD",
        ]

        return self.get_indicator_metadata(inds)
