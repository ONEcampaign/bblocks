""" """

import pandas as pd
import numpy as np
from package.config import PATHS
import os
from dataclasses import dataclass
import warnings

def read_pink_sheet(sheet: str):
    """read pink sheet data from web"""

    url = ("https://thedocs.worldbank.org/en/doc/5d903e848db1d1b83e0ec8f744e55570-"
           "0350012021/related/CMO-Historical-Data-Monthly.xlsx")
    if sheet not in ['Monthly Indices', 'Monthly Prices']:
        raise ValueError("invalid sheet name. "
                         "Please specify 'Monthly Indices' or 'Monthly Prices'")
    else:
        return pd.read_excel(url, sheet_name=sheet)


def _clean_pink_sheet(df: pd.DataFrame, sheet: str) -> pd.DataFrame:
    """clean, format, standardize pink sheet data"""

    if sheet == 'Monthly Prices':
        df.columns = df.iloc[3]
        return (df
                .rename(columns={np.nan: "period"})
                .iloc[6:]
                .assign(period = lambda d: pd.to_datetime(d.period, format="%YM%m"))
                .replace("..", np.nan)
                .reset_index(drop=True)
                )

    elif sheet == 'Monthly Indices':
        df = df.iloc[9:].replace("..", np.nan).reset_index(drop=True)
        df.columns = [
            "period",
            "Energy",
            "Non-energy",
            "Agriculture",
            "Beverages",
            "Food",
            "Oils & Meals",
            "Grains",
            "Other Food",
            "Raw Materials",
            "Timber",
            "Other Raw Mat.",
            "Fertilizers",
            "Metals & Minerals",
            "Base Metals (ex. iron ore)",
            "Precious Metals",
        ]
        df["period"] = pd.to_datetime(df.period, format="%YM%m")

        return df

    else:
        raise ValueError("invalid sheet name. "
                         "Please specify 'Monthly Indices' or 'Monthly Prices'")


@dataclass
class WorldBankPinkSheet:
    """An object to help download data from World Bank Pink sheets

    Parameters:
        sheet (str): name of the sheet in the Pink Sheet ['Monthly Prices', 'Monthly Indices']
    """

    sheet: str
    data: pd.DataFrame = None

    def __post_init__(self):
        self.update()

    @property
    def file_name(self):
        return f'World Bank Pink Sheet - {self.sheet}.csv'

    def update(self):
        """Updates the underlying data
        """
        (read_pink_sheet(self.sheet)
         .pipe(_clean_pink_sheet, self.sheet)
         .to_csv(f"{PATHS.imported_data}/{self.file_name}", index=False)
         )

        self.data = pd.read_csv(f"{PATHS.imported_data}/{self.file_name}")

    def get_data(self,
                 start_date: str = None,
                 end_date:str = None,
                 indicators: str | list = None,
                 update_data = False):
        """Get data as a Pandas DataFrame

        Args:
            start_date (str)
            end_date (str)
            indicators (str | list)
            update_data (bool): Set to True to update the underlying data

        Returns:
            pd.DataFrame: Pandas dataframe with the requested data
        """
        if update_data:
            self.update()
        elif not os.path.exists(f"{PATHS.imported_data}/{self.file_name}"):
            self.update()
        else:
            self.data = pd.read_csv(f"{PATHS.imported_data}/{self.file_name}")

        if start_date is not None:
            self.data = self.data[self.data['period']>=start_date]
        if end_date is not None:
            self.data = self.data[self.data['period']<=end_date]

        if (start_date is not None) & (end_date is not None):
            if start_date > end_date:
                raise ValueError('start date cannot be earlier than end date')

        if len(self.data) == 0:
            raise ValueError('No data available for current parameters')
        if indicators is not None:
            #if indicator is a string, add it to a list
            if isinstance(indicators, str):
                indicators = [indicators]

            #check that there is at least 1 valid indicator, otherwise raise error
            if sum([i in self.data.columns for i in indicators]) == 0:
                raise ValueError('No valid indicators selected')

            #check for valid indicators, raise warning if indicator is not found
            for indicator in indicators:
                if indicator not in self.data.columns:
                    warnings.warn(f'{indicator} not found')
                    indicators.remove(indicator)

            self.data = self.data[['period']+ indicators].reset_index(drop=True)

        return self.data










