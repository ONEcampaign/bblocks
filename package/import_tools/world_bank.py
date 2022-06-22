""" """

import pandas as pd
import numpy as np

def read_pink_sheet(sheet: str):
    """ """
    url = ("https://thedocs.worldbank.org/en/doc/5d903e848db1d1b83e0ec8f744e55570-"
           "0350012021/related/CMO-Historical-Data-Monthly.xlsx")
    if sheet not in ['Monthly Indices', 'Monthly Prices']:
        raise ValueError("invalid sheet name. "
                         "Please specify 'Monthly Indices' or 'Monthly Prices'")
    else:
        return pd.read_excel(url, sheet_name=sheet)


def __clean_prices(df: pd.dataFrame) -> pd.DataFrame:
    """ """

    df.columns = df.iloc[3]
    return (df
            .rename(columns={np.nan: "period"})
            .assign(period = lambda d: pd.to_datetime(d.period, format="%YM%m"))
            .iloc[6:]
            .replace("..", np.nan)
            .reset_index(drop=True)
            )


def __clean_index(df: pd.DataFrame) -> pd.DataFrame:
    """ """


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




