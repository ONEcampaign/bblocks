""" """

import pandas as pd

def read_pink_sheet(sheet: str):
    """ """
    url = ("https://thedocs.worldbank.org/en/doc/5d903e848db1d1b83e0ec8f744e55570-"
           "0350012021/related/CMO-Historical-Data-Monthly.xlsx")
    if sheet not in ['Monthly Indices', 'Monthly Prices']:
        raise ValueError("invalid sheet name. "
                         "Please specify 'Monthly Indices' or 'Monthly Prices'")
    else:
        return pd.read_excel(url, sheet_name=sheet)