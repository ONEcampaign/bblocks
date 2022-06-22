"""tests for world_bank module"""
import pandas as pd
import pytest
from package.import_tools.world_bank import read_pink_sheet

PINK_SHEET_URL =  ("https://thedocs.worldbank.org/en/doc/5d903e848db1d1b83e0ec8f744e55570-"
                   "0350012021/related/CMO-Historical-Data-Monthly.xlsx")


def test_invalid_sheet():
    """ """
    with pytest.raises(ValueError) as exception_info:
        read_pink_sheet("")
        assert exception_info.match("invalid sheet name. "
                                    "Please specify 'Monthly Indices' or 'Monthly Prices'")

def test_valid():
    """ """
    assert isinstance(read_pink_sheet('Monthly Indices'), pd.DataFrame)
    assert isinstance(read_pink_sheet('Monthly Prices'), pd.DataFrame)