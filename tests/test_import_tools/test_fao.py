import pytest
import pandas as pd
import bblocks.import_tools.fao as fao
from bblocks.config import PATHS
import os


def test_get_fao_index():

    df = fao.get_fao_index(update=True)
    assert isinstance(df, pd.DataFrame)

    file_name = f"{PATHS.imported_data}/fao_index.csv"
    time = os.path.getmtime(file_name)

    _ = fao.get_fao_index(update=True)
    assert os.path.getmtime(file_name) > time


def test_scrape_index_df():

    fao.__dict__["URL"]: str = "https://www.fao.org/invalidhref"

    with pytest.raises(ConnectionError):
        _ = fao.__scrape_index_df(fao.URL)
