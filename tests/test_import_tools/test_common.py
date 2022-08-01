import pandas as pd
import pytest
from numpy import nan
import os
from bblocks.config import PATHS

from bblocks.import_tools import common


def test_get_dsa():

    df = common.get_dsa()
    assert isinstance(df, pd.DataFrame)

    for risk_category in df["risk_of_debt_distress"].unique():
        assert risk_category in ["High", "Low", "Moderate", "In debt distress", nan]

    file_name = f"{PATHS.imported_data}/dsa_list.pdf"
    time = os.path.getmtime(file_name)

    df = common.get_dsa(update=True)
    assert os.path.getmtime(file_name) > time
