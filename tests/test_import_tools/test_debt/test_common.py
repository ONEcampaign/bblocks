import os

import pandas as pd
import pytest
from numpy import nan

from bblocks import set_bblocks_data_path, config
from bblocks.import_tools.debt import common

set_bblocks_data_path(config.BBPaths.tests_data)


def test_get_dsa():
    df = common.get_dsa()
    assert isinstance(df, pd.DataFrame)

    for risk_category in df["risk_of_debt_distress"].unique():
        assert risk_category in ["High", "Low", "Moderate", "In debt distress", nan]

    file_name = f"{config.BBPaths.raw_data}/dsa_list.pdf"
    time = os.path.getmtime(file_name)

    df = common.get_dsa(update=True)

    assert os.path.getmtime(file_name) > time

    common.__dict__["URL"]: str = "https://www.imf.org/external/Pubs/ft/dsa/df"

    with pytest.raises(ConnectionError):
        df = common.get_dsa(update=True)
