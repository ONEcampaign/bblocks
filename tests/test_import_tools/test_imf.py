import pandas as pd
import pytest
import os

from bblocks.config import PATHS
from bblocks.import_tools.imf import SDR


def test_sdr_update():
    """test update function"""

    sdr_obj = SDR()
    file_path = f"{PATHS.imported_data}/{sdr_obj.file_name}"
    time = os.path.getmtime(file_path)
    sdr_obj.update()

    # check underlying file has been modified
    assert os.path.getmtime(file_path) > time

    # check attributes updated correctly
    assert isinstance(sdr_obj.data, pd.DataFrame)
    assert isinstance(sdr_obj.date, str)

