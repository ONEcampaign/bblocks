import pandas as pd
import pytest
import os

from bblocks.config import PATHS
from bblocks.import_tools.imf import SDR


def test_sdr_load_indicator():

    sdr_obj = SDR()

    sdr_obj.load_indicator()
    assert "holdings" and 'allocations' in sdr_obj.indicators
    sdr_obj.load_indicator('holdings')
    assert 'holdings' in sdr_obj.indicators and 'allocations' not in sdr_obj.indicators
    sdr_obj.load_indicator('allocations')
    assert 'holdings' not in sdr_obj.indicators and 'allocations' in sdr_obj.indicators
    sdr_obj.load_indicator(['holdings', 'allocations'])
    assert 'holdings' in sdr_obj.indicators and 'allocations' in sdr_obj.indicators

    invalid_indicator = 'invalid'
    with pytest.raises(ValueError) as error:
        sdr_obj.load_indicator(invalid_indicator)
    assert invalid_indicator in str(error.value)


def test_sdr_update():

    sdr_obj = SDR()

    sdr_obj.load_indicator()
    file_path = f"{PATHS.imported_data}/{sdr_obj.file_name}"
    time = os.path.getmtime(file_path)

    sdr_obj.update()
    assert os.path.getmtime(file_path) > time


def test_sdr_get_data():
    sdr_obj = SDR()

    sdr_obj.load_indicator()
    df = sdr_obj.get_data()
    assert isinstance(df, pd.DataFrame)
    assert df.indicator.nunique() == 2

    sdr_obj.load_indicator()
    df = sdr_obj.get_data(indicators='holdings')
    assert df.indicator.nunique() == 1

    sdr_obj.load_indicator()
    df = sdr_obj.get_data(members='Zimbabwe')
    assert df.member.nunique() == 1

    invalid_member = 'invalid'
    with pytest.raises(ValueError) as error:
        sdr_obj.load_indicator()
        sdr_obj.get_data(members=invalid_member)
    assert 'No members found' in str(error.value)

    invalid_list = ['Zimbabwe', 'invalid']
    with pytest.warns(UserWarning) as record:
        sdr_obj.load_indicator()
        df = sdr_obj.get_data(members=invalid_list)
    assert len(record) == 1
    assert record[0].message.args[0] == "member not found: invalid"







