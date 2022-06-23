"""tests for world_bank module"""
import os.path
import pandas as pd
import pytest
from package.import_tools.world_bank import WorldBankPinkSheet
from package.config import PATHS


def test_invalid_sheet():
    """Test error handling for invalid sheet name"""
    with pytest.raises(ValueError) as error:
        WorldBankPinkSheet('')
        assert error.match("invalid sheet name. "
                           "Please specify 'Monthly Indices' or 'Monthly Prices'")

def test_update():
    """test update function"""
    pk_obj = WorldBankPinkSheet('Monthly Prices')
    file_path = f"{PATHS.imported_data}/{pk_obj.file_name}"
    time = os.path.getmtime(file_path)
    pk_obj.update()
    #check underlying file has been modified
    assert os.path.getmtime(file_path) > time

def test_get_data():
    """Test get_data function"""
    pk_obj_1 = WorldBankPinkSheet('Monthly Prices')
    assert isinstance(pk_obj_1.get_data(), pd.DataFrame)

    pk_obj_2 = WorldBankPinkSheet('Monthly Indices')
    assert isinstance(pk_obj_2.get_data(), pd.DataFrame)

    #test when update data is true
    assert isinstance(pk_obj_1.get_data(update_data=True), pd.DataFrame)

    #test dates
    start = '2020-01-01'
    end = '2021-10-01'
    pk_obj_3 = WorldBankPinkSheet('Monthly Prices')
    df = pk_obj_3.get_data(start_date=start, end_date = end)
    assert min(df.period) == start
    assert max(df.period) == end

    with pytest.raises(ValueError) as error:
        #check when dates are inverted
        pk_obj_3.get_data(start_date='2021-09-01', end_date = '2020_01_01')
        assert error.match('start date cannot be earlier than end date')

    #test no data returned
    with pytest.raises(ValueError) as error:
        pk_obj_3.get_data(start_date='2100-01-01')
        assert error.match('No data available for current parameters')

    #test invalid indicators
    with pytest.raises(ValueError) as error:
        pk_obj_1.get_data(indicators = '')
        assert error.match('No valid indicators selected')


def test_file_name():
    """test that file name is generated correctly"""
    pk_obj = WorldBankPinkSheet('Monthly Prices')
    assert 'Monthly Prices' in pk_obj.file_name


