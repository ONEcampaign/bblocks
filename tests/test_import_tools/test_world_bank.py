
import os.path
from package.import_tools.world_bank import WorldBankPinkSheet

import pandas as pd
import pytest

from package.config import PATHS
from package.import_tools.world_bank import WorldBankData


def test_load_indicator():
    wb_obj = WorldBankData()

    # Load indicator
    wb_obj.load_indicator(
        indicator_code="SP.POP.TOTL",
        indicator_name="Population",
        start_year=None,
        end_year=None,
        most_recent_only=True,
    )
    # Check that the indicator was loaded
    assert "SP.POP.TOTL" in wb_obj.indicators
    # Check that the indicator data was downloaded
    assert wb_obj.indicators["SP.POP.TOTL"] is not None
    # Check that the `most_recent_only` parameter was respected
    assert wb_obj.indicators["SP.POP.TOTL"][0].iso_code.duplicated().sum() == 0

    wb_obj.load_indicator(
        indicator_code="NY.GDP.MKTP.CD",
        indicator_name="GDP",
        start_year=2015,
        end_year=2018,
        most_recent_only=False,
    )

    # Check that the second indicator was appended to the list
    assert len(wb_obj.indicators) == 2
    # Check that the starting year in the data is indeed the start_year parameter
    assert wb_obj.indicators["NY.GDP.MKTP.CD"][0].date.dt.year.min() == 2015
    # Check that the ending year in the data is indeed the end_year parameter
    assert wb_obj.indicators["NY.GDP.MKTP.CD"][0].date.dt.year.max() == 2018

    # Check that both start and end date have to be provided

    with pytest.raises(ValueError) as error:
        wb_obj.load_indicator(
            indicator_code="NY.GDP.MKTP.CD",
            indicator_name="GDP",
            start_year=2015,
            most_recent_only=False,
        )
    assert 'both' in str(error.value)

def test_update():
    import os

    wb_obj = WorldBankData()

    # Check that it raises an error if no indicators have been loaded
    with pytest.raises(RuntimeError) as error:
        wb_obj.update()

    assert "loaded" in str(error.value)

    # Load indicator
    wb_obj.load_indicator(
        indicator_code="SP.POP.TOTL",
        indicator_name="Population",
        start_year=None,
        end_year=None,
        most_recent_only=True,
    )

    # Get the filename and time
    file_name = (
        f"{PATHS.imported_data}/" f'{wb_obj.indicators["SP.POP.TOTL"][1]["file_name"]}'
    )
    time = os.path.getmtime(file_name)

    # Update the indicator
    wb_obj.update()

    # Check that the underlying file has been modified
    assert os.path.getmtime(file_name) > time




def test_get_data():
    wb_obj = WorldBankData()

    # Load indicator
    wb_obj.load_indicator(
        indicator_code="SP.POP.TOTL",
        indicator_name="Population",
        start_year=None,
        end_year=None,
        most_recent_only=True,
    )

    # Get the data for the indicator
    df = wb_obj.get_data()

    assert isinstance(df, pd.DataFrame)

    # Load a second indicator
    wb_obj.load_indicator(
        indicator_code="NY.GDP.MKTP.CD",
        indicator_name="GDP",
        start_year=2015,
        end_year=2018,
        most_recent_only=False,
    )

    df = wb_obj.get_data()

    # Check that the data for the second indicator is included
    assert df.indicator.nunique() == 2

    # Check that it is possible to get only one of the indicators
    df = wb_obj.get_data(indicators='NY.GDP.MKTP.CD')

    assert df.indicator_code.nunique() == 1
    assert 'NY.GDP.MKTP.CD' in df.indicator_code.unique()

    # Check that it is possible to get multiple indicators
    df = wb_obj.get_data(indicators=['SP.POP.TOTL', 'NY.GDP.MKTP.CD'])

    assert df.indicator.nunique() == 2
    
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

    #test warning
    with pytest.warns(UserWarning) as record:
        pk_obj_1.get_data(indicators = ['Crude oil, Brent', 'Oil'])
        assert len(record)>0
        assert record[0].message.args[0] == 'Oil not found'

def test_file_name():
    """test that file name is generated correctly"""
    pk_obj = WorldBankPinkSheet('Monthly Prices')
    assert 'Monthly Prices' in pk_obj.file_name


