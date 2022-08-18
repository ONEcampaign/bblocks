import os.path

import pandas as pd
import pytest

from bblocks.config import PATHS
from bblocks.import_tools.world_bank import WorldBankData, PinkSheet


def test_world_bank_data_load_indicator():
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
    assert "both" in str(error.value)


def test__world_bank_data_update():
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


def test__world_bank_data_get_data():
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
    df = wb_obj.get_data(indicators="NY.GDP.MKTP.CD")

    assert df.indicator_code.nunique() == 1
    assert "NY.GDP.MKTP.CD" in df.indicator_code.unique()

    # Check that it is possible to get multiple indicators
    df = wb_obj.get_data(indicators=["SP.POP.TOTL", "NY.GDP.MKTP.CD"])

    assert df.indicator.nunique() == 2


def test_pink_sheet():

    invalid_sheet_name = "invalid sheet name"
    with pytest.raises(ValueError) as error:
        PinkSheet(sheet=invalid_sheet_name)
    assert 'Invalid sheet name' in str(error.value)

    pk_obj = PinkSheet(sheet = "Monthly Prices")
    assert pk_obj.sheet == "Monthly Prices"

    pk_obj = PinkSheet(sheet = "Monthly Indices")
    assert pk_obj.sheet == "Monthly Indices"

    # test update
    pk_obj = PinkSheet(sheet = "Monthly Prices")
    file_path = f"{PATHS.imported_data}/{pk_obj.file_name}"
    time = os.path.getmtime(file_path)
    pk_obj.update()
    # check underlying file has been modified
    assert os.path.getmtime(file_path) > time

    assert "Monthly Prices" in pk_obj.file_name

    pk_obj_1 = PinkSheet(sheet = "Monthly Indices")
    pk_obj_1.load_indicator()
    assert isinstance(pk_obj_1.data, pd.DataFrame)
    assert isinstance(pk_obj_1.indicators, dict)

    pk_obj_2 = PinkSheet(sheet = "Monthly Indices")
    pk_obj_2.load_indicator('Energy')
    assert isinstance(pk_obj_2.data, pd.DataFrame)
    assert isinstance(pk_obj_2.indicators, dict)
    assert 'Energy' in pk_obj_2.indicators.keys()

    pk_obj_3 = PinkSheet(sheet = "Monthly Indices")
    pk_obj_3.load_indicator(['Energy', 'Agriculture'])
    assert isinstance(pk_obj_3.data, pd.DataFrame)
    assert isinstance(pk_obj_3.indicators, dict)
    assert 'Agriculture' in pk_obj_3.indicators.keys()

    pk_obj_4 = PinkSheet(sheet = "Monthly Indices")
    pk_obj_4.load_indicator()
    start = "2020-01-01"
    end = "2021-10-01"
    indicator = 'Energy'
    indicator_list = ['Energy', 'Agriculture']

    df = pk_obj_4.get_data(start_date=start, end_date=end)
    assert min(df.period) == start
    assert max(df.period) == end

    df1 = pk_obj_4.get_data(indicators=indicator)
    assert df1.indicator.unique()[0] == indicator
    df2 = pk_obj_4.get_data(indicators=indicator_list)
    for indicator in indicator_list:
        assert indicator in df2.indicator.unique()

    with pytest.raises(ValueError) as error:
        # check when dates are inverted
        pk_obj_4.get_data(start_date=end, end_date=start)
    assert error.match("start date cannot be earlier than end date")

    with pytest.raises(ValueError) as error:
        pk_obj_4.get_data(start_date='2500-01-01')
    assert error.match('No data available for current parameters')

    invalid_indicator_list = ['Energy', 'invalid indicator']
    with pytest.warns(UserWarning) as warning:
        pk_obj_4.get_data(indicators=invalid_indicator_list)
    assert len(warning) == 1










