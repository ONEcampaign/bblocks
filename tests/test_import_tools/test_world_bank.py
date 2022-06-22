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
            end_year=2018,
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

