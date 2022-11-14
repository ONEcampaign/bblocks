import os.path

import pandas as pd
import pytest

from bblocks.config import PATHS
from bblocks.import_tools.world_bank import WorldBankData
from bblocks.import_tools import world_bank

from numpy import nan


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





def test_clean_prices():
    """Test clean_prices"""

    unformatted_df = pd.DataFrame({'World Bank Commodity Price Data (The Pink Sheet)': {
        0: 'monthly prices in nominal US dollars, 1960 to present',
        1: '(monthly series are available only in nominal US dollars)',
        2: 'Updated on November 02, 2022',
        3: nan,
        4: nan,
        5: '1960M01',
        6: '1960M02',
        7: '1960M03'},
                                   'Unnamed: 1': {0: nan,
                                                  1: nan,
                                                  2: nan,
                                                  3: 'Crude oil, average',
                                                  4: '($/bbl)',
                                                  5: 1.63000011444,
                                                  6: 1.63000011444,
                                                  7: 1.63000011444},
                                   'Unnamed: 2': {0: nan,
                                                  1: nan,
                                                  2: nan,
                                                  3: 'Crude oil, Brent',
                                                  4: '($/bbl)',
                                                  5: 1.63000011444,
                                                  6: 1.63000011444,
                                                  7: 1.63000011444}})

    formatted_df = (pd.DataFrame({'period': {0: '1960-02-01',
                                             1: '1960-03-01',
                                             2: '1960-02-01',
                                             3: '1960-03-01'},
                                  'indicator': {0: 'Crude oil, average',
                                                1: 'Crude oil, average',
                                                2: 'Crude oil, Brent',
                                                3: 'Crude oil, Brent'},
                                  'value': {0: 1.63000011444,
                                            1: 1.63000011444,
                                            2: 1.63000011444,
                                            3: 1.63000011444},
                                  'units': {0: '$/bbl', 1: '$/bbl', 2: '$/bbl', 3: '$/bbl'}})
                    .assign(period=lambda x: pd.to_datetime(x.period))
                    )

    pd.testing.assert_frame_equal(world_bank.clean_prices(unformatted_df), formatted_df)


def test_clean_index():
    """Test clean_index"""

    unformatted_df = pd.DataFrame({'World Bank Commodity Price Data (The Pink Sheet)': {0: 'monthly indices based on nominal US dollars, 2010=100, 1960 to present',
                                                                                        1: '(monthly series are available only in nominal US dollar terms)',
                                                                                        2: 'Updated on November 02, 2022',
                                                                                        3: nan,
                                                                                        4: nan,
                                                                                        5: nan,
                                                                                        6: nan,
                                                                                        7: nan,
                                                                                        8: '1960M01',
                                                                                        9: '1960M02'},
                                   'Unnamed: 1': {0: nan,
                                                  1: nan,
                                                  2: nan,
                                                  3: nan,
                                                  4: 'Energy',
                                                  5: nan,
                                                  6: nan,
                                                  7: nan,
                                                  8: 2.13444915445322,
                                                  9: 2.13444915445322},
                                   'Unnamed: 2': {0: nan,
                                                  1: nan,
                                                  2: nan,
                                                  3: nan,
                                                  4: 'Non-energy',
                                                  5: nan,
                                                  6: nan,
                                                  7: ' ',
                                                  8: 18.81589520307,
                                                  9: 18.68146974844},
                                   'Unnamed: 3': {0: nan,
                                                  1: nan,
                                                  2: nan,
                                                  3: nan,
                                                  4: nan,
                                                  5: 'Agriculture',
                                                  6: nan,
                                                  7: ' ',
                                                  8: 22.0320871106,
                                                  9: 21.80845926478},
                                   'Unnamed: 4': {0: nan,
                                                  1: nan,
                                                  2: nan,
                                                  3: nan,
                                                  4: nan,
                                                  5: nan,
                                                  6: 'Beverages',
                                                  7: nan,
                                                  8: 25.44330149114,
                                                  9: 25.1406604908},
                                   'Unnamed: 5': {0: nan,
                                                  1: nan,
                                                  2: nan,
                                                  3: nan,
                                                  4: nan,
                                                  5: nan,
                                                  6: 'Food',
                                                  7: ' ',
                                                  8: 21.19410147097,
                                                  9: 20.88540958087},
                                   'Unnamed: 6': {0: nan,
                                                  1: nan,
                                                  2: nan,
                                                  3: nan,
                                                  4: nan,
                                                  5: nan,
                                                  6: nan,
                                                  7: 'Oils & Meals',
                                                  8: 23.16907720375,
                                                  9: 22.47641358733},
                                   'Unnamed: 7': {0: nan,
                                                  1: nan,
                                                  2: nan,
                                                  3: nan,
                                                  4: nan,
                                                  5: nan,
                                                  6: nan,
                                                  7: 'Grains',
                                                  8: 23.57750535562,
                                                  9: 23.42698276517},
                                   'Unnamed: 8': {0: nan,
                                                  1: nan,
                                                  2: nan,
                                                  3: nan,
                                                  4: nan,
                                                  5: nan,
                                                  6: nan,
                                                  7: 'Other Food',
                                                  8: 16.43850765528,
                                                  9: 16.49180075934},
                                   'Unnamed: 9': {0: nan,
                                                  1: nan,
                                                  2: nan,
                                                  3: nan,
                                                  4: nan,
                                                  5: nan,
                                                  6: 'Raw Materials',
                                                  7: ' ',
                                                  8: 22.33348675217,
                                                  9: 22.35670425853},
                                   'Unnamed: 10': {0: nan,
                                                   1: nan,
                                                   2: nan,
                                                   3: nan,
                                                   4: nan,
                                                   5: nan,
                                                   6: nan,
                                                   7: 'Timber',
                                                   8: 16.23922227558,
                                                   9: 16.23922227558},
                                   'Unnamed: 11': {0: nan,
                                                   1: nan,
                                                   2: nan,
                                                   3: nan,
                                                   4: nan,
                                                   5: nan,
                                                   6: nan,
                                                   7: 'Other Raw Mat.',
                                                   8: 28.99805773844,
                                                   9: 29.04666546537},
                                   'Unnamed: 12': {0: nan,
                                                   1: nan,
                                                   2: nan,
                                                   3: nan,
                                                   4: nan,
                                                   5: 'Fertilizers',
                                                   6: nan,
                                                   7: ' ',
                                                   8: 12.8618468473,
                                                   9: 12.8618468473},
                                   'Unnamed: 13': {0: nan,
                                                   1: nan,
                                                   2: nan,
                                                   3: nan,
                                                   4: nan,
                                                   5: 'Metals  & Minerals',
                                                   6: nan,
                                                   7: nan,
                                                   8: 12.89589240086,
                                                   9: 12.9291469329},
                                   'Unnamed: 14': {0: nan,
                                                   1: nan,
                                                   2: nan,
                                                   3: nan,
                                                   4: nan,
                                                   5: nan,
                                                   6: 'Base Metals (ex. iron ore)',
                                                   7: nan,
                                                   8: 14.0736800082,
                                                   9: 14.11466486881},
                                   'Unnamed: 15': {0: nan,
                                                   1: nan,
                                                   2: nan,
                                                   3: nan,
                                                   4: 'Precious Metals',
                                                   5: nan,
                                                   6: nan,
                                                   7: ' ',
                                                   8: 3.26873458679,
                                                   9: 3.26873458679}})

    result = world_bank.clean_index(unformatted_df)

    expected_df = pd.DataFrame({'period': {0: '1960-02-01 00:00:00',
                                           1: '1960-02-01 00:00:00',
                                           2: '1960-02-01 00:00:00',
                                           3: '1960-02-01 00:00:00',
                                           4: '1960-02-01 00:00:00',
                                           5: '1960-02-01 00:00:00',
                                           6: '1960-02-01 00:00:00',
                                           7: '1960-02-01 00:00:00',
                                           8: '1960-02-01 00:00:00',
                                           9: '1960-02-01 00:00:00',
                                           10:'1960-02-01 00:00:00',
                                           11:'1960-02-01 00:00:00',
                                           12:'1960-02-01 00:00:00',
                                           13:'1960-02-01 00:00:00',
                                           14:'1960-02-01 00:00:00'},
                                'indicator': {0: 'Energy',
                                              1: 'Non-energy',
                                              2: 'Agriculture',
                                              3: 'Beverages',
                                              4: 'Food',
                                              5: 'Oils & Meals',
                                              6: 'Grains',
                                              7: 'Other Food',
                                              8: 'Raw Materials',
                                              9: 'Timber',
                                              10: 'Other Raw Mat.',
                                              11: 'Fertilizers',
                                              12: 'Metals & Minerals',
                                              13: 'Base Metals (ex. iron ore)',
                                              14: 'Precious Metals'},
                                'value': {0: 2.13444915445322,
                                          1: 18.68146974844,
                                          2: 21.80845926478,
                                          3: 25.1406604908,
                                          4: 20.88540958087,
                                          5: 22.47641358733,
                                          6: 23.42698276517,
                                          7: 16.49180075934,
                                          8: 22.35670425853,
                                          9: 16.23922227558,
                                          10: 29.04666546537,
                                          11: 12.8618468473,
                                          12: 12.9291469329,
                                          13: 14.11466486881,
                                          14: 3.26873458679},
                                'units': {0: 'index',
                                          1: 'index',
                                          2: 'index',
                                          3: 'index',
                                          4: 'index',
                                          5: 'index',
                                          6: 'index',
                                          7: 'index',
                                          8: 'index',
                                          9: 'index',
                                          10: 'index',
                                          11: 'index',
                                          12: 'index',
                                          13: 'index',
                                          14: 'index'}}).assign(period= lambda d: pd.to_datetime(d.period))

    pd.testing.assert_frame_equal(result, expected_df)

