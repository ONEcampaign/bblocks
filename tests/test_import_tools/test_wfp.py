import os
from typing import KeysView

import pytest

from bblocks.config import PATHS
from bblocks.import_tools import wfp


def test__get_inflation():

    assert wfp._get_inflation('nonsense') is None

    os.remove(PATHS.imported_data + r"/wfp_raw/nonsense_inflation.csv")


def test__get_country_codes():
    file_name = PATHS.imported_data + r"/wfp_raw/wfp_country_codes.csv"

    time = os.path.getmtime(file_name)

    wfp._get_country_codes()

    # Check that the underlying file has been modified
    assert os.path.getmtime(file_name) > time


def test_available_indicators():
    test_obj = wfp.WFPData()

    assert isinstance(test_obj.available_indicators, KeysView)
    assert len(test_obj.available_indicators) > 0


def test_load_indicator():
    test_obj = wfp.WFPData()

    test_obj.load_indicator("inflation")

    assert len(test_obj.indicators) > 0
    assert list(test_obj.indicators.keys())[0] == "inflation"

    # test that an error is raised when wrong indicator passed
    with pytest.raises(ValueError) as error:
        test_obj.load_indicator("nonsense")

    assert str(error.value) == "Indicator nonsense not available"


def test_update_inflation():
    test_obj = wfp.WFPData()

    wfp.__dict__['_CODES']= {'AFG': 1, 'ALB': 3, 'DZA': 4, 'AND': 7, 'AGO': 8}

    # Check that it raises an error if no indicators have been loaded
    with pytest.raises(RuntimeError) as error:
        test_obj.update()

    assert "loaded" in str(error.value)

    # Load indicator
    test_obj.load_indicator("inflation")

    # Get the filename and time
    folder_name = f"{PATHS.imported_data}/wfp_raw/AGO_inflation.csv"
    time = os.path.getmtime(folder_name)

    # Update the indicator
    test_obj.update()

    # Check that the underlying file has been modified
    assert os.path.getmtime(folder_name) > time


def test_update_insufficient():
    test_obj = wfp.WFPData()

    wfp.__dict__['_CODES']= {'AFG': 1, 'ALB': 3, 'DZA': 4, 'AND': 7, 'AGO': 8}

    # Load indicator
    test_obj.load_indicator("insufficient_food")

    # Get the filename and time
    file_name = f"{PATHS.imported_data}/wfp_raw/AGO_insufficient_food.csv"
    time = os.path.getmtime(file_name)

    # Update the indicator
    test_obj.update()

    # Check that the underlying file has been modified
    assert os.path.getmtime(file_name) > time


def test_get_data():
    test_obj = wfp.WFPData()

    assert len(test_obj.get_data()) == 0

    test_obj.load_indicator("inflation")

    with pytest.raises(ValueError):
        test_obj.get_data("insufficient_food")

    assert len(test_obj.get_data()) > 0

    test_obj.load_indicator("insufficient_food")

    df = test_obj.get_data(["insufficient_food", "inflation"])

    assert list(df.indicator.unique()) == [
        "people_with_insufficient_food_consumption",
        "Inflation Rate",
        "Food Inflation",
    ]
