import os
from typing import KeysView

import pytest

from bblocks import set_bblocks_data_path, config
from bblocks.config import BBPaths
from bblocks.import_tools import wfp

set_bblocks_data_path(config.BBPaths.tests_data)


def test__get_inflation():
    assert wfp._get_inflation("nonsense") is None

    os.remove(BBPaths.wfp_data / r"nonsense_inflation.csv")


def test_available_indicators():
    test_obj = wfp.WFPData()

    assert isinstance(test_obj.available_indicators, KeysView)
    assert len(test_obj.available_indicators) > 0


def test_load_indicator():
    test_obj = wfp.WFPData()

    test_obj.load_data(indicator="inflation")

    assert len(test_obj._data) > 0
    assert list(test_obj._data.keys())[0] == "inflation"

    # test that an error is raised when wrong indicator passed
    with pytest.raises(ValueError) as error:
        test_obj.load_data("nonsense")

    assert str(error.value) == "Indicator nonsense not available"


def test_get_data():
    test_obj = wfp.WFPData()

    assert len(test_obj.get_data()) == 0

    test_obj.load_data(indicator="inflation")

    assert len(test_obj.get_data("insufficient_food")) == 0

    assert len(test_obj.get_data("inflation")) > 0

    test_obj.load_data("insufficient_food")

    df = test_obj.get_data(["insufficient_food", "inflation"])

    assert (
        set(df.indicator.unique()).symmetric_difference(
            {
                "people_with_insufficient_food_consumption",
                "Inflation Rate",
                "Food Inflation",
            }
        )
        == set()
    )
