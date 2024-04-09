"""Tests for the ilo module"""

import pandas as pd
import pytest

from bblocks.import_tools import ilo
from bblocks import set_bblocks_data_path, config

set_bblocks_data_path(config.BBPaths.tests_data)


class TestILO:
    """Tests for ILO module"""

    obj = ilo.ILO()
    obj_2 = ilo.ILO()

    indicator_1 = "CLD_TPOP_SEX_AGE_GEO_NB_A"
    indicator_2 = "CPI_XCPI_COI_RT_Q"
    indicator_list = ["CLD_TPOP_SEX_AGE_NB_A", "CLD_XCHD_SEX_AGE_NB_A"]

    def test_update_data_error(self):
        """Test that update_data raises an error because no data is loaded"""

        with pytest.raises(RuntimeError, match="No indicators loaded"):
            self.obj.update_data()

    def test_available_indicators(self):
        """Test that the available indicators are returned and loaded to the object"""

        assert isinstance(self.obj.available_indicators(), pd.DataFrame)
        assert self.obj._available_indicators is not None

    def test_load_glossaries(self):
        """Test that the glossaries are loaded to the object"""

        self.obj._load_glossaries()
        assert isinstance(self.obj._glossaries, dict)

        # check that the values are dictionaries
        key = list(self.obj._glossaries.keys())[0]
        assert isinstance(self.obj._glossaries[key], dict)

    def test_load_area_dict(self):
        """Test that the area dictionary is loaded to the object"""

        self.obj._load_area_dict()
        assert isinstance(self.obj._area_dict, dict)

    def test_load_data(self):
        """Test loading a single indicator"""

        self.obj.load_data(self.indicator_1)
        assert self.indicator_1 in self.obj._data
        assert isinstance(self.obj._data[self.indicator_1], pd.DataFrame)

        self.obj._glossaries = None  # set glossaries back to None
        self.obj._area_dict = None  # set area dictionary back to None

        self.obj.load_data(self.indicator_2)
        assert self.indicator_2 in self.obj._data
        assert isinstance(self.obj._data[self.indicator_2], pd.DataFrame)

        assert (
            self.indicator_1 in self.obj._data
        )  # check that first indicator is still there

    def test_load_data_list(self):
        """Test loading a list of indicators"""

        self.obj.load_data(self.indicator_list)
        assert self.indicator_list[0] in self.obj._data
        assert isinstance(self.obj._data[self.indicator_list[0]], pd.DataFrame)
        assert self.indicator_list[1] in self.obj._data
        assert isinstance(self.obj._data[self.indicator_list[1]], pd.DataFrame)

    def test_load_data_error(self):
        """Test that an error is raised if the indicator is invalid"""

        with pytest.raises(ValueError, match="Indicator not available"):
            self.obj.load_data("invalid")

    def test_update_data_when_data_loaded_from_disk(self):
        """Test that the data is updated correctly when data is loaded from disk"""

        # mock data loaded from disk - data object is not empty but glossaries and areas are empty
        self.obj_2.load_data(self.indicator_1)
        self.obj_2._glossaries = None
        self.obj_2._area_dict = None

        self.obj_2.update_data()
        assert self.indicator_1 in self.obj_2._data
        assert isinstance(self.obj_2._data[self.indicator_1], pd.DataFrame)
        assert self.obj_2._glossaries is not None
        assert self.obj_2._area_dict is not None
