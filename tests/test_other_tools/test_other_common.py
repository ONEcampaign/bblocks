from bblocks import config, set_bblocks_data_path
from bblocks.other_tools.dictionaries import Dict

set_bblocks_data_path(config.BBPaths.tests_data)

TEST_DICT = {"France": "FRA", "Germany": "DEU", "Italy": "ITA", "Guatemala": "GTM"}


def test_change_keys():
    # Create a test dictionary of Dict class
    test = Dict(TEST_DICT)

    # Change keys from name_short to ISO3
    test.change_keys(to="ISO3")

    assert all(k == v for k, v in test.items())


def test_reverse():
    # Create a test dictionary of Dict class
    test = Dict(TEST_DICT)

    # Reverse the keys and values
    test.reverse()

    assert list(test.keys()) == list(TEST_DICT.values())


def test_set_keys_type():
    # Create a test dictionary of Dict class
    test = Dict({"1": "a", "2": "b", "3": "c"})

    # Set the keys to int
    test.set_keys_type(int)

    assert all(isinstance(k, int) for k in test.keys())


def test_set_values_type():
    # Create a test dictionary of Dict class
    test = Dict({"a": 1.1, "b": 2.2, "c": 3.3})

    # Set the values to string
    test.set_values_type(str)

    assert all(isinstance(v, str) for v in test.values())
