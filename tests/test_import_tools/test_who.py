"""Tests for WHO module"""

import pandas as pd
from numpy import nan

from bblocks import set_bblocks_data_path, config
from bblocks.import_tools import who

set_bblocks_data_path(config.BBPaths.tests_data)


def test_clean_ghed_codes():
    """Test _clean_ghed_codes()"""

    raw = pd.DataFrame(
        {
            "Category 1": {0: "INDICATORS", 1: "INDICATORS", 2: "INDICATORS"},
            "Category 2": {0: "AGGREGATES", 1: "AGGREGATES", 2: "AGGREGATES"},
            "Indicator short code": {0: "che_gdp", 1: "che_pc_usd", 2: "che_pc_ppp"},
            "Indicator name": {
                0: "Current Health Expenditure (CHE) as % Gross Domestic Product (GDP)",
                1: "Current Health Expenditure (CHE) per Capita in US$",
                2: "Current Health Expenditure (CHE) per Capita in PPP",
            },
            "Indicator units": {0: "Percentage", 1: "Ones", 2: "Ones"},
            "Indicator currency": {0: "-", 1: "US$", 2: "Int$ (PPP)"},
        }
    )

    expected = pd.DataFrame(
        {
            "category_1": {0: "INDICATORS", 1: "INDICATORS", 2: "INDICATORS"},
            "category_2": {0: "AGGREGATES", 1: "AGGREGATES", 2: "AGGREGATES"},
            "indicator_code": {0: "che_gdp", 1: "che_pc_usd", 2: "che_pc_ppp"},
            "indicator_name": {
                0: "Current Health Expenditure (CHE) as % Gross Domestic Product (GDP)",
                1: "Current Health Expenditure (CHE) per Capita in US$",
                2: "Current Health Expenditure (CHE) per Capita in PPP",
            },
            "indicator_units": {0: "Percentage", 1: "Ones", 2: "Ones"},
            "indicator_currency": {0: nan, 1: "US$", 2: "Int$ (PPP)"},
        }
    )

    result = who._clean_ghed_codes(raw)
    pd.testing.assert_frame_equal(result, expected)


def test_clean_ghed_data():
    """test clean_ghed_data()"""

    raw = pd.DataFrame(
        {
            "country": {0: "Algeria", 1: "Algeria", 2: "Algeria"},
            "country code": {0: "DZA", 1: "DZA", 2: "DZA"},
            "region (WHO)": {0: "AFR", 1: "AFR", 2: "AFR"},
            "income group": {0: "Low-Mid", 1: "Low-Mid", 2: "Low-Mid"},
            "year": {0: 2000, 1: 2001, 2: 2002},
            "che_gdp": {0: 3.48903275, 1: 3.83787704, 2: 3.7300415},
            "che_pc_usd": {
                0: 61.58218002000001,
                1: 66.80211639000001,
                2: 66.46335601999999,
            },
        }
    )

    expected = pd.DataFrame(
        {
            "country_name": {
                0: "Algeria",
                1: "Algeria",
                2: "Algeria",
                3: "Algeria",
                4: "Algeria",
                5: "Algeria",
            },
            "country_code": {
                0: "DZA",
                1: "DZA",
                2: "DZA",
                3: "DZA",
                4: "DZA",
                5: "DZA",
            },
            "region": {0: "AFR", 1: "AFR", 2: "AFR", 3: "AFR", 4: "AFR", 5: "AFR"},
            "income_group": {
                0: "Low-Mid",
                1: "Low-Mid",
                2: "Low-Mid",
                3: "Low-Mid",
                4: "Low-Mid",
                5: "Low-Mid",
            },
            "year": {0: 2000, 1: 2001, 2: 2002, 3: 2000, 4: 2001, 5: 2002},
            "indicator_code": {
                0: "che_gdp",
                1: "che_gdp",
                2: "che_gdp",
                3: "che_pc_usd",
                4: "che_pc_usd",
                5: "che_pc_usd",
            },
            "value": {
                0: 3.48903275,
                1: 3.83787704,
                2: 3.7300415,
                3: 61.58218002000001,
                4: 66.80211639000001,
                5: 66.46335601999999,
            },
        }
    )

    result = who._clean_ghed_data(raw)

    pd.testing.assert_frame_equal(result, expected)


def test_clean_ghed_metadata():
    """Test _clean_ghed_metadata()"""

    raw = pd.DataFrame(
        {
            "country": {0: "Algeria", 1: "Algeria", 2: "Algeria"},
            "country code": {0: "DZA", 1: "DZA", 2: "DZA"},
            "region (WHO)": {0: "AFR", 1: "AFR", 2: "AFR"},
            "Income group": {0: "Low-Mid", 1: "Low-Mid", 2: "Low-Mid"},
            "Indicator short code": {0: "fs", 1: "fs1", 2: "fs11"},
            "Indicator name": {
                0: "Current health expenditure by revenues of health care financing schemes",
                1: "Transfers from government domestic revenue (allocated to health purposes)",
                2: "Internal transfers and grants",
            },
            "Sources": {
                0: nan,
                1: "2000 - 2019: Calculated as the difference between HF.1.1 and FS.2",
                2: "2000 - 2019: Calculated as the difference between HF.1.1 and FS.2",
            },
            "Comments": {0: nan, 1: nan, 2: nan},
            "Methods of estimation": {
                0: nan,
                1: "2000 - 2019: Derived by applying the sum of the components",
                2: "2000 - 2019: Derived by applying the sum of the components",
            },
            "Data type": {
                0: nan,
                1: "2000 - 2019: Estimated",
                2: "2000 - 2019: Estimated",
            },
            "Footnote": {0: nan, 1: nan, 2: nan},
        }
    )

    expected = pd.DataFrame(
        {
            "country_code": {0: "DZA", 1: "DZA", 2: "DZA"},
            "indicator_code": {0: "fs", 1: "fs1", 2: "fs11"},
            "source": {
                0: nan,
                1: "2000 - 2019: Calculated as the difference between HF.1.1 and FS.2",
                2: "2000 - 2019: Calculated as the difference between HF.1.1 and FS.2",
            },
            "comments": {0: nan, 1: nan, 2: nan},
            "methods_of_estimation": {
                0: nan,
                1: "2000 - 2019: Derived by applying the sum of the components",
                2: "2000 - 2019: Derived by applying the sum of the components",
            },
            "data_type": {
                0: nan,
                1: "2000 - 2019: Estimated",
                2: "2000 - 2019: Estimated",
            },
            "footnote": {0: nan, 1: nan, 2: nan},
        }
    )

    result = who._clean_metadata(raw)

    pd.testing.assert_frame_equal(result, expected)
