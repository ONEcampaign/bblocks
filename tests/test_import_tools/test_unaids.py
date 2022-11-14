import pandas as pd
import pytest

from bblocks.import_tools import unaids


DIMENSIONS = ["All ages estimate", "All ages lower estimate", "All ages upper estimate"]
YEARS = ["2019", "2020", "2021"]


def _mock_unaids_response():
    """mocks unaids response"""

    return {
        "Data_Type": "Number",
        "Data_Round": 1,
        "tbinfo_test": "tbinfo_test",
        "Percent_Change": "false",
        "Area_Level": "2",
        "tableYear": ["2019", "2020", "2021"],
        "MultiIndicators": ["People living with HIV"],
        "MultiSubgroups": [
            ["All ages estimate", "All ages lower estimate", "All ages upper estimate"]
        ],
        "TimePeriod": ["2019", "2020", "2021"],
        "tableData": [
            {
                "Area_Name": "Afghanistan",
                "Area_ID": "AFG",
                "Data_Val": [[[353, 153, 593]], [[442, 189, 742]], [[552, 235, 922]]],
            },
            {
                "Area_Name": "Albania",
                "Area_ID": "ALB",
                "Data_Val": [[[5, 0.13893, 16]], [[7, 0.4219, 19]], [[9, 0.84501, 23]]],
            },
            {
                "Area_Name": "Algeria",
                "Area_ID": "DZA",
                "Data_Val": [[[430, 348, 569]], [[523, 432, 669]], [[632, 529, 770]]],
            },
        ],
        "Global_Numbers": [
            {
                "Area_Name": "All countries",
                "Area_ID": "03M49WLD",
                "Data_Val": [
                    [[8515095.9837, 7513899.143, 9721730.721]],
                    [[10403722.7975, 9180464.83, 11877988.42]],
                    [[12432577.965599999, 10970768.63, 14194342.56]],
                ],
            }
        ],
    }


def test_get_dimensions():
    """test get_dimensions"""

    response = _mock_unaids_response()
    dimensions = unaids.get_dimensions(response)

    assert isinstance(dimensions, list)
    assert dimensions == DIMENSIONS


def test_get_years():
    """tests get_years"""

    response = _mock_unaids_response()
    years = unaids.get_years(response)

    assert isinstance(years, list)
    assert years == YEARS


def test_parse_data_table():
    """tests parse_data_table"""

    response = _mock_unaids_response()
    df = unaids.parse_data_table(response, DIMENSIONS, YEARS)

    expected_df = pd.DataFrame(
        {
            "All ages estimate": {
                0: 353,
                1: 442,
                2: 552,
                3: 5,
                4: 7,
                5: 9,
                6: 430,
                7: 523,
                8: 632,
            },
            "All ages lower estimate": {
                0: 153.0,
                1: 189.0,
                2: 235.0,
                3: 0.13893,
                4: 0.4219,
                5: 0.84501,
                6: 348.0,
                7: 432.0,
                8: 529.0,
            },
            "All ages upper estimate": {
                0: 593,
                1: 742,
                2: 922,
                3: 16,
                4: 19,
                5: 23,
                6: 569,
                7: 669,
                8: 770,
            },
            "area_name": {
                0: "Afghanistan",
                1: "Afghanistan",
                2: "Afghanistan",
                3: "Albania",
                4: "Albania",
                5: "Albania",
                6: "Algeria",
                7: "Algeria",
                8: "Algeria",
            },
            "area_id": {
                0: "AFG",
                1: "AFG",
                2: "AFG",
                3: "ALB",
                4: "ALB",
                5: "ALB",
                6: "DZA",
                7: "DZA",
                8: "DZA",
            },
            "year": {
                0: "2019",
                1: "2020",
                2: "2021",
                3: "2019",
                4: "2020",
                5: "2021",
                6: "2019",
                7: "2020",
                8: "2021",
            },
        }
    )

    pd.testing.assert_frame_equal(df, expected_df)


def test_parse_global_data():
    """test parse_global_data"""

    expected_df = pd.DataFrame(
        {
            "All ages estimate": {
                0: 8515095.9837,
                1: 10403722.7975,
                2: 12432577.965599999,
            },
            "All ages lower estimate": {0: 7513899.143, 1: 9180464.83, 2: 10970768.63},
            "All ages upper estimate": {0: 9721730.721, 1: 11877988.42, 2: 14194342.56},
            "area_name": {0: "Global", 1: "Global", 2: "Global"},
            "area_id": {0: "03M49WLD", 1: "03M49WLD", 2: "03M49WLD"},
            "year": {0: "2019", 1: "2020", 2: "2021"},
        }
    )

    response = _mock_unaids_response()
    df = unaids.parse_global_data(response, DIMENSIONS, YEARS)

    pd.testing.assert_frame_equal(df, expected_df)


def test_clean_data():
    """test clean_data"""

    expected_df = pd.DataFrame(
        {
            "area_name": {
                0: "Afghanistan",
                1: "Afghanistan",
                2: "Afghanistan",
                3: "Albania",
                4: "Albania",
                5: "Albania",
                6: "Algeria",
                7: "Algeria",
                8: "Algeria",
                9: "Afghanistan",
                10: "Afghanistan",
                11: "Afghanistan",
                12: "Albania",
                13: "Albania",
                14: "Albania",
                15: "Algeria",
                16: "Algeria",
                17: "Algeria",
                18: "Afghanistan",
                19: "Afghanistan",
                20: "Afghanistan",
                21: "Albania",
                22: "Albania",
                23: "Albania",
                24: "Algeria",
                25: "Algeria",
                26: "Algeria",
            },
            "area_id": {
                0: "AFG",
                1: "AFG",
                2: "AFG",
                3: "ALB",
                4: "ALB",
                5: "ALB",
                6: "DZA",
                7: "DZA",
                8: "DZA",
                9: "AFG",
                10: "AFG",
                11: "AFG",
                12: "ALB",
                13: "ALB",
                14: "ALB",
                15: "DZA",
                16: "DZA",
                17: "DZA",
                18: "AFG",
                19: "AFG",
                20: "AFG",
                21: "ALB",
                22: "ALB",
                23: "ALB",
                24: "DZA",
                25: "DZA",
                26: "DZA",
            },
            "year": {
                0: "2019",
                1: "2020",
                2: "2021",
                3: "2019",
                4: "2020",
                5: "2021",
                6: "2019",
                7: "2020",
                8: "2021",
                9: "2019",
                10: "2020",
                11: "2021",
                12: "2019",
                13: "2020",
                14: "2021",
                15: "2019",
                16: "2020",
                17: "2021",
                18: "2019",
                19: "2020",
                20: "2021",
                21: "2019",
                22: "2020",
                23: "2021",
                24: "2019",
                25: "2020",
                26: "2021",
            },
            "indicator": {
                0: "People living with HIV - All ages",
                1: "People living with HIV - All ages",
                2: "People living with HIV - All ages",
                3: "People living with HIV - All ages",
                4: "People living with HIV - All ages",
                5: "People living with HIV - All ages",
                6: "People living with HIV - All ages",
                7: "People living with HIV - All ages",
                8: "People living with HIV - All ages",
                9: "People living with HIV - All ages",
                10: "People living with HIV - All ages",
                11: "People living with HIV - All ages",
                12: "People living with HIV - All ages",
                13: "People living with HIV - All ages",
                14: "People living with HIV - All ages",
                15: "People living with HIV - All ages",
                16: "People living with HIV - All ages",
                17: "People living with HIV - All ages",
                18: "People living with HIV - All ages",
                19: "People living with HIV - All ages",
                20: "People living with HIV - All ages",
                21: "People living with HIV - All ages",
                22: "People living with HIV - All ages",
                23: "People living with HIV - All ages",
                24: "People living with HIV - All ages",
                25: "People living with HIV - All ages",
                26: "People living with HIV - All ages",
            },
            "dimension": {
                0: "All ages estimate",
                1: "All ages estimate",
                2: "All ages estimate",
                3: "All ages estimate",
                4: "All ages estimate",
                5: "All ages estimate",
                6: "All ages estimate",
                7: "All ages estimate",
                8: "All ages estimate",
                9: "All ages lower estimate",
                10: "All ages lower estimate",
                11: "All ages lower estimate",
                12: "All ages lower estimate",
                13: "All ages lower estimate",
                14: "All ages lower estimate",
                15: "All ages lower estimate",
                16: "All ages lower estimate",
                17: "All ages lower estimate",
                18: "All ages upper estimate",
                19: "All ages upper estimate",
                20: "All ages upper estimate",
                21: "All ages upper estimate",
                22: "All ages upper estimate",
                23: "All ages upper estimate",
                24: "All ages upper estimate",
                25: "All ages upper estimate",
                26: "All ages upper estimate",
            },
            "value": {
                0: 353.0,
                1: 442.0,
                2: 552.0,
                3: 5.0,
                4: 7.0,
                5: 9.0,
                6: 430.0,
                7: 523.0,
                8: 632.0,
                9: 153.0,
                10: 189.0,
                11: 235.0,
                12: 0.13893,
                13: 0.4219,
                14: 0.84501,
                15: 348.0,
                16: 432.0,
                17: 529.0,
                18: 593.0,
                19: 742.0,
                20: 922.0,
                21: 16.0,
                22: 19.0,
                23: 23.0,
                24: 569.0,
                25: 669.0,
                26: 770.0,
            },
        }
    )

    indicator = "People living with HIV - All ages"
    response = _mock_unaids_response()
    df = unaids.parse_data_table(response, DIMENSIONS, YEARS)
    df = unaids.clean_data(df, indicator)

    pd.testing.assert_frame_equal(df, expected_df)


def test_get_category():
    """test get_category"""

    indicator = "People living with HIV - All ages"
    expected = "People living with HIV"

    category = unaids.get_category(indicator)

    assert category == expected


def test_response_params():
    """test response_params"""

    indicator = "People living with HIV - All ages"
    group = "country"

    expected = {
        'url': unaids.URL,
        'indicator': indicator,
        'category': 'People living with HIV',
        "area_name": 'world',
        "area_code": 2
    }

    params = unaids.response_params(group, indicator)

    assert params == expected


def test_response_to_df():
    """test response_to_df"""

    grouping = "region"
    response = _mock_unaids_response()

    expected_df = pd.DataFrame({'All ages estimate': {0: 353.0, 1: 442.0, 2: 552.0, 3: 5.0, 4: 7.0, 5: 9.0, 6: 430.0, 7: 523.0, 8: 632.0, 9: 8515095.9837, 10: 10403722.7975, 11: 12432577.965599999}, 'All ages lower estimate': {0: 153.0, 1: 189.0, 2: 235.0, 3: 0.13893, 4: 0.4219, 5: 0.84501, 6: 348.0, 7: 432.0, 8: 529.0, 9: 7513899.143, 10: 9180464.83, 11: 10970768.63}, 'All ages upper estimate': {0: 593.0, 1: 742.0, 2: 922.0, 3: 16.0, 4: 19.0, 5: 23.0, 6: 569.0, 7: 669.0, 8: 770.0, 9: 9721730.721, 10: 11877988.42, 11: 14194342.56}, 'area_name': {0: 'Afghanistan', 1: 'Afghanistan', 2: 'Afghanistan', 3: 'Albania', 4: 'Albania', 5: 'Albania', 6: 'Algeria', 7: 'Algeria', 8: 'Algeria', 9: 'Global', 10: 'Global', 11: 'Global'}, 'area_id': {0: 'AFG', 1: 'AFG', 2: 'AFG', 3: 'ALB', 4: 'ALB', 5: 'ALB', 6: 'DZA', 7: 'DZA', 8: 'DZA', 9: '03M49WLD', 10: '03M49WLD', 11: '03M49WLD'}, 'year': {0: '2019', 1: '2020', 2: '2021', 3: '2019', 4: '2020', 5: '2021', 6: '2019', 7: '2020', 8: '2021', 9: '2019', 10: '2020', 11: '2021'}}
                               )

    df = unaids.response_to_df(grouping, response)
    pd.testing.assert_frame_equal(df.reset_index(drop=True),
                                         expected_df.reset_index(drop=True))


def test_available_indicators():
    """test available_indicators"""

    assert isinstance(unaids.Aids().available_indicators, pd.DataFrame)
    assert list(unaids.Aids().available_indicators.columns) == ["indicator", "category"]


def test_check_response():
    """test check_response"""

    response = {"tableData": []}
    with pytest.raises(ValueError) as error:
        unaids.check_response(response)
    assert "No data available for this indicator" in str(error.value)


def mock_indicators():
    """return mock indicator dictionary as would be stored in Aids object"""

    return {
        "country": {
            "People living with HIV - All ages": pd.DataFrame(
                {
                    "area_name": {
                        31: "Afghanistan",
                        5535: "Afghanistan",
                        11039: "Afghanistan",
                    },
                    "area_id": {31: "AFG", 5535: "AFG", 11039: "AFG"},
                    "year": {31: 2021, 5535: 2021, 11039: 2021},
                    "indicator": {
                        31: "People living with HIV - All ages",
                        5535: "People living with HIV - All ages",
                        11039: "People living with HIV - All ages",
                    },
                    "dimension": {
                        31: "All ages estimate",
                        5535: "All ages lower estimate",
                        11039: "All ages upper estimate",
                    },
                    "value": {31: 10933.0, 5535: 4330.0, 11039: 42133.0},
                }
            )
        },
        "region": {
            "People living with HIV - All ages": pd.DataFrame(
                {
                    "area_name": {287: "Global", 575: "Global", 863: "Global"},
                    "area_id": {287: "03M49WLD", 575: "03M49WLD", 863: "03M49WLD"},
                    "year": {287: 2021, 575: 2021, 863: 2021},
                    "indicator": {
                        287: "People living with HIV - All ages",
                        575: "People living with HIV - All ages",
                        863: "People living with HIV - All ages",
                    },
                    "dimension": {
                        287: "All ages estimate",
                        575: "All ages lower estimate",
                        863: "All ages upper estimate",
                    },
                    "value": {287: 38362151.6994, 575: 33851565.9, 863: 43798273.29},
                }
            )
        },
    }


def test_concat_dataframes():

    expected_df = pd.DataFrame(
        {
            "area_name": {31: "Afghanistan", 5535: "Afghanistan", 11039: "Afghanistan"},
            "area_id": {31: "AFG", 5535: "AFG", 11039: "AFG"},
            "year": {31: 2021, 5535: 2021, 11039: 2021},
            "indicator": {
                31: "People living with HIV - All ages",
                5535: "People living with HIV - All ages",
                11039: "People living with HIV - All ages",
            },
            "dimension": {
                31: "All ages estimate",
                5535: "All ages lower estimate",
                11039: "All ages upper estimate",
            },
            "value": {31: 10933.0, 5535: 4330.0, 11039: 42133.0},
        }
    )

    df = unaids.concat_dataframes(
        mock_indicators(), ["People living with HIV - All ages"], ["country"]
    )
    pd.testing.assert_frame_equal(df, expected_df)


def test_check_area_grouping():
    """test check_area_grouping"""

    with pytest.raises(ValueError) as error:
        unaids.check_area_grouping("invalid_grouping")
    assert "Invalid" in str(error.value)

    group_list = unaids.check_area_grouping("country")
    assert group_list == ["country"]


def test_load_indicator():
    """test load_indicator"""

    aids = unaids.Aids()

    with pytest.raises(ValueError) as error:
        aids.load_indicator("invalid_indicator")
    assert "Invalid" in str(error.value)


def test_get_data():
    """test get_data"""

    aids = unaids.Aids()

    with pytest.raises(RuntimeError) as error:
        aids.get_data()
    assert "No indicators loaded" in str(error.value)


def test_update():
    """test update"""
    aids = unaids.Aids()
    with pytest.raises(RuntimeError) as error:
        aids.update()
    assert "No indicators loaded" in str(error.value)

