from bblocks.analysis_tools.get import period_avg, change_from_date, latest_by
import pandas as pd


def test_period_avg():
    # Create a sample dataframe with a datetime column and multiple numeric columns
    test_data = pd.DataFrame(
        {
            "date": ["2020-01-01", "2021-01-01", "2022-01-01", "2019-01-01"],
            "value": [1, 2, 3, 4],
            "value2": [5, 6, 7, 8],
            "country": ["A", "B", "B", "A"],
        }
    )

    # Calculate the average of the value column over the period 2020-01-01 to 2022-01-01
    result = period_avg(
        test_data,
        start_date="2020-01-01",
        end_date="2022-01-01",
        value_columns=["value", "value2"],
    )

    # Check passing parameters differently
    result1_1 = period_avg(
        test_data,
        start_date="2020-01-01",
        end_date="2022-01-01",
        value_columns="value",
        group_by="country",
    )

    assert result.value.sum() == 3.5
    assert result.value2.sum() == 11.5
    assert result1_1.value.sum() == 3.5
    assert "value2" not in result1_1.columns

    test_data2 = test_data.assign(date=pd.to_datetime(test_data.date).dt.year)

    result2 = period_avg(
        test_data2,
        start_date=2019,
        end_date=2022,
        value_columns=["value", "value2"],
    )

    assert result2.value.sum() == 5
    assert result2.value2.sum() == 13


def test_change_from_date():
    # Create a sample dataframe with a datetime column and multiple numeric columns
    test_data = pd.DataFrame(
        {
            "date": ["2018-01-01", "2021-01-01", "2018-01-01", "2021-01-01"],
            "value": [1, 3, 1, 7],
            "value2": [2, 4, 4, 2],
            "country": ["A", "A", "B", "B"],
        }
    ).astype({"date": "datetime64[ns]"})

    # Calculate the change in value from 2020-01-01 to 2021-01-01
    result = change_from_date(
        test_data,
        date_column="date",
        start_date="2018-01-01",
        end_date="2021-01-01",
        value_columns=["value", "value2"],
    )

    assert result.value.to_list() == [2.0, 6.0]
    assert result.value2.to_list() == [2.0, -2.0]

    test_data2 = pd.DataFrame(
        {
            "date": [2018, 2021, 2018, 2021],
            "value": [1, 3, 1, 7],
            "value2": [2, 4, 4, 2],
            "country": ["A", "A", "B", "B"],
        }
    )
    result2 = change_from_date(
        test_data,
        date_column="date",
        start_date="2018-01-01",
        end_date="2021-01-01",
        value_columns=["value", "value2"],
    )

    assert result2.value.to_list() == [2.0, 6.0]
    assert result2.value2.to_list() == [2.0, -2.0]

