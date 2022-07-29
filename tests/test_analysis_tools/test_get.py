from bblocks.analysis_tools.get import period_avg
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
    assert not 'value2' in result1_1.columns

    test_data2 = test_data.assign(date=pd.to_datetime(test_data.date).dt.year)

    result2 = period_avg(
        test_data2,
        start_date=2019,
        end_date=2022,
        value_columns=["value", "value2"],
    )

    assert result2.value.sum() == 5
    assert result2.value2.sum() == 13
