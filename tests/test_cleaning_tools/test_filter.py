import pandas as pd
from bblocks.cleaning_tools.filter import filter_latest_by


def test_filter_latest_by():
    # Create a sample dataframe with a datetime column and multiple numeric columns
    test_data = pd.DataFrame(
        {
            "date": [2018, 2019, 2016, 2021, 2018, 2020],
            "value": [9, 8, 7, 6, 5, 4],
            "value2": [1, 2, 3, 4, 5, 6],
            "country": ["A", "A", "B", "B", "C", "C"],
            "category": ["T1", "T1", "T2", "T2", "T2", "T2"],
        }
    )

    result = filter_latest_by(
        test_data,
        date_column="date",
        value_columns=["value", "value2"],
        group_by=["country", "category"],
    )

    assert result.value.to_list() == [8, 6, 4]
    assert result.value2.to_list() == [2, 4, 6]

    result2 = filter_latest_by(
        test_data,
        date_column="date",
        value_columns=["value", "value2"],
        group_by=["category"],
    )

    assert result2.value.to_list() == [8, 6]
    assert result2.value2.to_list() == [2, 4]

    test_data_date = pd.DataFrame(
        {
            "fecha": [
                "2018-01-01",
                "2018-01-02",
                "2019-12-01",
                "2019-12-12",
                "2021-03-17",
                "2021-04-15",
            ],
            "number": [9, 8, 7, 6, 5, 4],
            "number2": [1, 2, 3, 4, 5, 6],
            "pais": ["A", "A", "B", "B", "C", "C"],
            "category": ["T1", "T1", "T2", "T2", "T2", "T2"],
        }
    )

    result3 = filter_latest_by(
        test_data_date,
        date_column="fecha",
        value_columns=["number", "number2"],
        group_by=["pais"],
    )

    assert result3.fecha.to_list() == ["2018-01-02", "2019-12-12", "2021-04-15"]
