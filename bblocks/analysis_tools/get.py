from typing import Optional

import pandas as pd


def period_avg(
    data: pd.DataFrame,
    start_date: Optional[str|int] = None,
    end_date: Optional[str|int] = None,
    date_column: str = "date",
    value_columns: str | list = None,
    group_by: Optional[str | list] = None,
) -> pd.DataFrame:
    """
    Calculate the average of (a) column(s) over a period of time.

    Args:
        data: a DataFrame with a date column (datetime or int) and one or more numeric columns
        start_date: Optionally, specify the start date of the period
        end_date: Optionally, specify the end date of the period
        date_column: the name of the date (datetime or int) column
        value_columns: one or more columns to calculate the average over
        group_by: Optionally, specify which columns to consider for the average

    Returns:
        A DataFrame with the average of the specified columns over the specified period

    """
    from operator import xor

    # Create a copy of the dataframe to avoid modifying the original
    data = data.copy(deep=True)

    # If value column is None, set to 'value'
    if value_columns is None:
        value_columns = ["value"]

    # If value_columns is string convert to list
    if isinstance(value_columns, str):
        value_columns = [value_columns]

    # Since group_by is an optional argument, assign all columns but the value one
    # in case it is not passed
    if group_by is None:
        group_by = [c for c in data.columns if c not in value_columns]

    # If group_by is a string, convert it to a list
    if isinstance(group_by, str):
        group_by = [group_by]

    # group_by cannot include the date column
    group_by = [c for c in group_by if c != date_column]

    # Validate args
    if xor(start_date is not None, end_date is not None):
        raise ValueError("start_date and end_date must be both specified or both None")

    for col in [date_column, *value_columns, *group_by]:
        if col not in data.columns:
            raise ValueError(f"{col} not found in data")

    if (start_date is not None) and (end_date is not None):
        data = data[lambda d: d[date_column].between(start_date, end_date)]

    return data.groupby(by=group_by)[value_columns].mean().reset_index()
