import datetime
from operator import xor

import pandas as pd

from bblocks.cleaning_tools.clean import convert_to_datetime
from bblocks.logger import logger


def __validate_cols(
    d: pd.DataFrame,
    sdate: str | None,
    edate: str | None,
    date_col: str,
    value_col: str,
    grouper: list[str] | None,
) -> tuple:
    # If value column is None, set to 'value'
    if value_col is None:
        value_col = ["value"]

    # If value_columns is string convert to list
    if isinstance(value_col, str):
        value_col = [value_col]

    # Validate args
    if xor(sdate is not None, edate is not None):
        raise ValueError("start_date and end_date must be both specified or both None")

    for col in [date_col, *value_col]:
        if col not in d.columns:
            raise ValueError(f"{col} not found in _data")

    if grouper is None:
        grouper = [c for c in d.columns if c not in value_col]

    # If group_by is a string, convert it to a list
    if isinstance(grouper, str):
        grouper = [grouper]

    # group_by cannot include the date column
    grouper = [c for c in grouper if c != date_col]

    for col in grouper:
        if col not in d.columns:
            raise ValueError(f"{col} not found in _data")

    if not isinstance(sdate, datetime.datetime):
        sdate = convert_to_datetime(sdate)

    if not isinstance(edate, datetime.datetime):
        edate = convert_to_datetime(edate)

    return sdate, edate, date_col, value_col, grouper


def period_avg(
    data: pd.DataFrame,
    start_date: str | int | None = None,
    end_date: str | int | None = None,
    date_column: str = "date",
    value_columns: str | list | None = None,
    group_by: str | list | None = None,
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

    # Create a copy of the dataframe to avoid modifying the original
    data = data.copy(deep=True)

    # Check that date column is date and if not convert it
    if not pd.api.types.is_datetime64_any_dtype(data[date_column]):
        data[date_column] = convert_to_datetime(data[date_column])
        logger.info(f"Converted {date_column} to datetime")

    # Validate args
    start_date, end_date, date_column, value_columns, group_by = __validate_cols(
        data,
        sdate=start_date,
        edate=end_date,
        date_col=date_column,
        value_col=value_columns,
        grouper=group_by,
    )

    if (start_date is not None) and (end_date is not None):
        data = data[lambda d: d[date_column].between(start_date, end_date)]

    return data.groupby(by=group_by)[value_columns].mean().reset_index()


def change_from_date(
    data: pd.DataFrame,
    date_column: str,
    start_date: str | int,
    end_date: str | int,
    value_columns: str | list = None,
    group_by: str | list | None = None,
    percentage: bool = False,
) -> pd.DataFrame:
    """Calculate the change in value from a start to and end _data (in #)"""

    # Create a copy of the dataframe to avoid modifying the original
    data = data.copy(deep=True)

    # Validate args
    start_date, end_date, date_column, value_columns, group_by = __validate_cols(
        data,
        sdate=start_date,
        edate=end_date,
        date_col=date_column,
        value_col=value_columns,
        grouper=group_by,
    )

    def __range_diff(s):
        return s.diff().dropna()

    def __pct_diff(s):
        return s.pct_change().dropna()

    cols = list(data.columns)

    return (
        data.loc[lambda d: d[date_column].isin([start_date, end_date])]
        .sort_values(by=[date_column] + group_by)
        .groupby(by=group_by, group_keys=False)[value_columns]
        .apply(__range_diff if not percentage else __pct_diff)
        .reset_index()
        .filter(cols, axis=1)
    )
