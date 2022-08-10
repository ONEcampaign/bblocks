import pandas as pd
from typing import Optional
from bblocks.analysis_tools.get import __validate_cols
from bblocks.cleaning_tools import clean


def filter_latest_by(
    data: pd.DataFrame,
    date_column: str,
    value_columns: str | list = None,
    group_by: Optional[str | list] = None,
) -> pd.DataFrame:
    """
    Calculate the latest value of (a) column(s) over a period of time.

    Args:
        data: a DataFrame with a date column (datetime or int) and one or more numeric columns
        date_column: the name of the date (datetime or int) column
        value_columns: one or more columns to calculate the average over
        group_by: Optionally, specify which columns to consider for the latest operation
    Returns:
        A DataFrame with the latest value of the specified columns
    """

    # Create a copy of the dataframe to avoid modifying the original
    data = data.copy(deep=True)

    # columns order
    col_order = data.columns

    # Validate args
    _, _, date_column, value_columns, group_by = __validate_cols(
        data,
        sdate=None,
        edate=None,
        date_col=date_column,
        value_col=value_columns,
        grouper=group_by,
    )

    data["_date_"] = clean.to_date_column(data[date_column])

    cols = [date_column] + value_columns

    return (
        data.sort_values(by=["_date_"])
        .groupby(by=group_by)[cols]
        .last()
        .reset_index()
        .filter(col_order, axis=1)
    )
