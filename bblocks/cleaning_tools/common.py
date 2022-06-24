import pandas as pd


def align_dates(
    df: pd.DataFrame,
    date_col: str = "date",
    value_col: str = "value",
    frequency: str = "YS",
) -> pd.DataFrame:
    """Align the dates so all have the same starting end ending dates.

    This function takes a dataframe which may include missing dates and return one where
    all have rows for all dates.

    Available frequencies: https://pandas.pydata.org/docs/user_guide/timeseries.html
    #timeseries-offset-aliases

    Args:
        df: A DataFrame with at least an id column, a date column and a value column.
        date_col: The name of the date column. Must be a datetime column.
        value_col: The name of the value column.
        frequency: How to align the dates (e.g. "YS" for yearly, "MS" for monthly, etc.)

    Returns:
        DataFrame: A Pandas DataFrame with the same dates for unique ids.
    """

    # Create date range
    date_range = pd.date_range(
        start=df[date_col].min(), end=df[date_col].max(), freq=frequency
    )

    # Columns without value column
    cols_without_value = df.columns.difference([value_col, date_col])

    unique_ids = [df[c].dropna().unique() for c in cols_without_value]

    # New index, based on the cartesian product of iso_code and full date range
    new_index = pd.MultiIndex.from_product(
        [
            *unique_ids,
            date_range,
        ],
        names=list(cols_without_value) + [date_col],
    )

    # For every country, fill the gaps through interpolation
    return (
        df.set_index(list(cols_without_value) + [date_col])
        .reindex(index=new_index)
        .reset_index(drop=False)
    )


d2 = align_dates(test)
