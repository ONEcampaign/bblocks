import pandas as pd

from bblocks.analysis_tools.get import __validate_cols
from bblocks.cleaning_tools import clean
from bblocks.cleaning_tools.clean import convert_id


def filter_latest_by(
    data: pd.DataFrame,
    date_column: str,
    value_columns: str | list | None = None,
    group_by: str | list | None = None,
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


def _filter_by(
    df: pd.DataFrame,
    by: str,
    by_value: str,
    id_column: str = "iso_code",
    id_type: str = "regex",
) -> pd.DataFrame:
    """
    Helper function to filter a DataFrame by membership to a specific grouping.
    The groupings come from those available through the `country_converter` package.
    More info available at:
    https://github.com/konstantinstadler/country_converter#classification-schemes

    Args:
        df: the DataFrame to filter
        by: the type of grouping to filter by (e.g. "Continent", "UNRegion", "EU")
        by_value: the value of the grouping to filter by (e.g. "Africa", "Europe", "EU")
        id_column: the name of the column to use for the id (default: "iso_code")
        id_type: the type of id to use (default: "regex")

    Returns:
        A filtered copy of the DataFrame.

    """
    if id_column not in df.columns:
        raise ValueError(f"{id_column} not in columns")

    df_ = df.copy(deep=True)

    df_["id_"] = convert_id(series=df_[id_column], from_type=id_type, to_type=by)

    return (
        df_.loc[lambda d: d.id_ == by_value]
        .drop(columns=["id_"])
        .reset_index(drop=True)
    )


def filter_by_continent(
    df: pd.DataFrame,
    continent: str,
    id_column: str = "iso_code",
    id_type: str = "regex",
) -> pd.DataFrame:
    """
    Filter a DataFrame by continent.
    Args:
        df: the DataFrame to filter
        continent: the continent to filter by (e.g. "Africa", "Europe", "EU")
        id_column: the name of the column to use for the id (default: "iso_code")
        id_type: the type of id to use (default: "regex")

    Returns:
        A filtered copy of the DataFrame.

    """
    return _filter_by(
        df=df, by="Continent", by_value=continent, id_column=id_column, id_type=id_type
    )


def filter_by_un_region(
    df: pd.DataFrame,
    region: str,
    id_column: str = "iso_code",
    id_type: str = "regex",
) -> pd.DataFrame:
    """
    Filter a DataFrame by UN region. This includes, for example, "Western Africa",
    "Eastern Africa", "Southern Asia", "Northern America", "Central America", "Eastern Asia".

    Args:
        df: the DataFrame to filter
        region: the region to filter by (e.g. "Western Africa", "Eastern Africa", etc.)
        id_column: the name of the column to use for the id (default: "iso_code")
        id_type: the type of id to use (default: "regex")

    Returns:

    """
    return _filter_by(
        df=df, by="UNRegion", by_value=region, id_column=id_column, id_type=id_type
    )


def filter_african_countries(
    df: pd.DataFrame,
    id_column: str = "iso_code",
    id_type: str = "regex",
) -> pd.DataFrame:
    """
    Filter a DataFrame to keep only African countries.
    Args:
        df: the DataFrame to filter
        id_column: the name of the column to use for the id (default: "iso_code")
        id_type: the type of id to use (default: "regex")

    Returns:
        A filtered copy of the DataFrame.

    """
    return filter_by_continent(
        df=df, continent="Africa", id_column=id_column, id_type=id_type
    )


def filter_eu_countries(
    df: pd.DataFrame,
    id_column: str = "iso_code",
    id_type: str = "regex",
) -> pd.DataFrame:
    """
    Filter a DataFrame to keep only European countries. The current list of members
    of the European Union is always used.

    Args:
        df: the DataFrame to filter
        id_column: the name of the column to use for the id (default: "iso_code")
        id_type: the type of id to use (default: "regex")

    Returns:
        A filtered copy of the DataFrame.

    """
    return _filter_by(
        df=df, by="EU", by_value="EU", id_column=id_column, id_type=id_type
    )
