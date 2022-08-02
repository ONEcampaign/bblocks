import pandas as pd

from bblocks.cleaning_tools.clean import convert_id
from bblocks.dataframe_tools.common import get_population_df


def __validate_add_column_params(
    *,
    df: pd.DataFrame,
    id_column: str,
    id_type: str | None,
    date_column: str | None,
) -> tuple:
    """Validate parameters to use in an *add column* function type"""

    if id_type is None:
        id_type = "regex"

    if id_column not in df.columns:
        raise ValueError(f"id_column '{id_column}' not in dataframe columns")

    if date_column is not None and date_column not in df.columns:
        raise ValueError(f"date_column '{date_column}' not in dataframe columns")

    # create id and year columns
    df["id_"] = convert_id(df[id_column], id_type)

    if date_column is not None:

        if pd.api.types.is_numeric_dtype(df[date_column]):
            try:
                df["year"] = pd.to_datetime(df[date_column], format="%Y").dt.year
            except ValueError:
                raise ValueError(
                    f"could not parse date format in '{date_column}'."
                    f"To fix, convert column to datetime"
                )
        else:
            try:
                df["year"] = pd.to_datetime(
                    df[date_column], infer_datetime_format=True
                ).dt.year

            except ValueError:
                raise ValueError(
                    f"could not parse date format in '{date_column}'."
                    f"To fix, convert column to datetime"
                )

    on_ = ["id_", "year"] if date_column is not None else ["id_"]

    return df, on_


def add_population_column(
    df: pd.DataFrame,
    id_column: str,
    id_type: str | None = None,
    date_column: str | None = None,
    target_column: str = "population",
    update_population_data: bool = False,
) -> pd.DataFrame:
    """Add population column to a dataframe

    Args:
        df: the dataframe to which the column will be added
        id_column: the column containing the name, ISO3, ISO2, DAC code, UN code, etc.
        id_type: the type of ID used in th id_column. The default 'regex' tries to infer
            using the rules from the 'country_converter' package. For the DAC codes,
            "DAC" must be passed.
        date_column: Optionally, a date column can be specified. If so, the population
            for that year will be used. If it's missing, it will be missing in the returned
            column as well. If the data isn't specified, the most population data from
            the world bank is used.
        target_column: the column where the population data will be stored.
        update_population_data: whether to update the underlying data or not.

    Returns:
        DataFrame: the original DataFrame with a new column containing the population data.
    """

    # validate parameters
    df_, on_ = __validate_add_column_params(
        df=df.copy(deep=True),
        id_column=id_column,
        id_type=id_type,
        date_column=date_column,
    )

    pop_df = get_population_df(
        most_recent_only=True if date_column is None else False,
        update_population_data=update_population_data,
    ).rename(columns={"iso_code": "id_"})

    df[target_column] = df_.merge(pop_df, on=on_, how="left").population

    return df


def add_short_names_column(
    df: pd.DataFrame,
    id_column: str,
    id_type: str | None = None,
    target_column: str = "short_name",
) -> pd.DataFrame:
    """Add short names column to a dataframe

    Args:
        df: the dataframe to which the column will be added
        id_column: the column containing the name, ISO3, ISO2, DAC code, UN code, etc.
        id_type: the type of ID used in th id_column. The default 'regex' tries to infer
            using the rules from the 'country_converter' package. For the DAC codes,
            "DAC" must be passed.
        target_column: the column where the short names  will be stored.

    Returns:
        DataFrame: the original DataFrame with a new column containing short names.
    """

    if id_column not in df.columns:
        raise ValueError(f"id_column '{id_column}' not in dataframe columns")

    df[target_column] = convert_id(
        df[id_column], from_type=id_type, to_type="short_name"
    )

    return df


def add_iso_codes_column(
    df: pd.DataFrame,
    id_column: str,
    id_type: str | None = None,
    target_column: str = "iso_code",
) -> pd.DataFrame:
    """Add ISO3 column to a dataframe

    Args:
        df: the dataframe to which the column will be added
        id_column: the column containing the name, ISO3, ISO2, DAC code, UN code, etc.
        id_type: the type of ID used in th id_column. The default 'regex' tries to infer
            using the rules from the 'country_converter' package. For the DAC codes,
            "DAC" must be passed.
        target_column: the column where the iso codes  will be stored.

    Returns:
        DataFrame: the original DataFrame with a new column containing ISO3 codes.
    """

    if id_column not in df.columns:
        raise ValueError(f"id_column '{id_column}' not in dataframe columns")

    df[target_column] = convert_id(df[id_column], from_type=id_type, to_type="ISO3")

    return df
