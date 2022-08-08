from typing import Optional

import pandas as pd

from bblocks.cleaning_tools.clean import convert_id
from bblocks.dataframe_tools.common import (
    get_population_df,
    get_poverty_ratio_df,
    get_population_density_df,
)
from bblocks.other_tools.dictionaries import income_levels, __download_income_levels


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
        id_column: the column containing the name, ISO3, ISO2, DACcode, UN code, etc.
        id_type: the type of ID used in th id_column. The default 'regex' tries to infer
            using the rules from the 'country_converter' package. For the DAC codes,
            "DACcode" must be passed.
        date_column: Optionally, a date column can be specified. If so, the population
            for that year will be used. If it's missing, it will be missing in the returned
            column as well. If the data isn't specified, the most recent population data from
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


def add_population_share_column(
    df: pd.DataFrame,
    id_column: str,
    id_type: str | None = None,
    date_column: str | None = None,
    value_column: str = "value",
    target_column: str = "population_share",
    update_population_data: bool = False,
) -> pd.DataFrame:
    """Add population share column to a dataframe

    Args:
        df: the dataframe to which the column will be added
        id_column: the column containing the name, ISO3, ISO2, DACcode, UN code, etc.
        id_type: the type of ID used in th id_column. The default 'regex' tries to infer
            using the rules from the 'country_converter' package. For the DAC codes,
            "DACcode" must be passed.
        date_column: Optionally, a date column can be specified. If so, the population
            for that year will be used. If it's missing, it will be missing in the returned
            column as well. If the data isn't specified, the most recent population data from
            the world bank is used.
        value_column: the column containing the value to be used in the calculation.
        target_column: the column where the population data will be stored.
        update_population_data: whether to update the underlying data or not.

    Returns:
        DataFrame: the original DataFrame with a new column containing value as share of
        population.
    """

    # validate parameters
    df_, on_ = __validate_add_column_params(
        df=df.copy(deep=True),
        id_column=id_column,
        id_type=id_type,
        date_column=date_column,
    )

    if value_column not in df_.columns:
        raise ValueError(f"value_column '{value_column}' not in dataframe columns")

    pop_df = get_population_df(
        most_recent_only=True if date_column is None else False,
        update_population_data=update_population_data,
    ).rename(columns={"iso_code": "id_"})

    df[target_column] = round(
        100 * df_.value / df_.merge(pop_df, on=on_, how="left").population, 3
    )

    return df


def add_poverty_ratio_column(
    df: pd.DataFrame,
    id_column: str,
    id_type: str | None = None,
    date_column: str | None = None,
    target_column: str = "poverty_ratio",
    update_poverty_data: bool = False,
) -> pd.DataFrame:
    """Add poverty headcount column to a dataframe

    Args:
        df: the dataframe to which the column will be added
        id_column: the column containing the name, ISO3, ISO2, DACcode, UN code, etc.
        id_type: the type of ID used in th id_column. The default 'regex' tries to infer
            using the rules from the 'country_converter' package. For the DAC codes,
            "DACcode" must be passed.
        date_column: Optionally, a date column can be specified. If so, the population
            for that year will be used. If it's missing, it will be missing in the returned
            column as well. If the data isn't specified, the most recent data is used.
        target_column: the column where the population data will be stored.
        update_poverty_data: whether to update the underlying data or not.

    Returns:
        DataFrame: the original DataFrame with a new column containing the poverty data.
    """

    # validate parameters
    df_, on_ = __validate_add_column_params(
        df=df.copy(deep=True),
        id_column=id_column,
        id_type=id_type,
        date_column=date_column,
    )

    pov_df = get_poverty_ratio_df(
        most_recent_only=True if date_column is None else False,
        update_poverty_data=update_poverty_data,
    ).rename(columns={"iso_code": "id_"})

    df[target_column] = df_.merge(pov_df, on=on_, how="left").poverty_headcount_ratio

    return df


def add_population_density_column(
    df: pd.DataFrame,
    id_column: str,
    id_type: str | None = None,
    date_column: str | None = None,
    target_column: str = "population_density",
    update_population_data: bool = False,
) -> pd.DataFrame:
    """Add population density column to a dataframe

    Args:
        df: the dataframe to which the column will be added
        id_column: the column containing the name, ISO3, ISO2, DACcode, UN code, etc.
        id_type: the type of ID used in th id_column. The default 'regex' tries to infer
            using the rules from the 'country_converter' package. For the DAC codes,
            "DACcode" must be passed.
        date_column: Optionally, a date column can be specified. If so, the population
            for that year will be used. If it's missing, it will be missing in the returned
            column as well. If the data isn't specified, the most recent data is used.
        target_column: the column where the population data will be stored.
        update_population_data: whether to update the underlying data or not.

    Returns:
        DataFrame: the original DataFrame with a new column containing the population
            density data.
    """

    # validate parameters
    df_, on_ = __validate_add_column_params(
        df=df.copy(deep=True),
        id_column=id_column,
        id_type=id_type,
        date_column=date_column,
    )

    pov_df = get_population_density_df(
        most_recent_only=True if date_column is None else False,
        update_population_data=update_population_data,
    ).rename(columns={"iso_code": "id_"})

    df[target_column] = df_.merge(pov_df, on=on_, how="left").population_density

    return df


def add_income_level_column(
    df: pd.DataFrame,
    id_column: str,
    id_type: str | None = None,
    target_column: str = "income_level",
    update_income_level_data: bool = False,
) -> pd.DataFrame:
    """Add an income levels column to a dataframe

    Args:
        df: the dataframe to which the column will be added
        id_column: the column containing the name, ISO3, ISO2, DACcode, UN code, etc.
        id_type: the type of ID used in th id_column. The default 'regex' tries to infer
            using the rules from the 'country_converter' package. For the DAC codes,
            "DACcode" must be passed.
        target_column: the column where the income level data will be stored.
        update_income_level_data: whether to update the underlying data or not.

    Returns:
        DataFrame: the original DataFrame with a new column containing the income level data.
    """

    # validate parameters
    df_, on_ = __validate_add_column_params(
        df=df.copy(deep=True),
        id_column=id_column,
        id_type=id_type,
        date_column=None,
    )

    if update_income_level_data:
        __download_income_levels()
        print("Downloaded income levels data")

    df[target_column] = df_["id_"].map(income_levels)

    return df


def add_short_names_column(
    df: pd.DataFrame,
    id_column: str,
    id_type: str | None = None,
    target_column: str = "name_short",
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


def add_median_observation(
    df: pd.DataFrame,
    group_by: str | list = None,
    value_columns: str | list[str] = "value",
    append: bool = True,
    group_name: Optional[str] = None,
) -> pd.DataFrame:
    """Add median observation column to a dataframe

    Args:

        df: the dataframe to which the column will be added
        group_by: the column(s) by which to group the data to calculate the median.
        value_columns: the column(s) containing the values to be used for the median.
        append: if True, the median observation will be appended to the dataframe. If
            False, the median observation will be stored in a new column.
        group_name: the name of the group to be used in the id_column or as the name of
        the column containing the median observations.

    Returns:
        DataFrame: the original dataframe with added rows for the median (if append is True)
            or a new column containing the median observations (if append is False).
    """
    df_ = df.copy(deep=True)

    if isinstance(value_columns, str):
        value_columns = [value_columns]

    for c in value_columns:
        if c not in df_.columns:
            raise ValueError(f"value_column '{c}' not in dataframe columns")

    if group_by is not None:
        if isinstance(group_by, str):
            group_by = [group_by]

        for c in group_by:
            if c not in df.columns:
                raise ValueError(f"group_by column '{c}' not in dataframe columns")

    if group_by is None:
        group_by = [c for c in df_.columns if c not in value_columns]

    if group_name is None:
        group_name = "median_observation"

    median = df_.groupby(group_by)[value_columns].median()

    if append:
        _ = median.reset_index()
        _ = pd.concat([df_, _], ignore_index=True)
        _.iloc[len(df_) :] = _.iloc[len(df_) :].fillna(group_name)
        return _

    return df_.merge(
        median.reset_index(), on=group_by, how="left", suffixes=("", f" ({group_name})")
    )
