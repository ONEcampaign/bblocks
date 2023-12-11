from typing import Optional

import pandas as pd

from bblocks.cleaning_tools.clean import convert_id, convert_to_datetime
from bblocks.dataframe_tools.common import (
    get_population_df,
    get_poverty_ratio_df,
    get_population_density_df,
    get_gdp_df,
    get_gov_expenditure_df,
)


def __validate_add_column_params(
    *,
    df: pd.DataFrame,
    id_column: str,
    id_type: str | None,
    date_column: str | None,
) -> tuple:
    """Validate parameters to use in an *add column* function type"""

    # Create a dataframe copy to avoid overriding the original _data
    df = df.copy(deep=True)

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
                df["merge_year"] = pd.to_datetime(df[date_column], format="%Y").dt.year
            except ValueError:
                raise ValueError(
                    f"could not parse date format in '{date_column}'."
                    f"To fix, convert column to datetime"
                )
        else:
            try:
                df["merge_year"] = convert_to_datetime(df[date_column]).dt.year

            except ValueError:
                raise ValueError(
                    f"could not parse date format in '{date_column}'."
                    f"To fix, convert column to datetime"
                )

    on_ = ["id_", "merge_year"] if date_column is not None else ["id_"]

    return df, on_


def add_population_column(
    df: pd.DataFrame,
    id_column: str,
    id_type: str | None = None,
    date_column: str | None = None,
    target_column: str = "population",
    update_data: bool = False,
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
            column as well. If the _data isn't specified, the most recent population _data from
            the world bank is used.
        target_column: the column where the population _data will be stored.
        update_data: whether to update the underlying _data or not.

    Returns:
        DataFrame: the original DataFrame with a new column containing the population _data.
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
        update=update_data,
    ).rename(
        columns={"iso_code": "id_", "year": "merge_year", "population": target_column}
    )

    if date_column is None:
        pop_df = pop_df.drop(columns=["merge_year"])

    # Create a deep copy of the dataframe to avoid overwriting the original _data
    df_ = df_.merge(pop_df, on=on_, how="left")

    if date_column is not None:
        df_ = df_.drop(columns=["merge_year"])

    return df_.drop(columns=["id_"])


def add_poverty_ratio_column(
    df: pd.DataFrame,
    id_column: str,
    id_type: str | None = None,
    date_column: str | None = None,
    target_column: str = "poverty_ratio",
    update_data: bool = False,
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
            column as well. If the _data isn't specified, the most recent _data is used.
        target_column: the column where the population _data will be stored.
        update_data: whether to update the underlying _data or not.

    Returns:
        DataFrame: the original DataFrame with a new column containing the poverty _data.
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
        update=update_data,
    ).rename(
        columns={
            "iso_code": "id_",
            "year": "merge_year",
            "poverty_headcount_ratio": target_column,
        }
    )

    if date_column is None:
        pov_df = pov_df.drop(columns=["merge_year"])

    # Create a deep copy of the dataframe to avoid overwriting the original _data
    df_ = df_.merge(pov_df, on=on_, how="left")

    if date_column is not None:
        df_ = df_.drop(columns=["merge_year"])

    return df_.drop(columns=["id_"])


def add_population_density_column(
    df: pd.DataFrame,
    id_column: str,
    id_type: str | None = None,
    date_column: str | None = None,
    target_column: str = "population_density",
    update_data: bool = False,
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
            column as well. If the _data isn't specified, the most recent _data is used.
        target_column: the column where the population _data will be stored.
        update_data: whether to update the underlying _data or not.
    Returns:
        DataFrame: the original DataFrame with a new column containing the population
            density _data.
    """

    # validate parameters
    df_, on_ = __validate_add_column_params(
        df=df.copy(deep=True),
        id_column=id_column,
        id_type=id_type,
        date_column=date_column,
    )

    pod_df = get_population_density_df(
        most_recent_only=True if date_column is None else False,
        update=update_data,
    ).rename(
        columns={
            "iso_code": "id_",
            "year": "merge_year",
            "population_density": target_column,
        }
    )

    if date_column is None:
        pod_df = pod_df.drop(columns=["merge_year"])

    df_ = df_.merge(pod_df, on=on_, how="left")

    if date_column is not None:
        df_ = df_.drop(columns=["merge_year"])

    return df_.drop(columns=["id_"])


def add_gdp_column(
    df: pd.DataFrame,
    id_column: str,
    id_type: str | None = None,
    date_column: str | None = None,
    target_column: str = "gdp",
    usd: bool = True,
    include_estimates: bool = False,
    update_data: bool = False,
) -> pd.DataFrame:
    """Add GDP column to a dataframe

    Args:
        df: the dataframe to which the column will be added
        id_column: the column containing the name, ISO3, ISO2, DACcode, UN code, etc.
        id_type: the type of ID used in th id_column. The default 'regex' tries to infer
            using the rules from the 'country_converter' package. For the DAC codes,
            "DACcode" must be passed.
        date_column: Optionally, a date column can be specified. If so, the GDP
            for that year will be used. If it's missing, it will be missing in the returned
            column as well. If the date isn't specified, the most recent _data is used.
        include_estimates: Whether to include years for which the WEO _data is labelled as
            estimates.
        usd: Whether to add the _data as US dollars or Local Currency Units.
        target_column: the column where the gdp _data will be stored.
        update_data: whether to update the underlying _data or not.

    Returns:
        DataFrame: the original DataFrame with a new column containing the gdp _data from
            the IMF World Economic Outlook.
    """

    # validate parameters
    df_, on_ = __validate_add_column_params(
        df=df.copy(deep=True),
        id_column=id_column,
        id_type=id_type,
        date_column=date_column,
    )

    gdp_df = get_gdp_df(
        usd=usd,
        most_recent_only=True if date_column is None else False,
        include_estimates=include_estimates,
        update_data=update_data,
    ).rename(
        columns={
            "iso_code": "id_",
            "year": "merge_year",
            "value": target_column,
        }
    )

    if date_column is None:
        gdp_df = gdp_df.drop(columns=["merge_year"])

    df_ = df_.merge(gdp_df, on=on_, how="left")

    if date_column is not None:
        df_ = df_.drop(columns=["merge_year"])

    return df_.drop(columns=["id_"])


def add_gov_expenditure_column(
    df: pd.DataFrame,
    id_column: str,
    id_type: str | None = None,
    date_column: str | None = None,
    target_column: str = "gov_exp",
    usd: bool = True,
    include_estimates: bool = False,
    update_data: bool = False,
) -> pd.DataFrame:
    """Add Government Expenditure column to a dataframe

    Args:
        df: the dataframe to which the column will be added
        id_column: the column containing the name, ISO3, ISO2, DACcode, UN code, etc.
        id_type: the type of ID used in th id_column. The default 'regex' tries to infer
            using the rules from the 'country_converter' package. For the DAC codes,
            "DACcode" must be passed.
        date_column: Optionally, a date column can be specified. If so, the expenditure
            for that year will be used. If it's missing, it will be missing in the returned
            column as well. If the date isn't specified, the most recent _data is used.
        include_estimates: Whether to include years for which the WEO _data is labelled as
            estimates.
        usd: Whether to add the _data as US dollars or Local Currency Units.
        target_column: the column where the expenditure _data will be stored.
        update_data: whether to update the underlying _data or not.

    Returns:
        DataFrame: the original DataFrame with a new column containing the expenditure _data from
            the IMF World Economic Outlook.
    """

    # validate parameters
    df_, on_ = __validate_add_column_params(
        df=df.copy(deep=True),
        id_column=id_column,
        id_type=id_type,
        date_column=date_column,
    )

    gov_df = get_gov_expenditure_df(
        usd=usd,
        most_recent_only=True if date_column is None else False,
        update_data=update_data,
        include_estimates=include_estimates,
    ).rename(
        columns={
            "iso_code": "id_",
            "year": "merge_year",
            "value": target_column,
        }
    )

    if date_column is None:
        gov_df = gov_df.drop(columns=["merge_year"])

    df_ = df_.merge(gov_df, on=on_, how="left")

    if date_column is not None:
        df_ = df_.drop(columns=["merge_year"])

    return df_.drop(columns=["id_"])


def add_gdp_share_column(
    df: pd.DataFrame,
    id_column: str,
    id_type: str | None = None,
    date_column: str | None = None,
    value_column: str = "value",
    target_column: str = "gdp_share",
    decimals: int = 2,
    usd: bool = False,
    include_estimates: bool = False,
    update_data: bool = False,
) -> pd.DataFrame:
    """Add value as share of GDP column to a dataframe

    Args:
        df: the dataframe to which the column will be added
        id_column: the column containing the name, ISO3, ISO2, DACcode, UN code, etc.
        id_type: the type of ID used in th id_column. The default 'regex' tries to infer
            using the rules from the 'country_converter' package. For the DAC codes,
            "DACcode" must be passed.
        date_column: Optionally, a date column can be specified. If so, the GDP
            for that year will be used. If it's missing, it will be missing in the returned
            column as well. If the date isn't specified, the most recent _data is used.
        value_column: the column containing the value to be converted to a share of GDP.
        decimals: the number of decimals to use in the returned column.
        include_estimates: Whether to include years for which the WEO _data is labelled as
            estimates.
        usd: Whether to add the data as US dollars or Local Currency Units.
        target_column: the column where the gdp _data will be stored.
        update_data: whether to update the underlying _data or not.


    Returns:
        DataFrame: the original DataFrame with a new column containing the _data as a share
         of gdp _data, using the IMF World Economic Outlook.
    """
    kwargs = {
        k: v for k, v in dict(locals()).items() if k not in ["value_column", "decimals"]
    }

    if value_column not in df.columns:
        raise ValueError(f"value_column '{value_column}' not in dataframe columns")

    df_ = add_gdp_column(**kwargs)

    _ = df.copy(deep=True).reset_index(drop=True)

    _[target_column] = round(100 * df_[value_column] / df_[target_column], decimals)

    return _


def add_population_share_column(
    df: pd.DataFrame,
    id_column: str,
    id_type: str | None = None,
    date_column: str | None = None,
    value_column: str = "value",
    target_column: str = "population_share",
    decimals: int = 2,
    update_data: bool = False,
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
            column as well. If the _data isn't specified, the most recent population _data from
            the world bank is used.
        value_column: the column containing the value to be used in the calculation.
        target_column: the column where the population _data will be stored.
        decimals: the number of decimals to use in the returned column.
        update_data: whether to update the underlying _data or not.

    Returns:
        DataFrame: the original DataFrame with a new column containing value as share of
        population.
    """

    kwargs = {
        k: v for k, v in dict(locals()).items() if k not in ["value_column", "decimals"]
    }

    if value_column not in df.columns:
        raise ValueError(f"value_column '{value_column}' not in dataframe columns")

    df_ = add_population_column(**kwargs)

    _ = df.copy(deep=True).reset_index(drop=True)

    _[target_column] = round(100 * df_[value_column] / df_[target_column], decimals)

    return _


def add_gov_exp_share_column(
    df: pd.DataFrame,
    id_column: str,
    id_type: str | None = None,
    date_column: str | None = None,
    value_column: str = "value",
    target_column: str = "gov_exp_share",
    usd: bool = False,
    include_estimates: bool = False,
    update_data: bool = False,
) -> pd.DataFrame:
    """Add value as share of Government Expenditure column to a dataframe

    Args:
        df: the dataframe to which the column will be added
        id_column: the column containing the name, ISO3, ISO2, DACcode, UN code, etc.
        id_type: the type of ID used in th id_column. The default 'regex' tries to infer
            using the rules from the 'country_converter' package. For the DAC codes,
            "DACcode" must be passed.
        date_column: Optionally, a date column can be specified. If so, the expenditure _data
            for that year will be used. If it's missing, it will be missing in the returned
            column as well. If the date isn't specified, the most recent _data is used.
        value_column: the column containing the value to be converted to a share of expenditure.
        include_estimates: Whether to include years for which the WEO _data is labelled as
            estimates.
        usd: Whether to add the _data as US dollars or Local Currency Units.
        target_column: the column where the expenditure _data will be stored.
        update_data: whether to update the underlying _data or not.

    Returns:
        DataFrame: the original DataFrame with a new column containing the _data as a share
         of expenditure, using the IMF World Economic Outlook.
    """
    kwargs = {
        k: v for k, v in dict(locals()).items() if k not in ["value_column", "decimals"]
    }

    if value_column not in df.columns:
        raise ValueError(f"value_column '{value_column}' not in dataframe columns")

    df_ = add_gov_expenditure_column(**kwargs)

    _ = df.copy(deep=True).reset_index(drop=True)

    _[target_column] = round(100 * df_[value_column] / df_[target_column], 3)

    return _


def add_income_level_column(
    df: pd.DataFrame,
    id_column: str,
    id_type: str | None = None,
    target_column: str = "income_level",
    update_data: bool = False,
) -> pd.DataFrame:
    """Add an income levels column to a dataframe

    Args:
        df: the dataframe to which the column will be added
        id_column: the column containing the name, ISO3, ISO2, DACcode, UN code, etc.
        id_type: the type of ID used in th id_column. The default 'regex' tries to infer
            using the rules from the 'country_converter' package. For the DAC codes,
            "DACcode" must be passed.
        target_column: the column where the income level _data will be stored.
        update_data: whether to update the underlying _data or not.

    Returns:
        DataFrame: the original DataFrame with a new column containing the income level _data.
    """
    from bblocks.other_tools.dictionaries import income_levels, __download_income_levels

    # validate parameters
    df_, on_ = __validate_add_column_params(
        df=df.copy(deep=True),
        id_column=id_column,
        id_type=id_type,
        date_column=None,
    )

    if update_data:
        __download_income_levels()
        print("Downloaded income levels _data")

    df[target_column] = df_["id_"].map(income_levels())

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
        group_by: the column(s) by which to group the _data to calculate the median.
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


def add_flourish_geometries(
    df: pd.DataFrame,
    id_column: str,
    id_type: str | None = None,
    target_column: str = "geometry",
) -> pd.DataFrame:
    """Add flourish geometries column to a dataframe

    Args:
        df: the dataframe to which the column will be added
        id_column: the column containing the name, ISO3, ISO2, DAC code, UN code, etc.
        id_type: the type of ID used in th id_column. The default 'regex' tries to infer
            using the rules from the 'country_converter' package. For the DAC codes,
            "DAC" must be passed.
        target_column: the column where the flourish geometries  will be stored.
    Returns:
        DataFrame: the original DataFrame with a new column containing the flourish geometries.
    """
    from bblocks.other_tools.dictionaries import flourish_geometries

    if id_column not in df.columns:
        raise ValueError(f"id_column '{id_column}' not in dataframe columns")

    df_, _ = __validate_add_column_params(
        df=df.copy(deep=True), id_column=id_column, id_type=id_type, date_column=None
    )

    df[target_column] = df_.id_.map(flourish_geometries())

    return df


def add_value_as_share(
    df: pd.DataFrame,
    value_col: str,
    share_of_value_col: str,
    target_col: str | None = None,
    decimals: int = 2,
) -> pd.DataFrame:
    # Copy the dataframe to avoid modifying the original one
    df = df.copy(deep=True)

    if value_col not in df.columns:
        raise ValueError(f"value_col '{value_col}' not in dataframe columns")

    if share_of_value_col not in df.columns:
        raise ValueError(
            f"share_of_value_col '{share_of_value_col}' not in dataframe columns"
        )

    if target_col is None:
        target_col = f"{value_col}_as_share_of_{share_of_value_col}"

    df[target_col] = round(100 * df[value_col] / df[share_of_value_col], decimals)

    return df
