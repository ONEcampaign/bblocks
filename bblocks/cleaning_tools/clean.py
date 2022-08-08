import re
from typing import Type, Optional

import country_converter as coco
import pandas as pd
from numpy import nan


def clean_number(number: str, to: Type = float) -> float | int:
    """Clean a string and return as float or integer.
    When selecting to=int, the default python round behaviour is used.

    Args:
        number: the string to be cleaned
        to: the type to convert to (int or float)

    """

    if not isinstance(number, str):
        number = str(number)

    number = re.sub(r"[^\d.]", "", number)

    if number == "":
        return nan

    if to == float:
        return float(number)

    if to == int:
        return int(round(float(number)))


def clean_numeric_series(
    data: pd.Series | pd.DataFrame,
    series_columns: Optional[str | list] = None,
    to: Type = float,
) -> pd.DataFrame | pd.Series:
    """Clean a numeric column in a Pandas DataFrame or a Pandas Series which is
    meant to be numeric. When selecting to=int, the default python round behaviour
    is used.

    Args:
        data: it accepts a series or a dataframe. If a dataframe is passed, the column(s)
            to clean must be specified
        series_columns: optionally declared (only when data is a dataframe). To apply to
            one or more columns.
        to: the type to convert to (int or float)

    """

    if isinstance(data, pd.DataFrame) and (series_columns is None):
        raise ValueError("series_column must be specified when data is a DataFrame")

    if isinstance(data, pd.DataFrame):
        if isinstance(series_columns, str):
            series_columns = [series_columns]

        data[series_columns] = data[series_columns].apply(
            lambda s: s.apply(clean_number, to=to), axis=1
        )
        return data

    if isinstance(data, pd.Series):
        return data.apply(clean_number, to=to)


def to_date_column(series: pd.Series, date_format: Optional[str] = None) -> pd.Series:
    """Converts a Pandas series into a date series.
    The series must contain integers or strings that can be converted into
    datetime objects"""

    if pd.api.types.is_datetime64_any_dtype(series):
        return series

    if pd.api.types.is_numeric_dtype(series):
        try:
            return pd.to_datetime(series, format="%Y")

        except ValueError:
            raise ValueError(
                f"could not parse date format in. "
                f"To fix, convert column to datetime"
            )
    if date_format is None:
        return pd.to_datetime(series, infer_datetime_format=True)
    else:
        return pd.to_datetime(series, format=date_format)


def convert_id(
    series: pd.Series,
    from_type: str = "regex",
    to_type: str = "ISO3",
    not_found: str | None = None,
    *,
    additional_mapping: dict = None,
) -> pd.Series:
    """Takes a Pandas' series with country IDs and converts them into the desired type.

    Args:
        series: the Pandas series to convert
        from_type: the classification type according to which the series is encoded.
            Available types come from the country_converter package
            (https://github.com/konstantinstadler/country_converter#classification-schemes)
            For example: ISO3, ISO2, name_short, DACcode, etc.
        to_type: the target classification type. Same options as from_type
        not_found: what to do if the value is not found. Can pass a string or None.
            If None, the original value is passed through.
        additional_mapping: Optionally, a dictionary with additional mappings can be used.
            The keys are the values to be converted and the values are the converted values.
            The keys follow the same datatype as the original values. The values must follow
            the same datatype as the target type.
    """

    # if from and to are the same, return without changing anything
    if from_type == to_type:
        return series

    # Create convert object
    cc = coco.CountryConverter()

    # save the original index
    idx = series.index

    # Get the unique values for mapping. This is done in order to significantly improve
    # the performance of country_converter with very long datasets.
    s_unique = series.unique()

    # Create a correspondence dictionary
    raw_mapping = cc.get_correspondence_dict(
        classA=from_type, classB=to_type, replace_numeric=False
    )

    # If the keys are numeric, transform to integers. Otherwise, just unpack value lists
    if pd.api.types.is_numeric_dtype(series):
        mapping = {int(k): v[0] for k, v in raw_mapping.items()}
    else:
        mapping = {k: v[0] for k, v in raw_mapping.items()}

    # If values are integers, convert to integer types. Done with pd.to_numeric to avoid
    # errors with missing data
    if pd.api.types.is_numeric_dtype(pd.Series(mapping.values())):
        mapping = {
            k: pd.to_numeric(v, errors="coerce", downcast="integer")
            for k, v in mapping.items()
        }

    # In country_converter, ISO2 and ISO3 accept regular expressions. In order to remove
    # This removes the regular expression characters in favor of the first match.
    if to_type.lower() in ["iso2", "iso3"]:
        mapping = {
            k: re.sub(r"\W+", "", v.split("|")[0]).upper() for k, v in mapping.items()
        }

    # Create a list of missing values from the passed list with respect to the target
    _ = [k for k in s_unique if k not in mapping]
    missing = pd.Series(_, index=_, dtype=series.dtype)

    # If additional_mapping is passed, add to the mapping
    if additional_mapping is not None:
        mapping = mapping | additional_mapping

    return series.map(mapping).fillna(series if not_found is None else not_found)
