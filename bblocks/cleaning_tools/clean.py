import re
import warnings
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

    # Get the unique values for mapping. This is done in order to significantly improve
    # the performance of country_converter with very long datasets.
    s_unique = series.unique()

    # Create a correspondence dictionary
    mapping = pd.Series(
        cc.convert(names=s_unique, src=from_type, to=to_type, not_found=nan),
        index=s_unique,
    ).to_dict()

    # If additional_mapping is passed, add to the mapping
    if additional_mapping is not None:
        mapping = mapping | additional_mapping

    return series.map(mapping).fillna(series if not_found is None else not_found)


def date_to_str(series: pd.Series, date_format: str = "%d %B %Y") -> pd.Series:
    """Converts a Pandas' series into a string series.

    Args:
        series: the Pandas series to convert to a formatted date string
        date_format: the format to use for the date string. The default is "%d %B %Y"
    """

    if not pd.api.types.is_datetime64_any_dtype(series):
        try:
            series = pd.to_datetime(series, infer_datetime_format=True)
        except ValueError:
            raise ValueError(
                f"could not parse date format in. "
                f"To fix, convert column to datetime"
            )

    return series.dt.strftime(date_format)


def format_number(
    series: pd.Series,
    as_units: bool = False,
    as_percentage: bool = False,
    as_millions: bool = False,
    as_billions: bool = False,
    decimals: int = 2,
    add_sign: bool = False,
    other_format: str = "{:,.2f}",
) -> pd.Series:
    """Formats a Pandas' numeric series into a formatted string series.

    Args:
        series: the series to convert to a formatted string
        as_units: formatted with commas to separate thousands and the specified decimals
        as_percentage: formatted as a percentage with the specified decimals. This assumes
            that the series contains numbers where 1 would equal 100%.
        as_millions: divided by 1 million, formatted with commas and the specified decimals
        as_billions: divided by 1 billion, formatted with commas and the specified decimals
        decimals: the number of decimals to use
        other_format: Other formats to use. This option can only be used if all others
            are false. Examples are available at:
            https://mkaz.blog/code/python-string-format-cookbook/
    """

    if not pd.api.types.is_numeric_dtype(series):
        raise ValueError(f"The series must be of numeric type")

    if (true_params := sum([as_units, as_percentage, as_millions, as_billions])) > 1:
        raise KeyError(
            f"Only one of as_units, as_percentage, as_millions, as_billions can be True"
        )
    if true_params > 0 and other_format != "{:,.2f}":
        raise KeyError(f"other_format can only be used if all other options are False")

    if true_params == 0:
        warnings.showwarning(
            "Using 'other_format'. The decimals setting is ignored.",
            UserWarning,
            "clean.py",
            168,
        )

    sign = "-" if add_sign else ""

    formats = {
        "as_units": f"{{:{sign},.{decimals}f}}",
        "as_percentage": f"{{:{sign},.{decimals}%}}",
        "as_millions": f"{{:{sign},.{decimals}f}}",
        "as_billions": f"{{:{sign},.{decimals}f}}",
    }

    if as_units:
        return series.map(formats["as_units"].format)

    if as_percentage:
        return series.map(formats["as_percentage"].format)

    if as_millions:
        series = series / 1e6
        return series.map(formats["as_millions"].format)

    if as_billions:
        series = series / 1e9
        return series.map(formats["as_billions"].format)

    return series.map(f"{other_format}".format)
