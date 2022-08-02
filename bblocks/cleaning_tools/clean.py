from typing import Type, Optional
import country_converter as coco

import pandas as pd
from numpy import nan
import re

from bblocks.other_tools import dictionaries


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


def convert_id(
    series: pd.Series,
    from_type: str = "regex",
    to_type: str = "ISO3",
    not_found: str | None = None,
) -> pd.Series:

    """Takes a Pandas' series with country IDs and converts them into the desired type"""

    import logging

    logger = logging.getLogger("country_converter")
    logger.setLevel(logging.ERROR)

    # save the original index
    idx = series.index

    # if from and to are the same, return without changing anything
    if from_type == to_type:
        return series

    if from_type == "DAC":
        s_ = series.map(dictionaries.dac_codes).fillna(series)

        return pd.Series(
            coco.convert(s_, src="ISO3", to=to_type, not_found=not_found), index=idx
        )

    if to_type == "DAC" and from_type != "DAC":
        s_ = pd.Series(
            coco.convert(series, src=from_type, to="ISO3", not_found=not_found),
            index=idx,
        )

        return s_.map(dictionaries.dac_codes.reverse()).fillna(s_)

    else:
        return pd.Series(
            coco.convert(series, src=from_type, to=to_type, not_found=not_found),
            index=idx,
        )
