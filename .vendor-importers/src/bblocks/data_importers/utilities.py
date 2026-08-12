import pandas as pd
from typing import Literal, Mapping
import country_converter as coco
import pyarrow as pa

from bblocks.data_importers.config import logger


def convert_dtypes(
    df: pd.DataFrame,
    backend: Literal["pyarrow", "numpy_nullable"] = "pyarrow",
    *,
    casts: Mapping[str, pa.DataType] | None = None,
) -> pd.DataFrame:
    """Converts the DataFrame to the specified backend dtypes

    Args:
        df: The DataFrame to convert
        backend: The backend to use for the conversion. Default is "pyarrow"
        casts: Optional mapping of column names to pyarrow DataTypes for explicit casting

    Returns:
        A DataFrame with the pyarrow dtypes
    """

    # Check if the backend is valid
    supported_backends = {"pyarrow", "numpy_nullable"}
    if backend not in supported_backends:
        raise ValueError(
            f"Unsupported backend '{backend}'. Supported backends are {supported_backends}."
        )

    # Non-arrow path stays unchanged
    if backend != "pyarrow":
        return df.convert_dtypes(dtype_backend=backend)

    # Convert all columns to Arrow-backed dtypes once
    out = df.convert_dtypes(dtype_backend="pyarrow")

    if not casts:
        return out

    missing = [c for c in casts if c not in out.columns]
    if missing:
        raise KeyError(f"Columns not found: {missing}")

    # Cast only specified columns
    for col, pa_type in casts.items():
        arr = pa.array(out[col], type=pa_type)
        out[col] = pd.Series(
            arr, index=out.index, name=col, dtype=pd.ArrowDtype(pa_type)
        )

    return out


def convert_countries_to_unique_list(
    countries: list, src: str | None = None, to: str = "ISO3"
) -> list:
    """Converts a list of country names to a unique list of countries in the specified format

    Args:
        countries: A list of country names
        src: The source format of the country names. Default is None, uses the conversion mechanism of the country_converter package to determine the source format
        to: The format to convert the country names to. Default is "ISO3"

    Returns:
        A unique list of countries in the specified format
    """

    converted_list = set()

    for country in countries:
        converted_country = coco.convert(country, src=src, to=to)
        if converted_country == "not found":
            logger.warning(f"Country not found: {country}")
        else:
            converted_list.add(converted_country)

    return list(converted_list)
