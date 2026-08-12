"""Module to import IMF LIC Debt Sustainability Assessments (DSA) data


Main function: get_dsa()

"""

import re
from io import BytesIO
from typing import Final
from functools import lru_cache

import camelot
import httpx
import pandas as pd

from bblocks.data_importers.config import (
    Fields,
    DataFormattingError,
    DataExtractionError,
)
from bblocks.data_importers.data_validators import DataFrameValidator
from bblocks.data_importers.utilities import logger, convert_dtypes


URL: Final[str] = "https://www.imf.org/external/Pubs/ft/dsa/DSAlist.pdf"
_FOOTNOTE_TRAILER = re.compile(r"\s*\d+/\s*$")

# Columns
COLS = {
    1: Fields.country_name,
    2: "latest_publication",
    3: "risk_of_debt_distress",
    5: "debt_sustainability_assessment",
    6: "joint_with_world_bank",
    7: "latest_dsa_discussed",
}


def __strip_footnote_trailer(x: str | None) -> str | None:
    """Strip footnote trailer from string"""

    if not isinstance(x, str):
        return x
    return _FOOTNOTE_TRAILER.sub("", x).strip()


def __normalise_country_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize country names"""

    if Fields.country_name in df.columns:
        df[Fields.country_name] = df[Fields.country_name].apply(
            __strip_footnote_trailer
        )

    if df[Fields.country_name].isnull().any():
        raise DataFormattingError("Null values found in country names after cleaning")

    return df


def _download_pdf(url: str) -> bytes:
    """Download PDF"""

    headers = {
        "User-Agent": "bblocks data importers @ https://data.one.org",
        "Accept": "application/pdf",
    }

    try:
        with httpx.Client(follow_redirects=True, timeout=httpx.Timeout(30.0)) as client:
            r = client.get(url, headers=headers)
            r.raise_for_status()
            return r.content

    except httpx.RequestError as e:
        raise DataExtractionError(f"Error downloading DSA PDF: {str(e)}") from e

    except httpx.HTTPStatusError as e:
        raise DataExtractionError(f"Error downloading DSA PDF: {str(e)}") from e


def _pdf_to_df(src: bytes) -> pd.DataFrame:
    """Extract the single table from the one-page PDF"""

    file = BytesIO(src)

    try:
        tables = camelot.read_pdf(file, flavor="stream")

    except Exception as e:
        raise DataExtractionError(f"Could not read PDF to a dataframe: {str(e)}") from e

    if len(tables) != 1:
        raise DataExtractionError("Invalid PDF format. Check PDF")

    return tables[0].df


def __clean_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize the DSA table headers."""

    return df.filter(COLS.keys()).rename(columns=COLS)


def __normalise_booleans(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Normalize boolean columns to True/False."""
    if column in df.columns:
        df[column] = df[column].str.lower().eq("yes")
    return df


def __normalise_debt_distress(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise the "risk of debt distress" column"""

    df["risk_of_debt_distress"] = (
        df["risk_of_debt_distress"]
        .pipe(__strip_footnote_trailer)
        .str.strip()
        .str.capitalize()
    )

    # replace "" with NaN
    df["risk_of_debt_distress"] = df["risk_of_debt_distress"].replace("", pd.NA)

    return df


def __normalise_debt_sustainability(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise the "debt sustainability assessment" column"""

    df["debt_sustainability_assessment"] = (
        df["debt_sustainability_assessment"]
        .apply(__strip_footnote_trailer)
        .str.strip()
        .str.capitalize()
    )

    # replace "" with NaN
    df["debt_sustainability_assessment"] = df["debt_sustainability_assessment"].replace(
        "", pd.NA
    )

    return df


def __normalise_date(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Normalize date columns to datetime."""

    if column in df.columns:
        df[column] = pd.to_datetime(df[column], errors="coerce", format=None, utc=False)
    return df


def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Cleaning steps"""

    try:

        return (
            df.loc[lambda d: d[d.columns[0]].notna() & (d[d.columns[0]] != "")]
            .reset_index(drop=True)
            .loc[1:, :]  # drop header row
            .pipe(__clean_headers)
            .pipe(__normalise_booleans, "joint_with_world_bank")
            .pipe(__normalise_country_names)
            .pipe(__normalise_date, "latest_publication")
            .pipe(__normalise_date, "latest_dsa_discussed")
            .pipe(__normalise_debt_distress)
            .pipe(__normalise_debt_sustainability)
            .reset_index(drop=True)
            .pipe(convert_dtypes)
        )

    except Exception as e:
        raise DataFormattingError(f"Error cleaning DSA data: {str(e)}")


@lru_cache
def get_dsa() -> pd.DataFrame:
    """Get IMF LIC DSA list

    Get the list of LIC Debt Sustainability Assessments for PRGT-Eligible Countries
    as a pandas DataFrame, from:

    https://www.imf.org/external/Pubs/ft/dsa/DSAlist.pdf

    This function used LRU caching to avoid multiple downloads of the same data. To clear the
    cache restart the Python session.

    Returns:
        A DataFrame containing the DSA list with the following columns
            - country_name: Country name
            - latest_publication: Date of the latest DSA publication
            - risk_of_debt_distress: Risk of debt distress classification
            - debt_sustainability_assessment: Debt sustainability classification
            - joint_with_world_bank: Boolean indicating if the DSA was done jointly with the World Bank
            - latest_dsa_discussed: Date of latest DSA discussed by the Executive Board but not yet published
    """

    logger.info("Fetching DSA")

    content = _download_pdf(url=URL)
    df = _pdf_to_df(content)
    df = _clean_df(df)

    # Validation
    DataFrameValidator().validate(df, list(COLS.values()))

    logger.info("Successfully fetched DSA data")

    return df
