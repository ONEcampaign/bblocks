from __future__ import annotations
import logging

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import pandas as pd
import requests
import camelot
import country_converter as coco
from numpy import nan
import os

from bblocks.config import PATHS


@dataclass
class ImportData(ABC):
    indicators: dict = field(default_factory=dict)
    data: pd.DataFrame = None
    update_data: bool = False

    @abstractmethod
    def load_indicator(self, **kwargs) -> ImportData:
        """Load the data saved on disk"""
        pass

    @abstractmethod
    def update(self, **kwargs) -> ImportData:
        """Update object data"""
        pass

    @abstractmethod
    def get_data(self, **kwargs) -> pd.DataFrame:
        """Get the data as a Pandas DataFrame"""
        pass


# =============================================================================
# DSA scraper
# =============================================================================

def __download_dsa_pdf(url: str, local_path: str) -> None:
    """Downloads dsa pdf to the file"""

    try:
        response = requests.get(url)
        with open(local_path, "wb") as f:
            f.write(response.content)
    except ConnectionError:
        raise ConnectionError("Could not download PDF")


def __pdf_to_df(local_path: str) -> pd.DataFrame:
    """Reads a pdf and returns a dataframe"""

    try:
        tables = camelot.read_pdf(local_path, flavor="stream")
        if len(tables) != 1:
            raise ValueError("Invalid PDF format. Check PDF")

        return tables[0].df

    except ValueError:
        raise ValueError("Could not read PDF to a dataframe")


def __clean_dsa(df: pd.DataFrame) -> pd.DataFrame:
    """Clean dsa dataframe"""

    logging.getLogger("country_converter").setLevel(logging.ERROR)  # silence country_converter

    columns = {0: "country",
               1: "latest_publication",
               2: "risk_of_debt_distress"}

    return (df
            .filter(columns.keys())
            .rename(columns=columns)
            .assign(country=lambda d: coco.convert(d.country, to="name_short", not_found=nan))
            .dropna(subset=["country"])
            .replace({"…": nan, "": nan})
            .assign(latest_publication=lambda d: pd.to_datetime(d.latest_publication))
            .reset_index(drop=True)
            )


def get_dsa(update=False) -> pd.DataFrame:
    """Extract DSA data from the

    Extract the most recent Debt Sustainability Assessment (DSA) data
    for PRGT-Eligible Countries from the IMF website.
    URL = https://www.imf.org/external/Pubs/ft/dsa/DSAlist.pdf

    Args:
        update (bool): if True, updates the data from the IMF website. Otherwise
            it loads the data from the local file. If a local file does not exist,
            the data will be extracted from the website.

    Returns:
        pandas dataframe with country, latest publication date, and risk of debt distress
    """

    url = "https://www.imf.org/external/Pubs/ft/dsa/dsalist.pdf"
    local_path = f'{PATHS.imported_data}/dsa_list.pdf'

    if not os.path.exists(local_path) or update:
        __download_dsa_pdf(url, local_path)

    return __pdf_to_df(local_path).pipe(__clean_dsa)
