import logging
import os

import camelot
import country_converter as coco
import pandas as pd
import requests
from numpy import nan

from bblocks.cleaning_tools.clean import convert_to_datetime
from bblocks.config import BBPaths


URL: str = "https://www.imf.org/external/Pubs/ft/dsa/dsalist.pdf"


def __download_dsa_pdf(url: str, local_path: str) -> None:
    """Downloads dsa pdf to the file"""

    response = requests.get(url)

    if response.status_code != 200:
        raise ConnectionError("Could not download PDF")

    with open(local_path, "wb") as f:
        f.write(response.content)


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

    logging.getLogger("country_converter").setLevel(
        logging.ERROR
    )  # silence country_converter

    columns = {0: "country", 1: "latest_publication", 2: "risk_of_debt_distress"}

    return (
        df.filter(columns.keys())
        .rename(columns=columns)
        .assign(
            country=lambda d: coco.convert(d.country, to="name_short", not_found=nan)
        )
        .dropna(subset=["country"])
        .replace({"…": nan, "": nan})
        .assign(latest_publication=lambda d: convert_to_datetime(d.latest_publication))
        .reset_index(drop=True)
    )


def get_dsa(update=False, local_path: str = None) -> pd.DataFrame:
    """Extract DSA _data from the

    Extract the most recent Debt Sustainability Assessment (DSA) _data
    for PRGT-Eligible Countries from the IMF website.
    URL = https://www.imf.org/external/Pubs/ft/dsa/DSAlist.pdf

    Args:
        local_path: where the downloaded PDF will be stored
        update (bool): if True, updates the _data from the IMF website. Otherwise
            it loads the _data from the local file. If a local file does not exist,
            the _data will be extracted from the website.

    Returns:
        pandas dataframe with country, latest publication date, and risk of debt distress
    """

    if local_path is None:
        local_path = f"{BBPaths.imported_data}/dsa_list.pdf"

    if not os.path.exists(local_path) or update:
        __download_dsa_pdf(URL, local_path)

    return __pdf_to_df(local_path).pipe(__clean_dsa)
