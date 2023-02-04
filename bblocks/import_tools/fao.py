import os
import pathlib

import pandas as pd
import requests
from bs4 import BeautifulSoup

from bblocks.config import BBPaths

URL = "https://www.fao.org/worldfoodsituation/foodpricesindex/en/"


def __scrape_index_df(url: str) -> pd.DataFrame:
    """Parse FAO food price index page to retrieve _data csv url"""

    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(url)

    if response.status_code != 200 or response.url != url:
        raise ConnectionError("FAO food price index page not found")

    soup = BeautifulSoup(response.content, "html.parser")
    href = soup.find_all(string="CSV")[0].parent.get("href")

    return pd.read_csv(
        f"https://www.fao.org/{href}",
        skiprows=2,
        storage_options=headers,
        parse_dates=["Date"],
    )


def __clean_fao_food_price_index(df: pd.DataFrame) -> pd.DataFrame:
    """Clean index dataframe"""

    return (
        df.rename(columns={"Date": "date", "Food Price Index": "food_price_index"})
        .dropna(subset="date")
        .reset_index(drop=True)
        .loc[:, lambda d: ~d.columns.str.contains("Unnamed")]
    )


def get_fao_index(
    local_path: str | pathlib.Path = BBPaths.imported_data / "fao_index.csv",
    update: bool = False,
) -> pd.DataFrame:
    """Get FAO food price index

    Args:
        local_path: where the downloaded CSV will be stored

        update: if True, updates the _data from the FAO website. Otherwise
            it loads the _data from the local file. If a local file does not exist,
            the _data will be extracted from the website.
    """

    if not os.path.exists(local_path) or update:
        df = __scrape_index_df(URL).pipe(__clean_fao_food_price_index)
        df.to_csv(local_path, index=False)

    return pd.read_csv(local_path)
