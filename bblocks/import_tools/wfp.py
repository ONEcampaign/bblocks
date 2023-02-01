import json
import os
from dataclasses import dataclass
from typing import KeysView

import pandas as pd
import requests

from bblocks.config import BBPaths
from bblocks.import_tools.common import append_new_data, ImportData
from bblocks.logger import logger

COUNTRY_URL: str = "https://api.hungermapdata.org/covid/data"
INFLATION_URL: str = f"https://api.vam.wfp.org/dataviz/api/GetCsv?idx=71,116&iso3="
FOOD_URL: str = (
    "https://5763353767114258.eu-central-1.fc.aliyuncs.com/2016-08-15/"
    f"proxy/wfp-data-api.36/map-data/adm0/"
)


def _get_country_codes() -> None:
    """Script to fetch the country codes used by WFP. Saved as a dataframe."""

    # Get the json file from WFP website
    file = requests.get(COUNTRY_URL).content

    # WFP codes
    wfp = json.loads(file)["countries"]

    # Create a dictionary with the iso_code: country code
    codes = {d["iso3"]: d["adm0_code"] for d in wfp}

    # Create a dataframe with the country codes and save
    df = pd.DataFrame(list(codes.items()), columns=["iso_code", "wfp_code"])

    if not os.path.exists(BBPaths.wfp_data):
        os.makedirs(BBPaths.wfp_data)

    df.to_csv(BBPaths.wfp_data / r"wfp_country_codes.csv", index=False)

    logger.info("WFP country codes successfully downloaded.")


def _read_wfp_country_codes() -> dict:
    """Returns a dictionary with the country codes used by WFP."""
    file_path = BBPaths.wfp_data / "wfp_country_codes.csv"

    if not os.path.exists(file_path):
        _get_country_codes()

    d = pd.read_csv(file_path)
    return dict(zip(d["iso_code"], d["wfp_code"].astype(int)))


def _get_inflation(
    country_iso: str,
) -> None:
    """Get inflation data from VAM for a single country based on iso code"""

    try:
        df = pd.read_csv(
            INFLATION_URL + country_iso,
            usecols=[0, 1, 2],
            skipfooter=2,
            engine="python",
            parse_dates=["Time"],
        )
    except ConnectionError:
        print(f"Data not available for {country_iso}")
        return

    df = (
        df.rename(
            columns={
                "Time": "date",
                "Value (percent)": "value",
                "Indicator": "indicator",
            }
        )
        .assign(iso_code=country_iso)
        .sort_values(by=["indicator", "date"])
    )

    if not os.path.exists(BBPaths.wfp_data):
        os.makedirs(BBPaths.wfp_data)

    logger.info(f"VAM inflation data for {country_iso} successfully downloaded.")

    df.to_csv(BBPaths.wfp_data / f"{country_iso}_inflation.csv", index=False)


def _get_insufficient_food(code: int, iso: str) -> None:
    """Get food consumption data from WFP"""

    # Get the json file from WFP website. If empty return None
    r = requests.get(FOOD_URL + f"{code}/countryData.json")

    # Check if response is invalid return None
    if r.status_code == 404:
        return None

    # Create dataframe
    data = pd.DataFrame(r.json()["fcsGraph"])

    # Code may be valid but _data may be empty. If so, return None
    if len(data) == 0:
        return None

    # If _data is valid, clean it and save to munged
    data = (
        data.rename(
            columns=(
                {
                    "x": "date",
                    "fcs": "value",
                    "fcsHigh": "value_high",
                    "fcsLow": "value_low",
                }
            )
        )
        .assign(date=lambda d: pd.to_datetime(d.date, format="%Y-%m-%d"), iso_code=iso)
        .pipe(
            append_new_data, BBPaths.wfp_data / f"{iso}_insufficient_food.csv", "date"
        )
    )

    if not os.path.exists(BBPaths.wfp_data):
        os.makedirs(BBPaths.wfp_data)

    data.to_csv(BBPaths.wfp_data / f"{iso}_insufficient_food.csv", index=False)
    logger.info(f"Insufficient food data for {iso} successfully downloaded.")


def _read_files(iso_code: str, file_name: str) -> pd.DataFrame:
    try:
        return pd.read_csv(
            BBPaths.wfp_data / f"{iso_code}_{file_name}.csv", parse_dates=["date"]
        )

    except FileNotFoundError:
        return pd.DataFrame()


def _read_insufficient_food(iso_codes: list) -> pd.DataFrame:
    """Read and merge the data for the given iso codes."""

    data = pd.DataFrame()

    for iso in iso_codes:
        data = pd.concat(
            [data, _read_files(iso, "insufficient_food")], ignore_index=True
        )

    if len(data) == 0:
        print("No insufficient food data available. Run update to download _data")
        return data

    return (
        data.sort_values(by=["iso_code", "date"])
        .assign(indicator="people_with_insufficient_food_consumption")
        .reset_index(drop=True)
    )


def _read_inflation(iso_codes: list) -> pd.DataFrame:
    """Read and merge the data for the given iso codes."""

    data = pd.DataFrame()

    for iso in iso_codes:
        data = pd.concat([data, _read_files(iso, "inflation")], ignore_index=True)

    if len(data) == 0:
        print("No inflation data available. Run update to download data")
        return data

    return data.sort_values(by=["iso_code", "date"]).reset_index(drop=True)


_AVAILABLE_INDICATORS: dict = {
    "inflation": _read_inflation,
    "insufficient_food": _read_insufficient_food,
}


@dataclass
class WFPData(ImportData):
    """Class to download and read WFP inflation and insufficient food data"""

    @property
    def available_indicators(self) -> KeysView:
        """View the available indicators from WFP"""
        return _AVAILABLE_INDICATORS.keys()

    def _country_codes(self) -> dict:
        return _read_wfp_country_codes()

    def load_data(self, indicator: str | list) -> None:
        """Load an indicator into the WFPData object"""
        if isinstance(indicator, str):
            indicator = [indicator]

        for ind_ in indicator:
            try:
                self._data[ind_] = _AVAILABLE_INDICATORS[ind_](self._country_codes())
            except KeyError:
                raise ValueError(f"Indicator {ind_} not available")

    def update_data(self, reload_data: bool = True) -> None:
        """Update the data for all the indicators currently loaded"""

        if len(self._data) == 0:
            raise RuntimeError("No indicators loaded. Load indicators before updating")

        for indicator in self._data.keys():
            if indicator == "inflation":
                _ = [_get_inflation(iso) for iso in self._country_codes()]
            elif indicator == "insufficient_food":
                _ = [
                    _get_insufficient_food(code, iso)
                    for iso, code in self._country_codes().items()
                ]

        logger.info("Data correctly updated.")

        if reload_data:
            self.load_data(list(self._data.keys()))
