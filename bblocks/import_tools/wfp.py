import json
from dataclasses import dataclass
from typing import KeysView

import pandas as pd
import requests

from bblocks import config
from bblocks.import_tools.common import append_new_data, ImportData


def _get_country_codes() -> None:
    """Script to fetch the country codes used by WFP. Saved as a dataframe."""

    # Get the json file from WFP website
    url: str = "https://api.hungermapdata.org/covid/data"
    file = requests.get(url).content

    # WFP codes
    wfp = json.loads(file)["countries"]

    # Create a dictionary with the iso_code: country code
    codes = {d["iso3"]: d["adm0_code"] for d in wfp}

    # Create a dataframe with the country codes and save
    df = pd.DataFrame(list(codes.items()), columns=["iso_code", "wfp_code"])
    df.to_csv(
        config.PATHS.imported_data + r"/wfp_raw/wfp_country_codes.csv", index=False
    )

    print("WFP country codes successfully downloaded.")


def _read_wfp_country_codes() -> dict:
    """Returns a dictionary with the country codes used by WFP."""

    d = pd.read_csv(config.PATHS.imported_data + r"/wfp_raw/wfp_country_codes.csv")
    return dict(zip(d["iso_code"], d["wfp_code"].astype(int)))


def _get_inflation(country_iso: str) -> None:
    """Get inflation data from VAM for a single country based on iso code"""
    url = f"https://api.vam.wfp.org/dataviz/api/GetCsv?idx=71,116&iso3={country_iso}"

    try:
        df = pd.read_csv(
            url,
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

    df.to_csv(
        f"{config.PATHS.imported_data}/wfp_raw/{country_iso}_inflation.csv", index=False
    )


def _get_insufficient_food(code: int, iso: str) -> None:
    """Get food consumption data from WFP"""

    # API URL
    url = (
        "https://5763353767114258.eu-central-1.fc.aliyuncs.com/2016-08-15/"
        f"proxy/wfp-data-api.36/map-data/adm0/{code}/countryData.json"
    )

    # Get the json file from WFP website. If empty return None
    r = requests.get(url)

    # Check if response is invalid return None
    if r.status_code == 404:
        return None

    # Create dataframe
    data = pd.DataFrame(r.json()["fcsGraph"])

    # Code may be valid but data may be empty. If so, return None
    if len(data) == 0:
        return None

    # If data is valid, clean it and save to munged
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
        .pipe(append_new_data, rf"{iso}_insufficient_food.csv", "date")
    )

    data.to_csv(
        config.PATHS.imported_data + rf"/wfp_raw/{iso}_insufficient_food.csv",
        index=False,
    )


def _read_files(iso_code: str, file_name: str) -> pd.DataFrame:
    try:
        return pd.read_csv(
            config.PATHS.imported_data + rf"/wfp_raw/{iso_code}_{file_name}.csv",
            parse_dates=["date"],
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
        print("No insufficient food data available. Run update to download data")
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

_CODES: dict = _read_wfp_country_codes()


@dataclass
class WFPData(ImportData):
    """Class to download and read WFP inflation and insufficient food data"""

    @property
    def available_indicators(self) -> KeysView:
        """View the available indicators from WFP"""
        return _AVAILABLE_INDICATORS.keys()

    def load_indicator(self, indicator: str, **kwargs) -> None:
        """Load an indicator into the WFPData object"""
        self.indicators[indicator] = _AVAILABLE_INDICATORS[indicator](_CODES)

    def update(self, **kwargs) -> None:
        """Update the data for all the indicators currently loaded"""

        for indicator in self.indicators:
            if indicator == "inflation":
                _ = [_get_inflation(iso) for iso in _CODES]
            elif indicator == "insufficient_food":
                _ = [_get_insufficient_food(code, iso) for iso, code in _CODES.items()]

    def get_data(
        self,
        indicators: str | list = "all",
    ) -> pd.DataFrame:
        """
        Get the data for the given indicators as a Pandas DataFrame

        Args:
            indicators: 'all', or one or more indicators.

        Returns:
            pd.DataFrame: A dataframe (long) with the selected indicators
        """

        df = pd.DataFrame()

        if indicators != "all" and isinstance(indicators, str):
            indicators = [indicators]

        if isinstance(indicators, list):
            indicators = [
                self.indicators[_] for _ in indicators if _ in list(self.indicators)
            ]

        elif indicators == "all":
            indicators = self.indicators.values()

        for indicator in indicators:
            df = pd.concat([df, indicator], ignore_index=True)

        return df
