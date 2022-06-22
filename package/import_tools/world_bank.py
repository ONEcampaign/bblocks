from __future__ import annotations

import os
from typing import Optional

import pandas as pd
import wbgapi as wb

from package.config import PATHS
from package.import_tools.common import ImportData


def _get_wb_data(
    series: str,
    series_name: str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    most_recent_only: bool = False,
) -> pd.DataFrame:
    """Get data for an indicator, using wbgapi"""

    if (start_year is not None) is not (end_year is not None):
        raise ValueError("start_year and end_year must both be provided")

    time_period = (
        range(start_year, end_year + 1)
        if all([isinstance(start_year, int), isinstance(end_year, int)])
        else "all"
    )

    # get data
    return (
        wb.data.DataFrame(
            series=series,
            time=time_period,
            mrnev=1 if most_recent_only else None,
            numericTimeKeys=True,
            labels=False,
            columns="series",
            timeColumns=True,
        )
        .reset_index()
        .rename(
            columns={
                "economy": "iso_code",
                series: "value",
                "time": "date",
                f"{series}:T": "date",
            }
        )
        .assign(
            indicator=series_name if series_name is not None else series,
            indicator_code=series,
            date=lambda d: pd.to_datetime(d.date, format="%Y"),
        )
        .sort_values(by=["iso_code", "date"])
        .reset_index(drop=True)
        .filter(["date", "iso_code", "indicator", "indicator_code", "value"], axis=1)
    )


class WorldBankData(ImportData):
    """An object to help download data from the World Bank.
    In order to use, create an instance of this class.
    Then, call the load_indicator method to load an indicator. This can be done multiple times.
    If the data for an indicator has never been downloaded, it will be downloaded.
    If it has been downloaded, it will be loaded from disk.
    You can force an update by calling `update` if you want to refresh the data stored on disk.
    You can get a dataframe of the data by calling `get_data`."""

    def load_indicator(
        self,
        indicator_code: str,
        indicator_name=None,
        start_year: Optional | int = None,
        end_year: Optional | int = None,
        most_recent_only: bool = False,
    ) -> WorldBankData:
        """Get an indicator from the World Bank API

        Args:
            indicator_code: the code from the World Bank data portal (e.g. "SP.POP.TOTL")
            indicator_name: A human-readable name for the indicator (e.g. "Total Population")
            start_year: The first year to include in the data
            end_year: The last year to include in the data
            most_recent_only: If True, only get the most recent non-empty value for each country

        Returns:
            The same object to allow chaining

        """
        years_str = (
            f"{start_year}-{end_year}"
            if all([isinstance(start_year, int), isinstance(end_year, int)])
            else "all"
        )
        file_name = (
            f"{indicator_code}_{years_str}_"
            f"{'most_recent' if most_recent_only else ''}.csv"
        )

        __params = {
            "series": indicator_code,
            "series_name": indicator_name,
            "start_year": start_year,
            "end_year": end_year,
            "most_recent_only": most_recent_only,
        }

        # get the indicator data if it's not saved on disk.
        if not os.path.exists(f"{PATHS.imported_data}/{file_name}"):
            _get_wb_data(**__params).to_csv(
                f"{PATHS.imported_data}/{file_name}", index=False
            )

        _ = pd.read_csv(f"{PATHS.imported_data}/{file_name}")

        __params["file_name"] = file_name

        self.indicators[indicator_code] = _, __params

        return self

    def update(self) -> ImportData:
        """Update the data saved on disk for the different indicators

        When called, it will go through each indicator and update the data saved
        based on the parameters passed to load_indicator.

        Returns:
            The same object to allow chaining

        """
        if self.indicators is None:
            raise RuntimeError("No indicators loaded")

        for indicator_code, (_, args) in self.indicators.items():
            file_name = args.pop("file_name")
            _get_wb_data(**args).to_csv(
                f"{PATHS.imported_data}/{file_name}", index=False
            )

        return self

    def get_data(self, indicators: Optional | str | list = "all") -> pd.DataFrame:
        """
        Get the data as a Pandas DataFrame
        Args:
            indicators: By default, all indicators are returned in a single DataFrame.
                If a list of indicators is passed, only those indicators will be returned.
                A single indicator can be passed as a string as well.

        Returns:
            A Pandas DataFrame with the data for the indicators requested.

        """
        if self.data is not None:
            return self.data

        df = pd.DataFrame()

        if isinstance(indicators, list):
            indicators = {
                values for k, values in self.indicators.items() if k in indicators
            }
        elif indicators == "all":
            indicators = self.indicators.values()
        elif isinstance(indicators, str):
            indicators = {indicators: self.indicators[indicators]}

        for indicator in indicators:
            df = pd.concat([df, indicator[0]], ignore_index=True)

        self.data = df
        return self.data
