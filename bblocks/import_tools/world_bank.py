from __future__ import annotations

import os
import warnings
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd
import wbgapi as wb

from bblocks.import_tools.common import ImportData


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
                "index": "iso_code",
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
    If `update_data` is set to True when creating the object, the data will be updated
    from the World Bank for each indicator.
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
        if not os.path.exists(f"{self.data_path}/{file_name}") or self.update_data:
            _get_wb_data(**__params).to_csv(
                f"{self.data_path}/{file_name}", index=False
            )

        _ = pd.read_csv(f"{self.data_path}/{file_name}", parse_dates=["date"])

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
        if len(self.indicators) == 0:
            raise RuntimeError("No indicators loaded")

        for indicator_code, (_, args) in self.indicators.items():
            file_name = args.pop("file_name")
            _get_wb_data(**args).to_csv(f"{self.data_path}/{file_name}", index=False)

        return self

    def get_data(self, indicators: Optional[str | list] = "all") -> pd.DataFrame:
        """
        Get the data as a Pandas DataFrame
        Args:
            indicators: By default, all indicators are returned in a single DataFrame.
            If a list of indicators is passed, only those indicators will be returned.
            A single indicator can be passed as a string as well.

        Returns:
            A Pandas DataFrame with the data for the indicators requested.

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
            df = pd.concat([df, indicator[0]], ignore_index=True)

        self.data = df
        return self.data


def clean_prices(df: pd.DataFrame) -> pd.DataFrame:
    """clean, format, standardize pink sheet data"""

    df.columns = df.iloc[3]
    unit_dict = (df.iloc[4]
                 .str.replace('(', '', regex=True)
                 .str.replace(')', '', regex=True)
                 .dropna()
                 .to_dict()
                 )

    df = (
        df.rename(columns={np.nan: "period"})
            .iloc[6:]
            .replace("..", np.nan)
            .reset_index(drop=True)
            .melt(id_vars="period", var_name="indicator", value_name="value")
            .assign(units=lambda d: d.indicator.map(unit_dict),
                    period=lambda d: pd.to_datetime(d.period, format="%YM%m"),
                    indicator=lambda d: d.indicator.str.replace('*', '', regex=True).str.strip(),
                    value=lambda d: pd.to_numeric(d.value, errors='coerce'))
    )

    return df


def clean_index(df: pd.DataFrame):
    """  
    """
    df.columns = [
        "period",
        "Energy",
        "Non-energy",
        "Agriculture",
        "Beverages",
        "Food",
        "Oils & Meals",
        "Grains",
        "Other Food",
        "Raw Materials",
        "Timber",
        "Other Raw Mat.",
        "Fertilizers",
        "Metals & Minerals",
        "Base Metals (ex. iron ore)",
        "Precious Metals"]

    return (df.iloc[9:]
            .replace("..", np.nan)
            .reset_index(drop=True)
            .assign(period= lambda d: pd.to_datetime(d.period, format="%YM%m"))
            .melt(id_vars="period", var_name="indicator", value_name="value")
            .assign(units="index",
                    value=lambda d: pd.to_numeric(d.value, errors='coerce')))


@dataclass
class PinkSheet(ImportData):
    """An object to help download data from World Bank Pink sheets.

    In order to use, create an instance of this class, specifying the sheet name - 'Monthly Prices', 'Monthly Indices'.
    Then, call the load_indicator method to load an indicator, optionally specifying in indicator or
    list of indicators. This can be done multiple times. If the data has never been downloaded,
    it will be downloaded. If it has been downloaded, it will be loaded from disk.
    If `update_data` is set to True when creating the object, the full dataset will be downloaded to disk
    when `load_indicator` is called for the first time.
    You can force an update by calling `update` if you want to refresh the data stored on disk.
    You can get a dataframe of the data by calling `get_data`

    """

    sheet: str = None

    def __post_init__(self):
        if self.sheet not in ["Monthly Indices", "Monthly Prices"]:
            raise ValueError(
                "Invalid sheet name. "
                "Please specify 'Monthly Indices' or 'Monthly Prices'"
            )

    def load_indicator(self, indicator: Optional[str | list] = "all") -> ImportData:
        """Load data for an indicator or list of indicators.

        Args:
            indicator: indicator to load, either. The default
                is 'all' which loads both.

        Returns:
            The same object to allow chaining
        """

        if not os.path.exists(f"{self.data_path}/{self.file_name}") or (
                self.update_data and self.data is None
        ):
            self.update(reload_data=False)

        if self.data is None:
            self.data = pd.read_csv(f"{self.data_path}/{self.file_name}")

        if indicator == "all":
            indicator = list(self.data.indicator.unique())
        elif isinstance(indicator, str):
            indicator = [indicator]

        for i in indicator:
            if i not in self.data.indicator.unique():
                raise Warning(f"Indicator not found: {i}")
            else:
                self.indicators[i] = self.data[self.data.indicator == i].reset_index(
                    drop=True
                )

        return self

    def update(self, reload_data=True) -> ImportData:
        """Update the data saved on disk

        When called it downloads Pink sheet Data from the World Bank and saves it to disk.
        Optionally specify whether to reload the data to the object

        Args:
            reload_data: If True, the data will be reloaded to the object

        Returns:
            The same object to allow chaining
        """

        url = (
            "https://thedocs.worldbank.org/en/doc/5d903e848db1d1b83e0ec8f744e55570-0350012021/related/CMO"
            "-Historical-Data-Monthly.xlsx"
        )

        (
            pd.read_excel(url, sheet_name=self.sheet)
                .pipe(_clean_pink_sheet, sheet=self.sheet)
                .to_csv(f"{self.data_path}/{self.file_name}", index=False)
        )

        if reload_data:
            self.load_indicator(list(self.indicators))

        return self

    def get_data(
            self,
            indicators: Optional[str | list] = None,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get the data as a Pandas DataFrame

        Args:
            indicators: indicator to get. If 'all', both indicators will be returned
            start_date: start date of the data (e.g. '2019-01-01')
            end_date: end date of the data (e.g. '2019-12-31')

        Returns:
            Pandas DataFrame of the data
        """

        if (start_date is not None) & (end_date is not None):
            if start_date > end_date:
                raise ValueError("start date cannot be earlier than end date")

        df = pd.DataFrame()

        if indicators is not None:
            if isinstance(indicators, str):
                indicators = [indicators]
            for indicator in indicators:
                if indicator not in self.indicators:
                    warnings.warn(f"Indicator not found: {indicator}")
                else:
                    df = pd.concat([df, self.indicators[indicator]])

        else:
            df = self.data

        if start_date is not None:
            df = df[df["period"] >= start_date]
        if end_date is not None:
            df = df[df["period"] <= end_date]

        if len(df) == 0:
            raise ValueError("No data available for current parameters")

        return df.reset_index(drop=True)

    @property
    def file_name(self) -> str:
        """Returns the name of the stored file"""
        return f"World Bank Pink Sheet - {self.sheet}.csv"
