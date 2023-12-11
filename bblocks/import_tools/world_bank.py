from __future__ import annotations

from dataclasses import field, dataclass

import numpy as np
import pandas as pd
import wbgapi as wb

from bblocks.cleaning_tools.clean import convert_to_datetime
from bblocks.config import BBPaths
from bblocks.import_tools.common import ImportData

PINK_SHEET_URL = (
    "https://thedocs.worldbank.org/en/doc/5d903e848db1d1b83e0ec8f744e55570-0350012021/"
    "related/CMO-Historical-Data-Monthly.xlsx"
)


def _get_wb_data(
    indicator: str,
    db: int,
    start_year: int | None = None,
    end_year: int | None = None,
    most_recent_only: bool = False,
) -> pd.DataFrame:
    """Get _data for an indicator, using wbgapi"""

    if (start_year is None) ^ (end_year is None):
        raise ValueError("start_year and end_year must both be provided")

    time_period = (
        range(start_year, end_year + 1)
        if all([isinstance(start_year, int), isinstance(end_year, int)])
        else "all"
    )

    # get _data
    return (
        wb.data.DataFrame(
            series=indicator,
            time=time_period,
            mrnev=1 if most_recent_only else None,
            numericTimeKeys=True,
            labels=False,
            columns="series",
            timeColumns=True,
            db=db,
        )
        .reset_index()
        .rename(
            columns={
                "economy": "iso_code",
                "index": "iso_code",
                indicator: "value",
                "time": "date",
                f"{indicator}:T": "date",
            }
        )
        .assign(indicator_code=indicator, date=lambda d: convert_to_datetime(d.date))
        .sort_values(by=["iso_code", "date"])
        .reset_index(drop=True)
        .filter(["date", "iso_code", "indicator", "indicator_code", "value"], axis=1)
    )


@dataclass(repr=False)
class WorldBankData(ImportData):
    """An object to help download data from the World Bank.
    In order to use, create an instance of this class.
    Then, call the load_indicator method to load an indicator. This can be done multiple times.
    If the _data for an indicator has never been downloaded, it will be downloaded.
    If it has been downloaded, it will be loaded from disk.
    If `update_data` is set to True when creating the object, the _data will be updated
    from the World Bank for each indicator.
    You can force an update by calling `update` if you want to refresh the _data stored on disk.
    You can get a dataframe of the _data by calling `get_data`."""

    _indicators: dict[str, tuple[pd.DataFrame, dict]] = field(default_factory=dict)

    def load_data(
        self,
        indicator: str | list[str],
        start_year: int | None = None,
        end_year: int | None = None,
        most_recent_only: bool = False,
        db: int = 2,  # by default use WDI database
        **kwargs,
    ) -> WorldBankData:
        """Get an indicator from the World Bank API

        Args:
            indicator: the code from the World Bank data portal (e.g. "SP.POP.TOTL")
            start_year: The first year to include in the data
            end_year: The last year to include in the data
            most_recent_only: If True, only get the most recent non-empty value for each country
            db: The database to use. By default, use the WDI database (2)

        Returns:
            The same object to allow chaining
        """

        def _load_indicator(ind_: str) -> None:
            years_str = (
                f"{start_year}-{end_year}"
                if all([isinstance(start_year, int), isinstance(end_year, int)])
                else "all"
            )
            file_name = (
                f"{ind_}_{years_str}_"
                f"{'most_recent' if most_recent_only else ''}.csv"
            )

            _params = {
                "indicator": ind_,
                "start_year": start_year,
                "end_year": end_year,
                "most_recent_only": most_recent_only,
                "db": db,
            }

            # get the indicator _data if it's not saved on disk.
            path = BBPaths.raw_data / f"{file_name}"
            if not path.exists():
                _get_wb_data(**_params).to_csv(path, index=False)

            _data = pd.read_csv(path, parse_dates=["date"])

            _params["file_name"] = file_name

            self._indicators[ind_] = _data, _params

        if isinstance(indicator, str):
            indicator = [indicator]

        # load the indicator(s) data
        [_load_indicator(ind) for ind in indicator]

        return self

    def update_data(self, reload_data: bool = True) -> ImportData:
        """Update the _data saved on disk for the different indicators

        When called, it will go through each indicator and update the _data saved
        based on the parameters passed to load_indicator.

        Returns:
            The same object to allow chaining

        """
        if len(self._indicators) == 0:
            raise RuntimeError("No indicators loaded")

        for _, (_, args) in self._indicators.items():
            file_name = args.pop("file_name")
            _get_wb_data(**args).to_csv(BBPaths.raw_data / f"{file_name}", index=False)

            if reload_data:
                self.load_data(**args)

        return self

    def get_data(self, indicators: str | list = "all", **kwargs) -> pd.DataFrame:
        for _c, _d in self._indicators.items():
            self._data[_c] = _d[0]

        return super().get_data(indicators=indicators)


def clean_prices(df: pd.DataFrame) -> pd.DataFrame:
    """Clean Pink Sheet price _data"""

    df.columns = df.iloc[3]
    unit_dict = (
        df.iloc[4]
        .str.replace("(", "", regex=False)
        .str.replace(")", "", regex=False)
        .dropna()
        .to_dict()
    )

    df = (
        df.rename(columns={np.nan: "period"})
        .iloc[6:]
        .replace("..", np.nan)
        .reset_index(drop=True)
        .melt(id_vars="period", var_name="indicator", value_name="value")
    )

    df = df.assign(
        units=lambda d: d.indicator.map(unit_dict),
        period=lambda d: pd.to_datetime(d.period, format="%YM%m"),
    )

    df = df.assign(
        indicator=lambda d: d.indicator.str.replace("*", "", regex=False).str.strip(),
        value=lambda d: pd.to_numeric(d.value, errors="coerce"),
    )

    return df


def clean_index(df: pd.DataFrame) -> pd.DataFrame:
    """Clean Pink Sheet Index index _data"""

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
        "Precious Metals",
    ]

    return (
        df.iloc[9:]
        .replace("..", np.nan)
        .reset_index(drop=True)
        .assign(period=lambda d: pd.to_datetime(d.period, format="%YM%m"))
        .melt(id_vars="period", var_name="indicator", value_name="value")
        .assign(units="index", value=lambda d: pd.to_numeric(d.value, errors="coerce"))
    )


def read_pink_sheet(indicator: str) -> pd.DataFrame:
    """Extracts and cleans _data from the pink sheet excel file

    Args:
        indicator: the indicator to extract from the pink sheet. Either "prices" or "index"

    Returns:
        A clean pandas DataFrame with the _data

    """

    if indicator == "prices":
        df = pd.read_excel(PINK_SHEET_URL, sheet_name="Monthly Prices")
        return clean_prices(df)
    elif indicator == "indices":
        df = pd.read_excel(PINK_SHEET_URL, sheet_name="Monthly Indices")
        return clean_index(df)
    else:
        raise ValueError("Invalid indicator. Choose from 'prices' or 'indices'")


class PinkSheet(ImportData):
    """An object to help download _data from World Bank Pink sheets.

    In order to use, create an instance of this class, specifying the sheet name -
    'Monthly Prices', 'Monthly Indices'.
    Then, call the load_indicator method to load an indicator,
    optionally specifying in indicator or
    list of indicators. This can be done multiple times. If the _data has never
    been downloaded,
    it will be downloaded. If it has been downloaded, it will be
    loaded from disk.
    If `update_data` is set to True when creating the object, the full dataset
    will be downloaded to disk
    when `load_indicator` is called for the first time.
    You can force an update by calling `update` if you want to refresh the
    data stored on disk.
    You can get a dataframe of the data by calling `get_data`

    """

    def load_data(self, indicator: str = "prices") -> ImportData:
        """Load data for an indicator or list of indicators.
         Args:
            indicator: The indicator to load. Choose from 'prices' or 'indices'. Default is 'prices'

        Returns:
            The same object to allow chaining
        """

        file_path = BBPaths.raw_data / f"pink_sheet_{indicator}.csv"
        if not file_path.exists():
            df = read_pink_sheet(indicator)
            df.to_csv(file_path, index=False)

        self._data[indicator] = pd.read_csv(file_path)

        return self

    def update_data(self, reload_data: bool = True) -> ImportData:
        """Update the _data saved on disk

        When called it downloads Pink sheet Data from the World Bank and saves it to disk.
        Optionally specify whether to reload the _data to the object

        Returns:
            The same object to allow chaining
        """

        for indicator in self._data:
            file_path = BBPaths.raw_data / f"pink_sheet_{indicator}.csv"
            df = read_pink_sheet(indicator)
            df.to_csv(file_path, index=False)

            if reload_data:
                self._data[indicator] = df

        return self
