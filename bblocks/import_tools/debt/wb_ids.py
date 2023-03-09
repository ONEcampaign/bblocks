"""Import data from the World Bank's International Debt Statistics database"""
import json
from dataclasses import dataclass

import pandas as pd

from bblocks.config import BBPaths
from bblocks.import_tools.common import ImportData
from bblocks.import_tools.debt.get_data import get_indicator_data


def read_indicators(file: str = "ids") -> dict:
    """Read a json which contains the IDS indicators stored in the requested file

    Args:
        file (str): The file to read - either 'ids','debt_stocks', or 'debt_service'.
         Defaults to "ids"
    """
    with open(BBPaths.debt_settings / f"{file}_indicators.json", "r") as fp:
        return json.load(fp)


@dataclass
class DebtIDS(ImportData):
    """Import data from the World Bank's International Debt Statistics database.

    To use this object, first create an instance of it.
    Then use the `load_data` method to load indicators. One or more indicators can
    be loaded at a time, and a starting and end year must be specified.

    If the data has not been downloaded before, it will be downloaded from the
    World Bank API. If the data has been downloaded before, it will be loaded from
    the local data folder.

    To get a DataFrame, use the `get_data` method. You can get the data for one or more,
    or for all indicators at once.

    To update the data, use the `update_data` method. This will download the latest
    data from the World Bank API and overwrite the local data.

    - To get a list of available indicators, use the `get_available_indicators` method.
    - To get a list of available debt service indicators, use the
      `debt_service_indicators` method.
    - To get a list of available debt stocks indicators, use the
      `debt_stocks_indicators` method.

    """

    def __post_init__(self):
        """Set the path to the data folder and create it if it doesn't exist"""
        self._path = BBPaths.raw_data / "ids_data"

        # check the data path exists and if not create it
        if not self._path.exists():
            self._path.mkdir()

    def _check_stored_data(
        self, indicator: str, start_year: int, end_year: int
    ) -> str | bool:
        """Check if the data is already stored locally

        This also checks if the years requested are inside another file.

        Args:
            indicator (str): The indicator to check
            start_year (int): The start year of the data
            end_year (int): The end year of the data

        Returns:
            str: The filename of the data if it exists
            bool: False if the data doesn't exist

        """

        # get the filenames of all files in the path
        files = [f.name for f in self._path.iterdir() if indicator in f.name]

        for file in files:
            # get the years from the filename
            years = file.split("_")[-1].split(".")[0].split("-")

            # if the requested data is inside an available file, return file name
            if (int(years[0]) <= start_year) and (end_year <= int(years[1])):
                return file

        # if the data doesn't exist, return False
        return False

    @staticmethod
    def _indicator_parameters(indicator: str) -> tuple[str, int, int]:
        """Get the indicator, start year and end year from the indicator name."""
        indicator, years = indicator.split("_")
        start_year, end_year = years.split("-")
        return indicator, int(start_year), int(end_year)

    @classmethod
    def get_available_indicators(cls) -> dict:
        """Get a dictionary of all available indicators in the IDS database."""
        return {k: v["name"] for k, v in read_indicators().items()}

    @classmethod
    def debt_service_indicators(cls, detailed_category: bool = True) -> dict:
        """Get a dictionary of Debt Service indicators in the IDS database."""
        return {
            k: v["detailed_category" if detailed_category else "broad_category"]
            for k, v in read_indicators("debt_service").items()
        }

    @classmethod
    def debt_stocks_indicators(cls, detailed_category: bool = True) -> dict:
        """Get a dictionary of Debt Service indicators in the IDS database."""
        return {
            k: v["detailed_category" if detailed_category else "broad_category"]
            for k, v in read_indicators("debt_stocks").items()
        }

    def _get_indicator(
        self, indicator: str, start_year: int, end_year: int
    ) -> ImportData:
        """Get data for an indicator. This method is not meant to be accessed
        directly. Instead, use the `.get_data()` method.

        Args:
            indicator: The indicator to get. They must be in the IDS format
                (e.g. DT.DOD.DECT.CD). To view all available indicators, call
                `.get_available_indicators()`.

        Returns:
            The same object to allow chaining of methods
        """

        # Get a dataframe of the data
        data = get_indicator_data(
            indicator=indicator, start_year=start_year, end_year=end_year
        )

        # Rename the columns and convert the year column to a datetime
        data = data.rename(
            columns={"counterpart-area": "counterpart_area", "time": "year"}
        ).astype({"year": "datetime64[ns]"})

        # Save the data to a feather file
        data.to_feather(self._path / f"{indicator}_{start_year}-{end_year}.feather")

        return self

    def load_data(
        self, indicators: str | list, start_year: int, end_year: int
    ) -> ImportData:
        """Load the data for an indicator or a list of indicators.

        Args:
            indicators: The indicator(s) to load. They must be in the IDS format
                (e.g. DT.DOD.DECT.CD). To view all available indicators, call
                `.get_available_indicators()`.
            start_year: The first year to include in the data
            end_year: The last year to include in the data

        """

        # check if indicators is a string and convert to a list.
        if isinstance(indicators, str):
            indicators = [indicators]

        # check if the indicators are valid
        for indicator in indicators:
            if indicator not in self.get_available_indicators().keys():
                raise ValueError(f"{indicator} is not a valid indicator.")

            # check if the data is already stored locally
            stored_data = self._check_stored_data(
                indicator, start_year=start_year, end_year=end_year
            )

            # check if indicator already exists in the data dictionary
            for key in self._data:
                if indicator in key and key != f"{indicator}_{start_year}-{end_year}":
                    raise KeyError(
                        f"{indicator} has already been loaded. "
                        f"Only one version of the same indicator can be loaded at once."
                    )

            # if the data isn't stored locally, get it
            if not stored_data:
                # get the data
                self._get_indicator(indicator, start_year, end_year)

                # Once the data has been downloaded, save the filename to the
                # stored_data variable.
                stored_data = f"{indicator}_{start_year}-{end_year}.feather"

            # load the data into the data dictionary, keeping only requested years.
            self._data[f"{indicator}_{start_year}-{end_year}"] = (
                pd.read_feather(self._path / stored_data)
                .query(f"year >= {start_year} and year <= {end_year}")
                .reset_index(drop=True)
            )

        return self

    def update_data(self, reload_data: bool = True) -> ImportData:
        """Update the data for all loaded indicators."""

        # check if any data has been loaded
        if len(self._data) < 1:
            raise ValueError("No data has been loaded.")

        # Update the data for each indicator
        for indicator in self._data:
            # get the indicator name, start year and end year
            ind, start, end = self._indicator_parameters(indicator)
            # get the data
            self._get_indicator(indicator=ind, start_year=start, end_year=end)

            # reload the data if reload_data is True
            if reload_data:
                self.load_data(indicators=ind, start_year=start, end_year=end)

        return self

    def get_data(self, indicators: str | list = "all", **kwargs) -> pd.DataFrame:
        """Get the data for an indicator or a list of indicators.

        Args:
            indicators: The indicator(s) to get. They must be in the IDS format
                (e.g. DT.DOD.DECT.CD). To get all available indicators, set
                `indicators="all"`.

        Returns:
            A pandas dataframe with the requested data.

        """

        # check if indicators is a string and convert to a list.
        if isinstance(indicators, str):
            if indicators == "all":
                indicators = self._data.keys()
            else:
                indicators = [indicators]

        # check if the indicators are valid
        get_list = []
        for indicator in indicators:
            for ind in self._data:
                if indicator in ind:
                    get_list.append(ind)

        # Get the data nad return a dataframe
        return super().get_data(indicators=get_list)
