"""Tools to retrieve data from UNAIDS"""

import pandas as pd
import numpy as np
import requests
from dataclasses import dataclass
import json
from bblocks.import_tools.common import ImportData
import os
from bblocks.config import PATHS
from typing import Optional
import warnings

URL: str = "https://aidsinfo.unaids.org/datasheetdatarequest"
AREA_CODES: dict = {
    "country": {"name": "world", "code": 2},
    "region": {"name": "world-continents", "code": 1},
}
AVAILABLE_INDICATORS = pd.read_json(f"{PATHS.import_tools}/aids_indicators.json")


def get_response(url: str, indicator: str, category: str, area_name: str, area_code: str) -> dict:
    """returns a json response from UNAIDS"""

    request_data = {
        "reqObj[Group_Name]": category,
        "reqObj[Display_Name]": indicator,
        "reqObj[TabStatus]": area_name,
        "reqObj[Area_Level]": area_code,
    }

    try:
        response = requests.post(url, data=request_data)

        return json.loads(response.content)

    except ConnectionError:
        raise ConnectionError(f"Could not extract data for indicator: {indicator}")


def parse_data_table(response: dict, dimensions: list, years: list) -> pd.DataFrame:
    """parses data table in json response and returns a formatted dataframe"""

    records = []

    for row in response["tableData"]:
        area_name = row["Area_Name"]  # collect area name
        area_id = row["Area_ID"]  # collect area id

        for i, values in enumerate(row["Data_Val"]):
            values_dict = {d: v for d, v in zip(dimensions, values[0])}
            values_dict.update({"area_name": area_name, "area_id": area_id, "year": years[i]})

            records.append(values_dict)

    return pd.DataFrame.from_records(records)


def parse_global_data(response: dict, dimensions: list, years: list) -> pd.DataFrame:
    """parses global data in json response and returns a formatted dataframe"""

    global_data = [i[0] for i in response["Global_Numbers"][0]["Data_Val"]]
    return pd.DataFrame.from_records(global_data, columns=dimensions).assign(
        area_name="Global", area_id="03M49WLD", year=years
    )


def clean_data(df: pd.DataFrame, indicator: str) -> pd.DataFrame:
    """cleans dataframe and returns a formatted dataframe"""

    return (
        df.assign(indicator=indicator)
            .dropna(how="all", axis=1)
            .melt(
            id_vars=["area_name", "area_id", "year", "indicator"],
            var_name="dimension",
            value_name="value",
        )
            .assign(value=lambda d: pd.to_numeric(d.value, errors='coerce'))
    )


def get_category(indicator: str) -> str:
    """returns the category for an indicator"""

    return AVAILABLE_INDICATORS.loc[AVAILABLE_INDICATORS.indicator == indicator, "category"].unique()[0]


def check_response(response: dict) -> None:
    """checks if response has data"""
    if len(response["tableData"]) == 0:
        raise ValueError("No data available for this indicator")


def get_dimensions(response: dict) -> list:
    """returns the dimensions for an indicator"""
    return response["MultiSubgroups"][0]


def get_years(response: dict) -> list:
    """returns the years for an indicator"""
    return response["tableYear"]


def extract_data(indicator: str, grouping: str) -> pd.DataFrame:
    """Extract and clean aids data"""

    group_name = AREA_CODES[grouping]["name"]
    group_code = AREA_CODES[grouping]["code"]
    category = get_category(indicator)

    response = get_response(
        URL, indicator, category, group_name, group_code
    )

    dimensions = get_dimensions(response)
    years = get_years(response)

    df = parse_data_table(response, dimensions, years)
    if grouping == "region":
        df = pd.concat([df, parse_global_data(response, dimensions, years)])

    return clean_data(df, indicator)


def check_if_not_downloaded(indicator: str, area_grouping: str) -> bool:
    """Checks if data is already downloaded for an indicator and area grouping

    Returns:
        True if data is not downloaded, False if data is downloaded

    """
    if os.path.exists(f"{PATHS.imported_data}/aids_{area_grouping}_{indicator}"):
        return False
    else:
        return True


def check_area_grouping(area_grouping: str) -> list:
    """Checks if area grouping is valid and returns a list of area codes"""

    if (area_grouping not in AREA_CODES.keys()) and (area_grouping != 'all'):
        raise ValueError('Invalid grouping. Choose from ["country", "region", "all"]')

    if area_grouping == 'all':
        return [group for group in AREA_CODES]
    else:
        return [area_grouping]


def concat_dataframes(indicator_dict: dict[str: pd.DataFrame], indicators: list, groupings: list) -> pd.DataFrame:
    """concatenate dataframes stored in a dictionary"""

    df = pd.DataFrame()

    for grouping in groupings:
        for indicator in indicators:
            if indicator not in indicator_dict[grouping].keys():
                warnings.warn(f"No {grouping} data available for indicator: {indicator}")
            else:
                df = pd.concat([df, indicator_dict[grouping][indicator]])
    return df


class Aids(ImportData):
    """An object to extract data from UNAIDS.

    To use, create an instance of the class.
    The load indicators using the load_indicators method. This can be done multiple times.
    To return a dataframe of all available indicators to load, use the available_indicators method.
    If the data for an indicator has never been downloaded, it will be downloaded.
    If it has been downloaded, it will be loaded from disk. If update_data is set to true,
    the data will be downloaded each time an indicator is loaded.
    You can force an update by calling 'update', and all indicators will be reloaded into the object.
    You can get a dataframe by calling 'get_data' and passing the indicator name.
    """

    available_indicators: pd.DataFrame = AVAILABLE_INDICATORS

    def __init__(self):

        self.indicators = {'country': {}, 'region': {}}

    def load_indicator(self, indicator: str, area_grouping: str = 'all') -> ImportData:
        """Load an indicator to the object

        Args:
            indicator (str): The name of the indicator to load. To see a DataFrame of available indicators,
             use the available_indicators method.
            area_grouping (str): The grouping to use. Choose from ["country", "region"].

        Returns:
            The same object to allow chaining
        """

        if indicator not in list(self.available_indicators.indicator):
            raise ValueError("Invalid indicator")

        for grouping in check_area_grouping(area_grouping):

            # check if either indicator for grouping has not been downloaded or if update_data is True
            if check_if_not_downloaded(indicator, grouping) or self.update_data:

                df = extract_data(indicator, grouping)
                df.to_csv(
                    f"{PATHS.imported_data}/aids_{grouping}_{indicator}.csv",
                    index=False,
                )

            (self.indicators[grouping]
                .update({indicator: pd.read_csv(f"{PATHS.imported_data}/aids_{grouping}_{indicator}.csv")})
             )

        return self

    def update(self, reload_data: bool = True):
        """Update all loaded indicators saved on the disk

        When called, it will go through each loaded indicator
        and update the data saved on disk.

        Args:
            reload_data (bool): If True, the updated data will be reloaded
             into the object.

        Returns:
            The same object to allow chaining
        """

        if len(self.indicators) == 0:
            raise RuntimeError("No indicators loaded")

        for indicator in self.indicators:
            for area_grouping in self.indicators[indicator]:
                df = extract_data(indicator, area_grouping)
                df.to_csv(
                    f"{PATHS.imported_data}/aids_{area_grouping}_{indicator}.csv",
                    index=False,
                )
                if reload_data:
                    self.indicators[area_grouping][indicator] = df

        return self

    def get_data(self, indicators: Optional[str | list] = None, area_grouping: str = 'all') -> pd.DataFrame:
        """Get the data as a Pandas DataFrame

        Args:
            indicators:  By default, all indicators are returned in a single DataFrame.
                If a list of indicators is passed, only those indicators will be returned.
                A single indicator can be passed as a string as well.
            area_grouping (str): The area grouping to use. Choose from ["country", "region", "all"]. Default is "all".

        Returns:
            A Pandas DataFrame with the requested indicator data
        """

        if len(self.indicators) == 0:
            raise RuntimeError("No indicators loaded. Use the load_indicators method to load indicators "
                               "into the object. To see a DataFrame of available indicators to load, call the class "
                               "attribute available_indicators")

        if isinstance(indicators, str):
            indicators = [indicators]

        if indicators is None:

            indicators = list(set(list(self.indicators['country'].keys())
                                  + list(self.indicators['region'].keys())))

        groupings = check_area_grouping(area_grouping)
        return concat_dataframes(self.indicators, indicators, groupings)


if __name__ == "__main__":
    i = AVAILABLE_INDICATORS.indicator[0]
    aids = Aids()
    aids.load_indicator(i)
    data = aids.get_data()
