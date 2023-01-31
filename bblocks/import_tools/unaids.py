"""Tools to retrieve data from UNAIDS"""

import json
import os
from typing import Optional

import pandas as pd
import requests

from bblocks import config
from bblocks.config import BBPaths
from bblocks.import_tools.common import ImportData

URL: str = "https://aidsinfo.unaids.org/datasheetdatarequest"
AREA_CODES: dict = {
    "country": {"name": "world", "code": 2},
    "region": {"name": "world-continents", "code": 1},
}
AVAILABLE_INDICATORS = pd.read_json(f"{BBPaths.import_settings}/aids_indicators.json")


def get_response(
    url: str, indicator: str, category: str, area_name: str, area_code: str
) -> dict:
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
            values_dict.update(
                {"area_name": area_name, "area_id": area_id, "year": years[i]}
            )

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
        .assign(value=lambda d: pd.to_numeric(d.value, errors="coerce"))
    )


def get_category(indicator: str) -> str:
    """returns the category for an indicator"""

    return AVAILABLE_INDICATORS.loc[
        AVAILABLE_INDICATORS.indicator == indicator, "category"
    ].unique()[0]


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


def response_params(group: str, indicator: str):
    """Returns a list of parameters to be used in the response"""

    return {
        "url": URL,
        "indicator": indicator,
        "category": get_category(indicator),
        "area_name": AREA_CODES[group]["name"],
        "area_code": AREA_CODES[group]["code"],
    }


def response_to_df(grouping: str, response: dict, indicator: str) -> pd.DataFrame:
    """Convert response to dataframe"""

    dimensions = get_dimensions(response)
    years = get_years(response)

    df = parse_data_table(response, dimensions, years).pipe(clean_data, indicator)
    if grouping == "region":
        df_global = parse_global_data(response, dimensions, years).pipe(
            clean_data, indicator
        )
        return pd.concat([df, df_global])

    return df


def extract_data(indicator: str, grouping: str):
    """pipeline to extract data"""

    params = response_params(grouping, indicator)
    response = get_response(**params)
    check_response(response)
    return response_to_df(grouping, response, indicator)


def check_if_not_downloaded(indicator: str, area_grouping: str) -> bool:
    """Checks if data is already downloaded for an indicator and area grouping

    Returns:
        True if data is not downloaded, False if data is downloaded

    """
    return not os.path.exists(
        f"{BBPaths.imported_data}/aids_{area_grouping}_{indicator}.csv"
    )


def check_area_grouping(area_grouping: str) -> list:
    """Checks if area grouping is valid and returns a list of area codes"""

    if (area_grouping not in AREA_CODES.keys()) and (area_grouping != "all"):
        raise ValueError('Invalid grouping. Choose from ["country", "region", "all"]')

    if area_grouping == "all":
        return [group for group in AREA_CODES]

    return [area_grouping]


class Aids(ImportData):
    """An object to extract data from UNAIDS.

    To use, create an instance of the class.
    The load indicators using the load_indicators method. This can be done multiple times.
    To return a dataframe of all available indicators to load, use the available_indicators class attribute.
    If the data for an indicator has never been downloaded, it will be downloaded.
    If it has been downloaded, it will be loaded from disk. If update_data is set to true,
    the data will be downloaded each time an indicator is loaded.
    You can force an update by calling 'update', and all indicators will be reloaded into the object.
    You can get a dataframe by calling 'get_data' and passing the indicator name(s)
    (or None and this will return all indicators) and passing the area grouping(s) ('all' by default)
    """

    @property
    def available_indicators(self) -> pd.DataFrame:
        """Returns a dataframe of available indicators"""
        return AVAILABLE_INDICATORS

    def load_data(self, indicator: str, area_grouping: str = "all") -> ImportData:
        """Load an indicator to the object

        indicator (str): The name of the indicator to load. To see a DataFrame of available
            indicators, use the available_indicators method.
        area_grouping (str): The grouping to use. Choose from ["country", "region", "all"].

        Returns:
            The same object to allow chaining
        """

        if indicator not in list(self.available_indicators.indicator):
            raise ValueError(f"Invalid indicator: {indicator}")

        for grouping in check_area_grouping(area_grouping):
            # check if either indicator for grouping has not been downloaded
            if check_if_not_downloaded(indicator, grouping):
                df = extract_data(indicator, grouping)
                df.to_csv(
                    config.BBPaths.raw_data / f"aids_{grouping}_{indicator}.csv",
                    index=False,
                )

            # load _data from disk
            self._data[f"{indicator}_{grouping}"] = pd.read_csv(
                config.BBPaths.raw_data / f"aids_{grouping}_{indicator}.csv"
            )

        return self

    def update_data(self, reload_data: bool):
        """Update all loaded indicators saved on the disk

        When called, it will go through each loaded indicator/area grouping combination
        and update the data saved on disk.

        Returns:
            The same object to allow chaining
        """

        if len(self._data) < 1:
            raise RuntimeError("No indicators loaded")

        area_groupings = set([area.split("_")[1] for area in self._data])

        for area_grouping in area_groupings:
            indicators_ = set(
                [i_.split("_")[0] for i_ in self._data if area_grouping in i_]
            )
            for indicator in indicators_:
                df = extract_data(indicator, area_grouping)
                df.to_csv(
                    config.BBPaths.raw_data / f"aids_{area_grouping}_{indicator}.csv",
                    index=False,
                )
                if reload_data:
                    self._data[f"{indicator}_{area_grouping}"] = df

        return self

    def get_data(
        self, indicators: Optional[str | list] = None, area_grouping: str = "all"
    ) -> pd.DataFrame:
        """Get the data as a Pandas DataFrame

        Args:
            indicators: By default, all indicators are returned in a single DataFrame.
                If a list of indicators is passed, only those indicators will be returned.
                A single indicator can be passed as a string as well.
            area_grouping (str): The area grouping to use. Choose from ["country", "region", "all"].
                Default is "all".

        Returns:
            A Pandas DataFrame with the requested indicator data
        """
        if indicators is None or indicators == "all":
            indicators = set([indicator.split("_")[0] for indicator in self._data])

        area_groupings = check_area_grouping(area_grouping)

        indicators_list = set(
            [
                f"{indicator}_{area_grouping}"
                for indicator in indicators
                for area_grouping in area_groupings
            ]
        )

        return super().get_data(indicators=list(indicators_list))
