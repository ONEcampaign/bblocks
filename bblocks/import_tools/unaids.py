""" """
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


def get_response(indicator: str, category: str, area_name: str, area_code: str) -> dict:
    """returns a json response from UNAIDS"""

    url = 'https://aidsinfo.unaids.org/datasheetdatarequest'

    request_data = {'reqObj[Group_Name]': category,
                    'reqObj[Display_Name]': indicator,
                    'reqObj[TabStatus]': area_name,
                    'reqObj[Area_Level]': area_code}

    try:
        response = requests.post(url, data=request_data)

        return json.loads(response.content)

    except ConnectionError:
        raise ConnectionError('Could not extract data')


def parse_data_table(response: dict, dimensions: list, years: list) -> pd.DataFrame:
    """parses data table in json response and returns a formatted dataframe"""

    records = []
    for row in response['tableData']:
        area_name = row['Area_Name']
        area_id = row['Area_ID']
        for i in range(len(row['Data_Val'])):
            values = row['Data_Val'][i]
            values_dict = {dimensions[j]: values[0][j] for j in range(len(dimensions))}
            values_dict.update({'area_name': area_name, 'area_id': area_id, 'year': years[i]})

            records.append(values_dict)

    return pd.DataFrame.from_records(records)


def parse_global_data(response: dict, dimensions: list, years: list) -> pd.DataFrame:
    """parses global data in json response and returns a formatted dataframe"""

    global_data = [i[0] for i in response['Global_Numbers'][0]['Data_Val']]
    return (pd.DataFrame
            .from_records(global_data, columns=dimensions)
            .assign(area_name='Global',
                    area_id='03M49WLD',
                    year=years)
            )


def clean_data(df: pd.DataFrame, indicator: str) -> pd.DataFrame:
    """cleans dataframe and returns a formatted dataframe"""

    return (df.assign(indicator=indicator)
            .dropna(how='all', axis=1)
            .melt(id_vars=['area_name', 'area_id', 'year', 'indicator'],
                  var_name='dimension',
                  value_name='value')
            .replace('...', np.nan)
            .assign(value = lambda d: pd.to_numeric(d.value))
            )


def get_category(df: pd.DataFrame, indicator: str) -> str:
    """returns the category for an indicator"""

    return df.loc[df.indicator == indicator, 'category'].unique()[0]


def check_response(response: dict) -> None:
    """ """
    if len(response['tableData']) == 0:
        raise ValueError('No data available for this indicator')


def get_dimensions(response: dict) -> list:
    """ """
    return response['MultiSubgroups'][0]


def get_years(response: dict) -> list:
    """"""
    return response['tableYear']


@dataclass
class Aids(ImportData):
    """An object to extract data from UNAIDS.
    To use, create an instance of the class and set the grouping - either 'country' or 'region'
    (default is 'country'. 'region' includes global values).
    The load indicators using the load_indicators method. This can be done multiple times.
    To return a dataframe of all available indicators to load, use the available_indicators method.
    If the data for an indicator has never been downloaded, it will be downloaded.
    If it has been downloaded, it will be loaded from disk. If update_data is set to true,
    the data will be downloaded each time an indicator is loaded.
    You can force an update by calling 'update', and all indicators will be reloaded into the object.
    You can get a dataframe by calling 'get_data' and passing the indicator name.
    """

    grouping: str = 'country'

    @staticmethod
    def available_indicators() -> pd.DataFrame:
        """return a dataframe of available indicators"""
        return pd.read_json(f'{PATHS.import_tools}/aids_indicators.json')

    def __post_init__(self):

        area_codes = {'country': {'name': 'world', 'code': 2},
                      'region': {'name': 'world-continents', 'code': 1}}

        if self.grouping not in ['country', 'region']:
            raise ValueError('Invalid grouping. Choose from ["country", "region"]')
        else:
            self.__group_name = area_codes[self.grouping]['name']
            self.__group_code = area_codes[self.grouping]['code']

    def load_indicator(self, indicator: str) -> ImportData:
        """Load an indicator"""

        if indicator not in list(self.available_indicators().indicator):
            raise ValueError('Invalid indicator')
        if indicator in self.indicators:
            raise ValueError(f'{indicator} already loaded')

        if not os.path.exists(f"{PATHS.imported_data}/aids_{self.grouping}_{indicator}") or \
                (indicator not in self.indicators and self.update_data):
            df = self.__extract_data(indicator)
            self.indicators[indicator] = df
            df.to_csv(f"{PATHS.imported_data}/aids_{self.grouping}_{indicator}.csv", index=False)

        self.indicators[indicator] = pd.read_csv(f"{PATHS.imported_data}/aids_{self.grouping}_{indicator}.csv")

        return self

    def __extract_data(self, indicator) -> pd.DataFrame:
        """Extract and clean aids data"""

        category = get_category(self.available_indicators(), indicator)
        response = get_response(indicator, category, self.__group_name, self.__group_code)

        dimensions = get_dimensions(response)
        years = get_years(response)

        df = parse_data_table(response, dimensions, years)
        if self.grouping == 'region':
            df = pd.concat([df, parse_global_data(response, dimensions, years)])

        return clean_data(df, indicator)

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
            df = self.__extract_data(indicator)
            df.to_csv(f"{PATHS.imported_data}/aids_{self.grouping}_{indicator}", index=False)

            if reload_data is True:
                self.indicators[indicator] = df

        return self

    def get_data(self, indicators: Optional[str | list] = None) -> pd.DataFrame:
        """Get the data as a Pandas DataFrame

        Args:
            indicators:  By default, all indicators are returned in a single DataFrame.
            If a list of indicators is passed, only those indicators will be returned.
            A single indicator can be passed as a string as well.

        Returns:
            A Pandas DataFrame with the requested indicator data
        """

        if len(self.indicators) == 0:
            raise RuntimeError("No indicators loaded")

        if indicators is None:
            return pd.concat(self.indicators.values())

        if isinstance(indicators, list):
            df = pd.concat([self.indicators[i] for i in indicators])
            for i in indicators:
                if i not in self.available_indicators().indicator:
                    warnings.warn(f'Invalid indicator: {i}')

            return df

        if indicators not in self.indicators:
            raise ValueError(f'Indicator not loaded: {indicators}')

        return self.indicators[indicators]