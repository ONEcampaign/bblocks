"""Module to get data from WFP

The World Food Programme (WFP) provides data on inflation and food security.
Inflation data is available for various countries and indicators and can be accessed through the [VAM data portal](https://dataviz.vam.wfp.org/economic/inflation).
Food Security data is available at the national and subnational levels and can be accessed through the [Hunger Map tool](https://hungermap.wfp.org/).

This module provides 2 importers to get the data from WFP:
1. WFPInflation: To get inflation data
2. WFPFoodSecurity: To get food security data

Simple usage:

1. Inflation data:
Instantiate the object and call the `get_data()` method to get the data for a specific indicator and country
>>> wfp = WFPInflation()
>>> data = wfp.get_data(indicators = "Headline inflation (YoY)", countries = ["KEN", "UGA"])

2. Food Security data:
Instantiat the object and call the `get_data()` method to get the data for a specific country
>>> wfp = WFPFoodSecurity()
>>> data = wfp.get_data(countries = ["KEN", "UGA"])

The data is cached to avoid downloading it multiple times. To clear the cache, call the clear_cache method.
>>> wfp.clear_cache()


Developer notes:

The script requests data from WFP HungerMap and VAM internal APIs.
Both VAM and HungerMap sites robots.txt files allow scraping. However, since they do not offer
public APIs, the is scraped in a similar fashion to how a user would interact with the site. i.e.
By requesting data for a single country at a time. This is done to avoid overloading the servers.
No multithreading is used to avoid overloading the servers and to prevent unexpected behaviour and errors.

WFP uses custom country IDs to identify countries in their APIs. Different internal APIs exist which identify
countries using different IDs. The script uses the HungerMap API to get the country IDs and then uses these IDs
for both the HungerMap and VAM APIs. These IDs have been cross-checked at the time of development but
may change in the future and can cause unexpected behaviour.

It is possible that these internal APIs may change in the future which may cause the script to break.
If this happens, the script will need to be updated to reflect the changes in the APIs. If you encounter
any issues with the script, please raise an issue on the GitHub repository for the data_importers package.

"""

import io
import requests
import pandas as pd
import numpy as np
import country_converter as coco
from typing import Literal

from bblocks.data_importers.config import (
    logger,
    DataExtractionError,
    DataFormattingError,
    Fields,
    Units,
)
from bblocks.data_importers.utilities import (
    convert_dtypes,
    convert_countries_to_unique_list,
)
from bblocks.data_importers.data_validators import DataFrameValidator


HUNGERMAP_API: str = "https://api.hungermapdata.org/v2"
HUNGERMAP_HEADERS: dict = {"referrer": "https://hungermap.wfp.org/"}

VAM_API: str = "https://api.vam.wfp.org"
VAM_HEADERS: dict = {"referrer": "https://dataviz.vam.wfp.org/"}


INFLATION_IND_TYPE = Literal[
    "Headline inflation (YoY)", "Headline inflation (MoM)", "Food inflation"
]
FOOD_SECURITY_LEVEL_TYPE = Literal["national", "subnational"]


_cached_countries: dict | None = None  # cached countries


def extract_countries(timeout: int = 20, retries: int = 2) -> dict:
    """Load available countries to the object with timeout and retry mechanism

    This method gets the countries tracked in HungerMap which have food security data available
    It collects their iso3 codes, adm0 codes and data types. Data types can be "ACTUAL", "PREDICTED" or "MIXED

    Args:
        timeout: The time in seconds to wait for a response from the API. Defaults to 20s
        retries: The number of times to retry the request in case of a failure. Defaults to 2
    """

    global _cached_countries

    # If data is already cached, return it
    if _cached_countries is not None:
        return _cached_countries

    endpoint = f"{HUNGERMAP_API}/adm0data.json"  # endpoint to get the country IDs

    logger.info("Importing available country IDs ...")

    # try to get the data from the API with retries
    attempt = 0
    while attempt <= retries:
        try:
            response = requests.get(
                endpoint, headers=HUNGERMAP_HEADERS, timeout=timeout
            )
            response.raise_for_status()

            # parse the response and create a dictionary
            _cached_countries = {
                i["properties"]["iso3"]: {
                    Fields.entity_code: i["properties"]["adm0_id"],
                    Fields.data_type: i["properties"]["dataType"],
                    Fields.country_name: coco.convert(
                        i["properties"]["iso3"], to="name_short", not_found=np.nan
                    ),
                }
                for i in response.json()["body"]["features"]
                # if i["properties"]["dataType"] is not None
            }

            return _cached_countries

        # handle timeout errors
        except requests.exceptions.Timeout:
            if attempt < retries:
                attempt += 1
            else:
                raise DataExtractionError(
                    f"Request timed out while getting country IDs after {retries + 1} attempts"
                )

        # handle other request errors
        except requests.exceptions.RequestException as e:
            if attempt < retries:
                attempt += 1
            else:
                raise DataExtractionError(
                    f"Error getting country IDs after {retries + 1} attempts: {e}"
                )


class WFPInflation:
    """A class to import inflation data from the World Food Programme (WFP)

    The World Food Programme (WFP) [VAM data portal](https://dataviz.vam.wfp.org/economic/inflation) provides data on i
    nflation for various countries collected from Trading Economics. The data is available for different indicators
    including headline inflation (year-on-year and month-on-month) and food inflation.

    Usage:
    First instantiate an importer object:
    >>> wfp = WFPInflation()

    Optionally set the timeout for the requests in seconds. By default, it is set to 20s.:
    >>> wfp = WFPInflation(timeout = 30)

    See the available indicators:
    >>> wfp.available_indicators

    Get the data:
    >>> data = wfp.get_data(indicators = "Headline inflation (YoY)", countries = ["KEN", "UGA"])
    If no indicator is specified, data for all available indicators is returned and if no country is specified, data for all available countries is returned.
    It is advised to specify the required indicator and countries to avoid long wait times.

    To clear the cache:
    >>> wfp.clear_cache()


    Args:
        timeout: The time in seconds to wait for a response from the API. Defaults to 20s

    """

    def __init__(self, *, timeout: int = 20):

        self._timeout = timeout
        self._indicators = {
            "Headline inflation (YoY)": 116,
            "Headline inflation (MoM)": 117,
            "Food inflation": 71,
        }
        self._countries = None  # available countries
        self._data = {
            "Headline inflation (YoY)": {},
            "Headline inflation (MoM)": {},
            "Food inflation": {},
        }

    def __repr__(self) -> str:
        """String representation of the WFPInflation object"""

        loaded = [k for k, v in self._data.items() if v]  # get the loaded indicators
        return (
            f"{self.__class__.__name__}("
            f"timeout={self._timeout!r}, "
            f"imported indicators={loaded!r}"
            f")"
        )

    def load_available_countries(self):
        """Load available countries to the object"""

        self._countries = extract_countries(self._timeout, retries=2)

    def extract_data(
        self, country_code: int, indicator_code: int | list[int]
    ) -> io.BytesIO:
        """Extract the data from the source

        Queries the WFP API to get the inflation data for a specific country and indicator

        Args:
            country_code: The adm0 code of the country
            indicator_code: The indicator code. Can be a single code or a list of codes

        Returns:
            A BytesIO object with the data
        """

        if isinstance(indicator_code, int):
            indicator_code = [indicator_code]

        endpoint = f"{VAM_API}/economicExplorer/TradingEconomics/InflationExport"
        params = {
            "adm0Code": country_code,
            "economicIndicatorIds": indicator_code,
            # "endDate": "2024-10-31", # defaults to latest
            # "startDate": "2023-07-01", # defaults to all available
        }

        try:
            resp = requests.post(
                endpoint, json=params, headers=VAM_HEADERS, timeout=self._timeout
            )
            resp.raise_for_status()
            return io.BytesIO(resp.content)

        except requests.exceptions.Timeout:
            raise DataExtractionError("Request timed out while getting inflation data")

        except requests.exceptions.RequestException as e:
            raise DataExtractionError(f"Error getting inflation data: {e}")

    @staticmethod
    def format_data(
        data: io.BytesIO, indicator_name: str, iso3_code: str
    ) -> pd.DataFrame:
        """Format the data

        This method reads the data from the BytesIO object, formats it and returns a DataFrame

        Args:
            data: The BytesIO object with the data
            indicator_name: The name of the indicator
            iso3_code: The ISO3 code of the country

        Returns:
            A DataFrame with the formatted data
        """

        try:
            return (
                pd.read_csv(data)
                .drop(
                    columns=["IndicatorName", "CountryName"]
                )  # drop unnecessary columns
                .rename(
                    columns={
                        "Date": Fields.date,
                        "Value": Fields.value,
                        "SourceOfTheData": Fields.source,
                    }
                )
                .assign(
                    **{
                        Fields.indicator_name: indicator_name,
                        Fields.iso3_code: iso3_code,
                        Fields.country_name: coco.convert(
                            iso3_code, to="name_short", not_found=np.nan
                        ),
                        Fields.date: lambda d: pd.to_datetime(
                            d[Fields.date], format="%d/%m/%Y"
                        ),
                        Fields.unit: Units.percent,
                    }
                )
                .pipe(convert_dtypes)
            )
        except Exception as e:
            raise DataFormattingError(
                f"Error formatting data for country - {iso3_code}: {e}"
            )

    def load_data(self, indicator_name: str, iso3_codes: list[str]) -> None:
        """Load data to the object

        This method runs the process to extract, format and load the data to the object for a specific indicator
        and list of countries. It checks if the data is already loaded for specified countries and skips the process if it is. If the data
        is not available for a specific country, it logs a warning and sets the data to None.

        Args:
            indicator_name: The name of the indicator
            iso3_codes: A list of ISO3 codes of the countries to load the data for
        """

        # make a list of unloaded countries
        unloaded_countries = [
            c for c in iso3_codes if c not in self._data[indicator_name]
        ]

        # if all countries have been loaded skip the process
        if len(unloaded_countries) == 0:
            return None

        logger.info(f"Importing data for indicator: {indicator_name} ...")

        for iso3_code in unloaded_countries:

            # if the country is not available raise a warning set the data to None and continue
            if iso3_code not in self._countries:
                logger.warning(f"Data not found for country - {iso3_code}")
                self._data[indicator_name][iso3_code] = None
                continue

            # extract the data, format it and load it to the object
            data = self.extract_data(
                self._countries[iso3_code]["entity_code"],
                self._indicators[indicator_name],
            )
            df = self.format_data(data, indicator_name, iso3_code)

            # if the dataframe is empty log a warning, set the data to None and continue
            if df.empty:
                logger.warning(
                    f"No {indicator_name} data found for country - {iso3_code}"
                )
                self._data[indicator_name][iso3_code] = None
                continue

            self._data[indicator_name][iso3_code] = df

        logger.info(f"Data imported successfully for indicator: {indicator_name}")

    @property
    def available_indicators(self) -> list[str]:
        """Returns a list of available indicators"""

        return list(self._indicators.keys())

    def get_data(
        self,
        indicators: INFLATION_IND_TYPE | list[INFLATION_IND_TYPE] | None = None,
        countries: str | list[str] = None,
    ) -> pd.DataFrame:
        """Get inflation data

        Get a dataframe with the data for the specified inflation indicator and countries

        Args:
            indicators: The inflation indicators to get data for. Can be a single indicator or a list of indicators.
                If None, data for all available indicators is returned. By default, returns all available indicators.
                To see the available indicators use the available_indicators property
            countries: The countries (name or ISO3 code) to get data for. If None, data for all available countries is returned
                By default returns data for all available countries

        Returns:
            A DataFrame with the requested data
        """

        if indicators:
            if isinstance(indicators, str):
                indicators = [indicators]

            # check that all indicators are valid
            for indicator in indicators:
                if indicator not in self._indicators:
                    raise ValueError(
                        f"Invalid indicator - {indicator}. Please choose from {list(self._indicators.keys())}"
                    )

        # if no indicator is specified, get data for all available indicators
        else:
            indicators = list(self._indicators.keys())

        # check if country IDs are loaded, if not then load them
        if self._countries is None:
            self.load_available_countries()

        # validate countries
        if countries:
            if isinstance(countries, str):
                countries = [countries]

            # check that countries are valid, if not then drop them to make a unique list
            country = convert_countries_to_unique_list(countries, to="ISO3")

            # if the list is empty then raise an error
            if len(country) == 0:
                raise ValueError("No valid countries found")

        # if no country is specified, get data for all available countries
        else:
            country = list(self._countries.keys())

        # load the data for the requested countries and indicators if not already loaded
        for indicator in indicators:
            self.load_data(indicator_name=indicator, iso3_codes=country)

        # concatenate the dataframes for the requested countries and indicators if available
        data_list = [
            self._data[indicator][code]
            for indicator in indicators
            for code in country
            if code in self._data[indicator] and self._data[indicator][code] is not None
        ]

        # if no data is found return an empty DataFrame and log a warning
        if len(data_list) == 0:
            logger.warning("No data found for the requested countries")
            return pd.DataFrame()

        return pd.concat(data_list, ignore_index=True)

    def clear_cache(self) -> None:
        """Clear the cached data"""

        self._data = {
            "Headline inflation (YoY)": {},
            "Headline inflation (MoM)": {},
            "Food inflation": {},
        }

        # clear the cached countries
        self._countries = None
        global _cached_countries
        _cached_countries = None

        logger.info("Cache cleared")


class WFPFoodSecurity:
    """Class to import food security data from the WFP Hunger Map API

    The World Food Programme (WFP) [Hunger Map](https://hungermap.wfp.org/) is a global hunger monitoring system which provides data
    on food security and other related indicators.

    The data accessible through this object is "people with insufficient food consumption" and is available at the national and subnational levels.

    Usage:
    First instantiate an importer object:
    >>> wfp = WFPFoodSecurity()

    Get the data:
    >>> data = wfp.get_data(countries = ["KEN", "UGA"])
    This will return a pandas DataFrame with the data for the specified countries.

    You can also get the data at the subnational level:
    >>> data = wfp.get_data(countries = ["KEN", "UGA"], level = "subnational")

    To clear the cache:
    >>> wfp.clear_cache()

    To get the available countries and their details:
    >>> wfp.available_countries

    Args:
        timeout: The time in seconds to wait for a response from the API. Defaults to 20s
        retries: The number of times to retry the request in case of a failure. Defaults to 2

    Indicator definition:
    People with insufficient food consumption refer to those with poor or borderline food consumption, according to the Food Consumption Score (FCS). The Food Consumption Score (FCS) is a proxy of household's food access and a core WFP indicator used to classify households into different groups based on the adequacy of the foods consumed in the week prior to being surveyed. FCS is the most commonly used food security indicator by WFP and partners. This indicator is a composite score based on households’ dietary diversity, food frequency, and relative nutritional importance of different food groups. The FCS is calculated using the frequency of consumption of eight food groups by a household during the 7 days before the survey using standardized weights for each of the food groups reflecting its respective nutrient density, and then classifies households as having ‘poor’, ‘borderline’ or ‘acceptable’ food consumption: Poor food consumption: Typically refers to households that are not consuming staples and vegetables every day and never or very seldom consume protein-rich food such as meat and dairy (FCS of less than 21 or 28). Borderline food consumption: Typically refers to households that are consuming staples and vegetables every day, accompanied by oil and pulses a few times a week (FCS of less than 35 or 42). Acceptable food consumption: Typically refers to households that are consuming staples and vegetables every day, frequently accompanied by oil and pulses, and occasionally meat, fish and dairy (FCS greater than 42).

    """

    def __init__(self, *, timeout: int = 20, retries: int = 2):

        self._timeout = timeout
        self._retries = retries

        self._countries: None | dict = None
        self._data = {"national": {}, "subnational": {}}

    def __repr__(self) -> str:
        """String representation of the WFPFoodSecurity object"""

        # add the national and subnational as a string if the dictionary value is not empty
        loaded = [k for k, v in self._data.items() if v]  # get the loaded levels

        return (
            f"{self.__class__.__name__}("
            f"timeout={self._timeout!r}, "
            f"retries={self._retries!r}, "
            f"loaded levels={loaded!r}"
            f")"
        )

    def _load_available_countries(self) -> None:
        """Load available countries to the object
        Excludes countries for which there is no registered data. i.e. data_type is None in the response
        """

        d = extract_countries(timeout=self._timeout, retries=self._retries)
        self._countries = {k: v for k, v in d.items() if v["data_type"] is not None}

    def _extract_data(self, entity_code: int, level: FOOD_SECURITY_LEVEL_TYPE) -> dict:
        """Extract the data from the source

        Args:
            entity_code: The adm0 code of the country
            level: The level of data to extract. Can be "national" or "subnational"

        Returns:
            the json response from the API
        """

        # get the specific endpoint based on the level
        if level == "national":
            endpoint = (
                f"https://api.hungermapdata.org/v2/adm0/{entity_code}/countryData.json"
            )
        elif level == "subnational":
            endpoint = (
                f"https://api.hungermapdata.org/v2/adm0/{entity_code}/adm1data.json"
            )
        else:
            raise ValueError("level must be 'national' or 'subnational'")

        # try to get the data from the API with retries
        attempt = 0
        while attempt <= self._retries:
            try:
                response = requests.get(
                    endpoint, headers=HUNGERMAP_HEADERS, timeout=self._timeout
                )
                response.raise_for_status()
                return response.json()

            # handle timeout errors
            except requests.exceptions.Timeout:
                if attempt < self._retries:
                    attempt += 1
                else:
                    raise DataExtractionError(
                        f"Request timed out for adm0 code - {entity_code} after {self._retries + 1} attempts"
                    )

            # handle other request errors
            except requests.exceptions.RequestException as e:
                if attempt < self._retries:
                    attempt += 1
                else:
                    raise DataExtractionError(
                        f"Error extracting data for country adm0_code - {entity_code} after {self._retries + 1} attempts: {e}"
                    )

    @staticmethod
    def _parse_national_data(data: dict, iso_code: str) -> pd.DataFrame:
        """Parse the national data

        This method parses the national data and returns a DataFrame with the data
        It looks for the field "fcsGraph" from the response which contains the data to render the chart
        for the indicator "people with insufficient food consumption" over time

        Args:
            data: The json response from the API
            iso_code: The iso3 code of the country

        Returns:
            A DataFrame with the parsed data
        """

        try:

            return (
                pd.DataFrame(data["fcsGraph"])
                .rename(
                    columns={
                        "x": Fields.date,
                        "fcs": Fields.value,
                        "fcsHigh": Fields.value_upper,
                        "fcsLow": Fields.value_lower,
                    }
                )
                .assign(
                    **{
                        Fields.iso3_code: iso_code,
                        Fields.country_name: coco.convert(
                            iso_code, to="name_short", not_found=np.nan
                        ),
                        Fields.indicator_name: "people with insufficient food consumption",
                        Fields.source: "World Food Programme",
                        Fields.date: lambda d: pd.to_datetime(
                            d.date, format="%Y-%m-%d"
                        ),
                    }
                )
                .pipe(convert_dtypes)
            )

        except Exception as e:
            raise DataFormattingError(
                f"Error parsing national data for country - {iso_code}: {e}"
            )

    @staticmethod
    def _parse_subnational_data(data: dict, iso_code: str) -> pd.DataFrame:
        """Parse the subnational data

        This method parses the subnational data and returns a DataFrame with the data
        It looks for the field "fcsGraph" from the response which contains the data to render the chart for
        each region for the indicator "people with insufficient food consumption" over time. The method
        loops through the regions concatenating the data for each region into a single DataFrame

        Args:
            data: The json response from the API
            iso_code: The iso3 code of the country

        Returns:
            A DataFrame with the parsed data
        """

        try:
            return (
                pd.concat(
                    [
                        pd.DataFrame(_d["properties"]["fcsGraph"]).assign(
                            region_name=_d["properties"]["Name"]
                        )
                        for _d in data["features"]
                    ],
                    ignore_index=True,
                )
                .rename(
                    columns={
                        "x": Fields.date,
                        "fcs": Fields.value,
                        "fcsHigh": Fields.value_upper,
                        "fcsLow": Fields.value_lower,
                    }
                )
                .assign(
                    **{
                        Fields.iso3_code: iso_code,
                        Fields.country_name: coco.convert(
                            iso_code, to="name_short", not_found=np.nan
                        ),
                        Fields.indicator_name: "people with insufficient food consumption",
                        Fields.source: "World Food Programme",
                        Fields.date: lambda d: pd.to_datetime(
                            d.date, format="%Y-%m-%d"
                        ),
                    }
                )
                .pipe(convert_dtypes)
            )

        except Exception as e:
            raise DataFormattingError(
                f"Error parsing subnational data for country - {iso_code}: {e}"
            )

    def _load_data(self, iso_codes: list[str], level: FOOD_SECURITY_LEVEL_TYPE) -> None:
        """Load data to the object

        This method runs the process to extract, parse and load the data to the object for a specific level
        and list of countries. It checks if the data is already loaded for specified countries and skips the process if it is. If the data
        is not available for a specific country, it logs a warning

        Args:
            iso_codes: A list of ISO3 codes of the countries to load the data for
            level: The level of data to load. Can be "national" or "subnational"
        """

        # make a list of unloaded countries
        unloaded_countries = [c for c in iso_codes if c not in self._data[level]]

        # if all countries have been loaded skip the process
        if len(unloaded_countries) == 0:
            return None

        logger.info(f"Importing {level} data")

        for iso_code in unloaded_countries:

            # if a requested country is not available log no data found and return
            if iso_code not in self._countries:
                logger.info(f"No data found for country - {iso_code}")
                continue

            # extract, parse and load the data
            logger.info(f"Importing {level} data for country - {iso_code} ...")

            response = self._extract_data(
                self._countries[iso_code][Fields.entity_code], level=level
            )

            # parse and load the data
            if level == "national":
                df = self._parse_national_data(response, iso_code)

                # validate national data
                DataFrameValidator().validate(
                    df,
                    required_cols=[
                        Fields.iso3_code,
                        Fields.date,
                        Fields.value,
                        Fields.indicator_name,
                    ],
                )

            else:
                df = self._parse_subnational_data(response, iso_code)

                # validate data
                DataFrameValidator().validate(
                    df,
                    required_cols=[
                        Fields.iso3_code,
                        Fields.date,
                        Fields.value,
                        Fields.indicator_name,
                        Fields.region_name,
                    ],
                )

            self._data[level][iso_code] = df

        logger.info(f"{level.capitalize()} data imported successfully")

    @property
    def available_countries(self) -> pd.DataFrame:
        """Returns a DataFrame with the available countries and their details"""

        if self._countries is None:
            self._load_available_countries()

        return (
            pd.DataFrame(self._countries)
            .T.reset_index()
            .rename(columns={"index": Fields.iso3_code})
            .pipe(convert_dtypes)
        )

    def get_data(
        self,
        countries: str | list[str] | None = None,
        level: FOOD_SECURITY_LEVEL_TYPE = "national",
    ) -> pd.DataFrame:
        """Get data for "people with insufficient food consumption"

        Args:
            countries: The countries (name or ISO3 code) to get data for. If None, data for all available countries is returned
            level: The level of data to get. Can be "national" or "subnational". Defaults to "national"

        Returns:
            A DataFrame with the data
        """

        # check if country IDs are loaded, if not then load them
        if self._countries is None:
            self._load_available_countries()

        # if no country is specified, get data for all available countries
        if not countries:
            countries = list(self._countries.keys())

        else:
            # if a single country is specified, convert it to a list
            if isinstance(countries, str):
                countries = [countries]

            # convert the country names to ISO3 codes
            countries = convert_countries_to_unique_list(countries, to="ISO3")

            if len(countries) == 0:
                raise ValueError("No valid countries found")

        # load the data for the requested countries and level if not already loaded
        self._load_data(countries, level)

        # concatenate the dataframes
        data_list = [
            self._data[level][code]
            for code in countries
            if code in self._data[level] and self._data[level][code] is not None
        ]

        if len(data_list) == 0:
            logger.warning("No data found for the requested countries")
            return pd.DataFrame()

        return pd.concat(data_list, ignore_index=True)

    def clear_cache(self) -> None:
        """Clear the cache"""

        self._data = {"national": {}, "subnational": {}}

        # clear the cached countries
        self._countries = None
        global _cached_countries
        _cached_countries = None

        logger.info("Cache cleared")
