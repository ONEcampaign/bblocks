"""Helper functions to generate the URL and extract the data from the
IDS database."""
import time

import pandas as pd
import requests
from pyjstat import pyjstat

from bblocks.logger import logger


def _time_period(start_year: int, end_year: int) -> str:
    """Take a period range and convert it to an API compatible string"""

    time_period = ""

    for y in range(start_year, end_year + 1):
        if y < end_year:
            time_period += f"yr{y};"
        else:
            time_period += f"yr{y}"

    return time_period


def _country_list(countries: str | list) -> str:
    """Take a country list and convert it to an API compatible string"""

    country_list = ""

    if isinstance(countries, str):
        return countries

    for c in countries:
        country_list += f"{c};"

    return country_list[:-1]


def _api_url(
    indicator: str,
    countries: str | list,
    start_year: int,
    end_year: int,
    source: int,
) -> str:
    """Query string for API for IDS data. One indicator at a time"""

    if not isinstance(indicator, str):
        raise TypeError("Must pass single indicator (as string) at a time")

    countries = _country_list(countries)
    time_period = _time_period(start_year, end_year)

    return (
        "https://api.worldbank.org/v2/"
        f"sources/{source}/country/{countries}/"
        f"series/{indicator}/time/{time_period}/"
        f"data?format=jsonstat"
    )


def _clean_ids_response(data: pd.DataFrame, indicator: str) -> pd.DataFrame:
    return (
        data.loc[data.value.notna()]
        .assign(series_code=indicator)
        .astype({"value": "float64"})
        .reset_index(drop=True)
    )


def get_indicator_data(
    indicator: str,
    countries: str | list = "all",
    start_year: int = 2017,
    end_year: int = 2025,
    source: int = 6,
    try_again: bool = True,
) -> pd.DataFrame:
    """Get data for a single indicator from the World Bank API

    Args:
        indicator: The indicator to get data for
        countries: The countries to get data for. Defaults to all countries
        start_year: The start year for the data. Defaults to 2017
        end_year: The end year for the data. Defaults to 2025
        source: The source of the data. Defaults to 6 (IDS)
        try_again: Whether to try again if the API fails. Defaults to True

    Returns:
        A dataframe with the data

    """

    # Get API url
    url = _api_url(indicator, countries, start_year, end_year, source)

    # Get data
    try:
        data = pyjstat.Dataset.read(url).write(output="dataframe")
        logger.debug(f"Got data for {indicator}")

        return data.pipe(_clean_ids_response, indicator=indicator)

    except requests.exceptions.HTTPError as http:
        logger.debug(f"Failed to get data for {indicator}: {http}")

    except requests.exceptions.JSONDecodeError as j:
        logger.debug(f"Failed to get data for {indicator}: {j}")

    except Exception as e:
        logger.debug(f"Failed to get data for {indicator}: {e}")

    if try_again:
        time.sleep(300)
        get_indicator_data(
            indicator=indicator,
            countries=countries,
            start_year=start_year,
            end_year=end_year,
            source=source,
            try_again=False,
        )
