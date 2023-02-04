import json
import pathlib

import pandas as pd
import requests
from pyjstat import pyjstat

from bblocks import config


def _time_period(start_year: int, end_year: int) -> str:
    """Take a period range and convert it to an API compatible string"""

    return ";".join([f"yr{y}" for y in range(start_year, end_year + 1)])


def _country_list(countries: str | list) -> str:
    """Take a country list amd convert it to an API compatible string"""

    if isinstance(countries, str):
        return countries

    return ";".join(countries)


def _fetch_ids_indicator(url: str) -> pd.DataFrame:
    return pyjstat.Dataset.read(url).write(output="dataframe")


def _clean_ids_response(data: pd.DataFrame, indicator: str) -> pd.DataFrame:
    return (
        data.loc[lambda d: d.value.notna()]
        .assign(series_code=indicator)
        .astype({"value": float})
        .reset_index(drop=True)
    )


def _ids_countries_url(page: int) -> str:
    """Return a URL for the World Bank IDS API to get a list of countries,
    specifying the page number"""
    return f"https://api.worldbank.org/v2/country?per_page=300&page={page}&format=json"


def _get_ids_countries_dict(response: json) -> dict:
    # If there is only one page, return a dictionary, if not, loop to get all _data
    if response[0]["pages"] == 1:
        return {c["id"]: c["name"] for c in response[1]}

    else:
        d = {}
        for page in range(1, response[0]["pages"] + 1):
            url = _ids_countries_url(page=page)
            response = requests.get(url).json()
            d.update({c["id"]: c["name"] for c in response[1]})
        return d


def download_ids_codes() -> None:
    """
    Download a dictionary of World Bank 3-letter country codes and their names.
    Save as a CSV in the debt folder.

    """
    # Countries list URL
    url = _ids_countries_url(page=1)

    # Fetch _data
    response = requests.get(url).json()

    # Get dictionary of country codes and names
    d = _get_ids_countries_dict(response=response)

    path = config.BBPaths.import_settings / "ids_codes.csv"
    # Save the dictionary to a csv file
    (
        pd.DataFrame.from_dict(d, orient="index")
        .reset_index()
        .rename(columns={"index": "code", 0: "name"})
        .to_csv(path, index=False)
    )


def ids_codes() -> dict:
    """Return a dataframe of World Bank 3-letter country codes and their names"""
    path = config.BBPaths.import_settings / "ids_codes.csv"

    if not pathlib.Path.exists(path):
        download_ids_codes()

    return pd.read_csv(path).set_index("code")["name"].to_dict()


def ids_api_url(
    indicator: str,
    countries: str | list,
    start_year: int,
    end_year: int,
    source: int,
) -> str:
    """
    Take the required parameters and return the url for the World Bank IDS API.

    Args:
        indicator: code from the World Bank IDS database (e.g. "DT.AMT.BLAT.CD")
        countries: one or more World Bank 3-letter codes. A list of codes is provided
            using the `ids_codes` function. Alternatively, "all" can be used.
        start_year: first year of the period
        end_year: last year of the period (inclusive)
        source: the database number (see World Bank IDS documentation)

    Returns:
        A string with the URL to be used to get the _data from the World Bank IDS API

    """
    if not isinstance(indicator, str):
        raise TypeError("Must pass single indicator (as string) at a time")

    countries = _country_list(countries)
    time_period = _time_period(start_year, end_year)

    return (
        "https://api.worldbank.org/v2/"
        f"sources/{source}/country/{countries}/"
        f"series/{indicator}/time/{time_period}/"
        f"_data?format=jsonstat"
    )


def get_indicator_data(
    indicator: str,
    countries: str | list = "all",
    start_year: int = 2017,
    end_year: int = 2025,
    source: int = 6,
) -> pd.DataFrame:
    """
    Query the World Bank IDS API and return the _data as a pandas DataFrame.
    The _data is requested from the World Bank as a pyjstat dataset, which is
    converted to a pandas DataFrame.

    Args:
        indicator: code from the World Bank IDS database (e.g. "DT.AMT.BLAT.CD")
        countries: one or more World Bank 3-letter codes. A list of codes is provided
            using the `ids_codes` function. Alternatively, "all" can be used.
        start_year: first year of the period
        end_year: last year of the period (inclusive)
        source: the database number (see World Bank IDS documentation)

    Returns:
        A dataframe with the _data from the World Bank IDS API

    """
    # Get API url
    url = ids_api_url(
        indicator=indicator,
        countries=countries,
        start_year=start_year,
        end_year=end_year,
        source=source,
    )

    # Get _data
    try:
        data = _fetch_ids_indicator(url=url)
        return data.pipe(_clean_ids_response, indicator=indicator)

    except requests.exceptions.HTTPError:
        print(f"Failed to get _data for {indicator}")

    except requests.exceptions.JSONDecodeError:
        print(f"Failed to get _data for {indicator}")

    except Exception as e:
        print("Ran into other trouble: ", e)
