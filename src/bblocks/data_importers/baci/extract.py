"""Extraction functions for BACI data importer."""

import requests
import bs4
from bs4 import BeautifulSoup

from src.bblocks.data_importers.utilities import logger
from src.bblocks.data_importers.config import DataExtractionError


# URL to the BACI data page
URL: str = "https://www.cepii.fr/DATA_DOWNLOAD/baci/doc/baci_webpage.html"


def _get_soup() -> BeautifulSoup:
    """Request the CEPII BACI page and return a BeautifulSoup object."""

    try:
        logger.debug(f"Fetching soup for BACI page")
        response = requests.get(URL)
        response.raise_for_status()

        return BeautifulSoup(response.content, "html.parser")

    except requests.RequestException as e:
        raise DataExtractionError(f"Failed to fetch BACI page: {e}")




def _parse_data_links(soup: BeautifulSoup) -> dict[str, str]:
    """Parse the BACI data links from the BeautifulSoup object.

    Finds the section with download links id="download-links" and extracts all links
    that start with "HS".

    Args:
        soup: BeautifulSoup object of the BACI page

    Returns:
        A dictionary mapping the HS version to the download link.
    """

    try:
        logger.debug(f"Parsing BACI data links")

        # find the section with download links
        section = soup.find("section", {"id": "download-links"})

        data_links_dict = {a.text: a["href"]
                           for a in section.find_all("a")
                           if a.text.startswith("HS")
                           }

        if not data_links_dict:
            raise DataExtractionError("No BACI data links found")

        return data_links_dict

    except Exception as e:
        raise DataExtractionError(f"Failed to parse BACI data links: {e}")




