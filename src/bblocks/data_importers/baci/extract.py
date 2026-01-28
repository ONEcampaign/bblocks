"""Extraction functions for BACI data importer.

TODO: caching
TODO: validation


"""

import requests
import bs4
from bs4 import BeautifulSoup
import pandas as pd
import io
import zipfile
from zipfile import ZipFile
import pyarrow.csv as pv
import pyarrow as pa


from bblocks.data_importers.config import DataExtractionError, Fields, logger


# URL to the BACI data page
URL: str = "https://www.cepii.fr/DATA_DOWNLOAD/baci/doc/baci_webpage.html"

BACI_DATA_COLUMNS = {
        "t": Fields.year,
        "i": Fields.exporter_code,
        "j": Fields.importer_code,
        "k": Fields.product_code,
        "v": Fields.value,
        "q": Fields.quantity,
    }



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


#TODO: add caching
def extract_data_links() -> dict[str, str]:
    """Extract the BACI data links from the CEPII BACI page.

    Returns:
        A dictionary mapping the HS version to the download link.

    """

    soup = _get_soup()
    return _parse_data_links(soup)


def _parse_readme(readme_content: str) -> dict:
    """Parse the Readme.txt content to extract metadata.

    Args:
        readme_content: The content of the Readme.txt file as a string.

    Returns:
        A dictionary containing the extracted metadata.
    """

    # normalize all line breaks to "\n" for consistent processing
    readme_content = readme_content.replace("\r\n", "\n").replace("\r", "\n")

    blocks = [block.strip() for block in readme_content.split("\n\n") if block.strip()]
    metadata = {}

    for block in blocks:
        if block.startswith("List of Variables:"):
            continue
        lines = block.splitlines()
        if ":" in lines[0]:
            key, first_value_line = lines[0].split(":", 1)
            key = key.strip()
            # Strip each line individually and join with space
            value_lines = [first_value_line] + lines[1:]
            value = " ".join(line.strip() for line in value_lines).strip()
            metadata[key] = value


    return metadata


class BaciDataManager:
    """ """


    def __init__(self, hs_version: str, url: str):

        self._hs_version = hs_version
        self._url = url

        self._zip_file = None
        self._data = None
        self._product_codes = None
        self._country_codes = None
        self._metadata = None

    def extract_zip_file(self) -> None:
        """Extract the BACI zip file from the given URL.

        Args:
            url: URL to the BACI zip file

        Returns:
            A ZipFile object containing the extracted data
        """

        try:
            logger.info(f"Extracting BACI data")

            response = requests.get(self._url)
            response.raise_for_status()

            zip_data = io.BytesIO(response.content)
            self._zip_file = ZipFile(zip_data)

        except requests.RequestException as e:
            raise DataExtractionError(f"Failed to extract BACI data: {e}")

    def _list_data_files(self) -> list[str]:
        """List all relevant BACI data files in the ZIP archive."""

        files = self._zip_file.namelist()

        # Filter for CSV files that start with "BACI" and hs version such a "BACI_HS22....csv"
        data_files = [
            f
            for f in files
            if f.startswith(f"BACI_{self._hs_version}") and f.endswith(".csv")
        ]

        if not data_files:
            raise FileNotFoundError(
                f"No BACI data files found for HS version {self._hs_version}"
            )

        return data_files

    def _read_data_files(self):
        """Read data files to a DataFrame using pyarrow."""

        tables = []

        for name in self._list_data_files():
            with self._zip_file.open(name) as f:
                tables.append(pv.read_csv(f))

        table = pa.concat_tables(tables, unicode_promote_options="default")

        self._data = (
            table
            .rename_columns([BACI_DATA_COLUMNS.get(c, c) for c in table.schema.names])
            .to_pandas(split_blocks=True, self_destruct=True)
        )

    def _read_product_codes(self) -> pd.DataFrame:
        """Read product codes"""

        # Find the product codes file in the ZIP archive
        product_code_file = next((f for f in self._zip_file.namelist() if f.startswith("product_codes")), None)

        if not product_code_file:
            raise FileNotFoundError("No product codes found")

        # Read the product codes CSV file into a DataFrame
        self._product_codes = (pd.read_csv(self._zip_file.open(product_code_file))
                               .rename(columns ={"code": Fields.product_code,
                                                 "description": Fields.product_description})
                               )

    def _read_country_codes(self):
        """Read country codes"""

        country_codes_file = next(
            (f for f in self._zip_file.namelist() if f.startswith("country_codes")), None
        )

        if not country_codes_file:
            raise FileNotFoundError("No country codes file found in the ZIP file.")

        # Read the country codes CSV file into a DataFrame
        self._country_codes = (pd.read_csv(self._zip_file.open(country_codes_file))
                               .drop(columns = "country_iso2") # drop duplicate iso2 column
                               .rename(columns = {"country_code": Fields.country_code,
                                                  "country_name": Fields.country_name,
                                                  "country_iso3": Fields.iso3_code,
                                                  })
                               )

    def _read_readme(self) -> None:
        """Read metadata from the Readme.txt file in the ZIP archive."""

        # Find the Readme.txt file in the ZIP archive
        readme_file = next((f for f in self._zip_file.namelist() if f.startswith("Readme.txt")), None)

        if not readme_file:
            raise FileNotFoundError("No metadata found")

        with self._zip_file.open(readme_file) as f:
            readme_content = f.read().decode("utf-8")

        # Parse the Readme content to extract metadata
        metadata = _parse_readme(readme_content)
        if not metadata:
            raise DataExtractionError("No metadata found")

        self._metadata = metadata


    def parse_data(self):
        """Parse data and save to object"""

        logger.info(f"Parsing BACI data")

        self._read_data_files()
        self._read_product_codes()
        self._read_country_codes()
        self._read_readme()


    def load_data(self):
        """Extract and load all data to the object"""

        self.extract_zip_file()
        self.parse_data()
        logger.info(f"Successfully loaded BACI data")










