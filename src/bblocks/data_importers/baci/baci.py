"""BACI importer"""

import pandas as pd
from diskcache import Cache
from platformdirs import user_cache_dir
import atexit
import requests
from bs4 import BeautifulSoup
import io
from zipfile import ZipFile
import pyarrow.csv as pv
import pyarrow as pa

from bblocks.data_importers.config import Fields, logger, DataExtractionError
# from bblocks.data_importers.baci.extract import extract_data_links, BaciDataExtractor


_CACHE_EXPIRY_SECONDS: int = 48 * 60 * 60  # cache expiry after 48 hours
_CACHE_DIR = user_cache_dir("bblocks/baci")
_DATA_CACHE = Cache(_CACHE_DIR, size_limit=1e12)
_DATA_CACHE.stats(enable=True)  # Enable hit/miss tracking

# Ensure cache is properly closed on exit to persist WAL data to disk
atexit.register(_DATA_CACHE.close)



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


@_DATA_CACHE.memoize(expire=_CACHE_EXPIRY_SECONDS)
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


class BaciDataExtractor:
    """ """


    def __init__(self, hs_version: str, url: str):

        self.hs_version = hs_version
        self.url = url

        self.zip_file = None
        self.data = None
        self.product_codes = None
        self.country_codes = None
        self.metadata = None

    def extract_zip_file(self) -> None:
        """Extract the BACI zip file from the given URL."""

        try:
            response = requests.get(self.url)
            response.raise_for_status()

            zip_data = io.BytesIO(response.content)
            self.zip_file = ZipFile(zip_data)

        except requests.RequestException as e:
            raise DataExtractionError(f"Failed to extract BACI data: {e}")

    def _list_data_files(self) -> list[str]:
        """List all relevant BACI data files in the ZIP archive."""

        files = self.zip_file.namelist()

        # Filter for CSV files that start with "BACI" and hs version such a "BACI_HS22....csv"
        data_files = [
            f
            for f in files
            if f.startswith(f"BACI_{self.hs_version}") and f.endswith(".csv")
        ]

        if not data_files:
            raise FileNotFoundError(
                f"No BACI data files found for HS version {self.hs_version}"
            )

        return data_files

    def _read_data_files(self) -> None:
        """Read data files to a DataFrame using pyarrow."""

        tables = []

        for name in self._list_data_files():
            with self.zip_file.open(name) as f:
                tables.append(pv.read_csv(f))

        table = pa.concat_tables(tables, unicode_promote_options="default")

        self.data = (
            table
            .rename_columns([BACI_DATA_COLUMNS.get(c, c) for c in table.schema.names])
            .to_pandas(split_blocks=True, self_destruct=True)
        )

    def _read_product_codes(self) -> None:
        """Read product codes"""

        # Find the product codes file in the ZIP archive
        product_code_file = next((f for f in self.zip_file.namelist() if f.startswith("product_codes")), None)

        if not product_code_file:
            raise FileNotFoundError("No product codes found")

        # Read the product codes CSV file into a DataFrame
        self.product_codes = (pd.read_csv(self.zip_file.open(product_code_file))
                               .rename(columns ={"code": Fields.product_code,
                                                 "description": Fields.product_description})
                               )

    def _read_country_codes(self) -> None:
        """Read country codes"""

        country_codes_file = next(
            (f for f in self.zip_file.namelist() if f.startswith("country_codes")), None
        )

        if not country_codes_file:
            raise FileNotFoundError("No country codes file found in the ZIP file.")

        # Read the country codes CSV file into a DataFrame
        self.country_codes = (pd.read_csv(self.zip_file.open(country_codes_file))
                               .drop(columns = "country_iso2") # drop duplicate iso2 column
                               .rename(columns = {"country_code": Fields.country_code,
                                                  "country_name": Fields.country_name,
                                                  "country_iso3": Fields.iso3_code,
                                                  })
                               )

    def _read_readme(self) -> None:
        """Read metadata from the Readme.txt file in the ZIP archive."""

        # Find the Readme.txt file in the ZIP archive
        readme_file = next((f for f in self.zip_file.namelist() if f.startswith("Readme.txt")), None)

        if not readme_file:
            raise FileNotFoundError("No metadata found")

        with self.zip_file.open(readme_file) as f:
            readme_content = f.read().decode("utf-8")

        # Parse the Readme content to extract metadata
        metadata = _parse_readme(readme_content)
        if not metadata:
            raise DataExtractionError("No metadata found")

        self.metadata = metadata


    def read_data(self) -> None:
        """Parse data and save to object"""

        self._read_data_files()
        self._read_product_codes()
        self._read_country_codes()
        self._read_readme()


def _add_product_labels(data_manager: BaciDataExtractor) -> None:
    """Add product labels to the data DataFrame

    Returns:
        DataFrame with product labels added
    """

    prod_mapping = (data_manager.product_codes
                    .set_index(Fields.product_code)
                    [Fields.product_description]
                    .to_dict()
                    )

    data_manager.data = (data_manager.data
                          .assign(**{Fields.product_description: lambda d: d[Fields.product_code].map(prod_mapping)})
                          )

def _add_country_labels(data_manager: BaciDataExtractor) -> None:
    """Add country labels to the data DataFrame including country name and ISO3 code

    Returns:
        DataFrame with country labels added
    """

    country_name_mapping = (data_manager.country_codes
                            .set_index(Fields.country_code)
                            [Fields.country_name]
                            .to_dict()
                            )

    iso3_mapping = (data_manager.country_codes
                    .set_index(Fields.country_code)
                    [Fields.iso3_code]
                    .to_dict()
                    )

    data_manager.data = (data_manager.data
            .assign(**{
                Fields.exporter_name: lambda d: d[Fields.exporter_code].map(country_name_mapping),
                Fields.importer_name: lambda d: d[Fields.importer_code].map(country_name_mapping),
                Fields.exporter_iso3_code: lambda d: d[Fields.exporter_code].map(iso3_mapping),
                Fields.importer_iso3_code: lambda d: d[Fields.importer_code].map(iso3_mapping),
            })
            )

@_DATA_CACHE.memoize(expire=_CACHE_EXPIRY_SECONDS)
def _extract_data(hs_version: str) -> dict:
    """Helper function to load data for a specific HS version
    Verify the correct HS version is provided, and ensure the data link is available.
    """

    data_links = extract_data_links()

    if hs_version not in list(data_links.keys()):
        raise ValueError(f"HS version {hs_version} not available. "
                         f"Available versions: {list(data_links.keys())}")

    logger.info(f"Extracting BACI data for HS version {hs_version}")
    data_manager = BaciDataExtractor(hs_version=hs_version, url=data_links[hs_version])
    data_manager.extract_zip_file() # extract data

    logger.info("Parsing BACI data files")
    data_manager.read_data() # read data
    _add_product_labels(data_manager) # add product labels
    _add_country_labels(data_manager) # add country labels

    # TODO: validation checks

    return {
        "data": data_manager.data,
        "country_codes": data_manager.country_codes,
        "product_codes": data_manager.product_codes,
        "metadata": data_manager.metadata,
    }

class BACI:
    """BACI data importer class"""

    def __init__(self):

        self._hs_versions: dict | None = None
        self._data: dict[str, dict] = dict()

    def _load_data(self, hs_version: str):
        """Load data to object"""

        if hs_version not in self._data:
            self._data[hs_version] = _extract_data(hs_version)
            logger.info("Successfully loaded BACI data for HS version {hs_version}")


    def available_hs_versions(self) -> list[str]:
        """Get a list of available HS versions in the BACI dataset"""

        if self._hs_versions is None:
            self._hs_versions = extract_data_links()

        return list(self._hs_versions.keys())

    def get_data(self, hs_version: str = "HS22",
                 include_product_labels: bool = False,
                 include_country_labels: bool = False
                 ) -> pd.DataFrame:
        """ """

        self._load_data(hs_version)

        df = self._data[hs_version]["data"]

        # Determine columns to keep
        columns = list(df.columns)
        if not include_product_labels:
            columns.remove(Fields.product_description)
        if not include_country_labels:
            for col in [
                Fields.exporter_name,
                Fields.importer_name,
                Fields.exporter_iso3_code,
                Fields.importer_iso3_code,
            ]:
                columns.remove(col)

        return df.loc[:, columns]

    def get_country_codes(self, hs_version: str = "HS22") -> pd.DataFrame:
        """Get the country codes DataFrame for the specified HS version"""

        self._load_data(hs_version)

        return self._data[hs_version]["country_codes"]

    def get_product_codes(self, hs_version: str = "HS22") -> pd.DataFrame:
        """Get the product codes DataFrame for the specified HS version"""

        self._load_data(hs_version)

        return self._data[hs_version]["product_codes"]

    def get_metadata(self, hs_version: str = "HS22") -> dict:
        """Get metadata for the specified HS version"""

        self._load_data(hs_version)

        return self._data[hs_version]["metadata"]

    def clear_cache(self) -> None:
        """Clear the cached World Bank data."""

        _DATA_CACHE.clear()
        self._hs_versions = None
        self._data = dict()

        logger.info("BACI cache cleared.")

