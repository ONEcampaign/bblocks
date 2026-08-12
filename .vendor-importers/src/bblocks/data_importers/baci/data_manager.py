"""Data manager for BACI data importer."""

import pandas as pd
import requests
import io
from zipfile import ZipFile
import pyarrow as pa
import pyarrow.csv as pv

from bblocks.data_importers.config import DataExtractionError
from bblocks.data_importers.config import Fields
from bblocks.data_importers.utilities import convert_dtypes


BACI_DATA_COLUMNS = {
    "t": Fields.year,
    "i": Fields.exporter_code,
    "j": Fields.importer_code,
    "k": Fields.product_code,
    "v": Fields.value,
    "q": Fields.quantity,
}


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
    """Class to extract and parse BACI data for a given HS version and download url

    The class handles downloading the BACI zip file, extracting relevant data files,
    reading them into pandas DataFrames, and parsing metadata from the Readme.txt file.
    """

    def __init__(self, hs_version: str, url: str):

        self.hs_version = hs_version
        self.url = url

        self._zip_file = None
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
            with self._zip_file.open(name) as f:
                tables.append(pv.read_csv(f))

        table = pa.concat_tables(tables, unicode_promote_options="default")

        self.data = table.rename_columns(
            [BACI_DATA_COLUMNS.get(c, c) for c in table.schema.names]
        ).to_pandas(split_blocks=True, self_destruct=True)

        self.data = convert_dtypes(self.data)

    def _read_product_codes(self) -> None:
        """Read product codes"""

        # Find the product codes file in the ZIP archive
        product_code_file = next(
            (f for f in self._zip_file.namelist() if f.startswith("product_codes")),
            None,
        )

        if not product_code_file:
            raise FileNotFoundError("No product codes found")

        # Read the product codes CSV file into a DataFrame
        self.product_codes = (
            pd.read_csv(self._zip_file.open(product_code_file))
            .rename(
                columns={
                    "code": Fields.product_code,
                    "description": Fields.product_description,
                }
            )
            .pipe(convert_dtypes, casts={Fields.product_description: pa.large_string()})
        )

    def _read_country_codes(self) -> None:
        """Read country codes"""

        country_codes_file = next(
            (f for f in self._zip_file.namelist() if f.startswith("country_codes")),
            None,
        )

        if not country_codes_file:
            raise FileNotFoundError("No country codes file found in the ZIP file.")

        # Read the country codes CSV file into a DataFrame
        self.country_codes = (
            pd.read_csv(self._zip_file.open(country_codes_file))
            .drop(columns="country_iso2")  # drop duplicate iso2 column
            .rename(
                columns={
                    "country_code": Fields.country_code,
                    "country_name": Fields.country_name,
                    "country_iso3": Fields.iso3_code,
                }
            )
            .pipe(convert_dtypes)
        )

    def _read_readme(self) -> None:
        """Read metadata from the Readme.txt file in the ZIP archive."""

        # Find the Readme.txt file in the ZIP archive
        readme_file = next(
            (f for f in self._zip_file.namelist() if f.startswith("Readme.txt")), None
        )

        if not readme_file:
            raise FileNotFoundError("No metadata found")

        with self._zip_file.open(readme_file) as f:
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

    def extract(self) -> None:
        """Extract and parse data to object"""

        self.extract_zip_file()
        self.read_data()

        # remove zip file from memory
        self._zip_file.close()
        self._zip_file = None
