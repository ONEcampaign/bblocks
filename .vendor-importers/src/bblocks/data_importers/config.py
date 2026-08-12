"""Configuration for the data importers.

Including:
- Path configuration
- Logger configuration
- Custom exceptions

TODO: cache settings

"""

import logging
from pathlib import Path
from typing import Literal

# Configure Logging
logger = logging.getLogger(__name__)
shell_handler = logging.StreamHandler()  # Create terminal handler
logger.setLevel(logging.INFO)  # Set levels for the logger, shell and file
shell_handler.setLevel(logging.INFO)  # Set levels for the logger, shell and file

# Format the outputs   "%(levelname)s (%(asctime)s): %(message)s"
fmt_file = "%(levelname)s: %(message)s"

# "%(levelname)s %(asctime)s [%(filename)s:%(funcName)s:%(lineno)d] %(message)s"
fmt_shell = "%(levelname)s: %(message)s"

shell_formatter = logging.Formatter(fmt_shell)  # Create formatters
shell_handler.setFormatter(shell_formatter)  # Add formatters to handlers
logger.addHandler(shell_handler)  # Add handlers to the logger


class Paths:
    """Configuration for paths"""

    project = Path(__file__).resolve().parent.parent
    data = project / "data_importers" / ".data"
    wb_importer = project / "data_importers" / "world_bank"


class DataExtractionError(Exception):
    """Raised when data extraction fails."""


class DataFormattingError(Exception):
    """Raised when data formatting fails."""


class DataValidationError(Exception):
    """Raised when data validation fails."""


def set_data_path(path):
    """Set the path to the folder containing the raw data or where raw data will be stored.

    Args:
        path: Path to the raw data folder
    """

    Paths.data = Path(path).resolve()


# Types
weo_version = Literal["latest"] | tuple[Literal["April", "October"], int]


# Field and column names


class Fields:
    # value fields
    value = "value"
    value_upper = "value_upper"
    value_lower = "value_lower"

    # country, region and other entity names and codes
    country_name = "country_name"
    country_code = "country_code"
    region_name = "region_name"
    entity_name = "entity_name"  # The name of the entity (country or region) to be used when entities are mixed
    iso2_code = "iso2_code"
    iso3_code = "iso3_code"
    entity_code = "entity_code"
    counterpart_code = "counterpart_code"
    counterpart_name = "counterpart_name"
    region_code = "region_code"
    income_level_code = "income_level_code"
    income_level_name = "income_level_name"

    # time fields
    year = "year"
    date = "date"

    # other fields
    indicator_code = "indicator_code"
    indicator_name = "indicator_name"
    unit = "unit"
    currency = "currency"
    source = "source"
    data_type = "data_type"
    time_range = "time_range"
    notes = "notes"
    quantity = "quantity"
    footnote = "footnote"

    # trade-related fields
    exporter_code = "exporter_code"
    importer_code = "importer_code"
    exporter_iso3_code = "exporter_iso3_code"
    importer_iso3_code = "importer_iso3_code"
    exporter_name = "exporter_name"
    importer_name = "importer_name"
    product_code = "product_code"
    product_name = "product_name"
    product_description = "product_description"

    @classmethod
    def get_base_idx(cls):
        return [cls.year, cls.entity_code, cls.entity_name]

    @classmethod
    def get_ids_idx(cls):
        return [cls.year, cls.entity_code, cls.entity_name, cls.counterpart_code]


class Units:
    """A class to store the units of measurement for the data"""

    percent = "percent"
