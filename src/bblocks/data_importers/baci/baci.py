"""BACI importer"""

import pandas as pd
from diskcache import Cache
from platformdirs import user_cache_dir
import atexit

from bblocks.data_importers.config import Fields, logger
from bblocks.data_importers.baci.extract import extract_data_links, BaciDataManager


_CACHE_EXPIRY_SECONDS: int = 48 * 60 * 60  # cache expiry after 48 hours
_CACHE_DIR = user_cache_dir("bblocks/baci")
_DATA_CACHE = Cache(_CACHE_DIR)
_DATA_CACHE.stats(enable=True)  # Enable hit/miss tracking

# Ensure cache is properly closed on exit to persist WAL data to disk
atexit.register(_DATA_CACHE.close)


def _add_product_labels(data_manager: BaciDataManager) -> None:
    """Add product labels to the data DataFrame

    Returns:
        DataFrame with product labels added
    """

    prod_mapping = (data_manager._product_codes
                    .set_index(Fields.product_code)
                    [Fields.product_description]
                    .to_dict()
                    )

    data_manager._data = (data_manager._data
                          .assign(**{Fields.product_description: lambda d: d[Fields.product_code].map(prod_mapping)})
                          )

def _add_country_labels(data_manager: BaciDataManager) -> None:
    """Add country labels to the data DataFrame including country name and ISO3 code

    Returns:
        DataFrame with country labels added
    """

    country_name_mapping = (data_manager._country_codes
                            .set_index(Fields.country_code)
                            [Fields.country_name]
                            .to_dict()
                            )

    iso3_mapping = (data_manager._country_codes
                    .set_index(Fields.country_code)
                    [Fields.iso3_code]
                    .to_dict()
                    )

    data_manager._data = (data_manager._data
            .assign(**{
                Fields.exporter_name: lambda d: d[Fields.exporter_code].map(country_name_mapping),
                Fields.importer_name: lambda d: d[Fields.importer_code].map(country_name_mapping),
                Fields.exporter_iso3_code: lambda d: d[Fields.exporter_code].map(iso3_mapping),
                Fields.importer_iso3_code: lambda d: d[Fields.importer_code].map(iso3_mapping),
            })
            )

@_DATA_CACHE.memoize(expire=_CACHE_EXPIRY_SECONDS)
def _load_data(hs_version: str) -> BaciDataManager:
    """Helper function to load data for a specific HS version
    Verify the correct HS version is provided, and ensure the data link is available.
    """

    data_links = extract_data_links()

    if hs_version not in list(data_links.keys()):
        raise ValueError(f"HS version {hs_version} not available. "
                         f"Available versions: {list(data_links.keys())}")

    logger.info(f"Extracting BACI data for HS version {hs_version}")

    data_manager = BaciDataManager(hs_version=hs_version, url=data_links[hs_version])
    data_manager.extract_zip_file() # extract data

    logger.info("Parsing BACI data files")

    data_manager.read_data() # read data
    _add_product_labels(data_manager) # add product labels
    _add_country_labels(data_manager) # add country labels

    # TODO: validation checks

    logger.info(f"Successfully extracted BACI data for HS version {hs_version}")

    return data_manager

class BACI:
    """BACI data importer class"""

    def __init__(self):

        self._hs_versions: dict | None = None
        self._data: dict[str, dict] = dict()


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

        # if data for the requested HS version is not loaded, load it
        if hs_version not in self._data:
            self._data[hs_version] = _load_data(hs_version)

        df = self._data[hs_version]._data.copy(deep=True)

        if not include_product_labels:
            df = df.drop(columns=[Fields.product_description])

        if not include_country_labels:
            df = df.drop(columns=[
                Fields.exporter_name,
                Fields.importer_name,
                Fields.exporter_iso3_code,
                Fields.importer_iso3_code,
            ])

        return df

    def get_country_codes(self, hs_version: str = "HS22") -> pd.DataFrame:
        """Get the country codes DataFrame for the specified HS version"""

        if hs_version not in self._data:
            self._load_data(hs_version)

        return self._data[hs_version]._country_codes

    def get_product_codes(self, hs_version: str = "HS22") -> pd.DataFrame:
        """Get the product codes DataFrame for the specified HS version"""

        if hs_version not in self._data:
            self._load_data(hs_version)

        return self._data[hs_version]._product_codes

    def get_metadata(self, hs_version: str = "HS22") -> dict:
        """Get metadata for the specified HS version"""

        if hs_version not in self._data:
            self._load_data(hs_version)

        return self._data[hs_version]._metadata

