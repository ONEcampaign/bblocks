"""BACI importer"""

import pandas as pd

from bblocks.data_importers.config import Fields, logger
from bblocks.data_importers.baci.extract import extract_data_links, BaciDataManager



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

    def _load_data(self, hs_version) -> None:
        """Helper function to load data for a specific HS version
        Verify the correct HS version is provided, and ensure the data link is available.
        """

        if hs_version not in self.available_hs_versions():
            raise ValueError(f"HS version {hs_version} not available. "
                             f"Available versions: {list(self._hs_versions.keys())}")

        self._data[hs_version] = BaciDataManager(hs_version=hs_version, url=self._hs_versions[hs_version])
        self._data[hs_version].load_data()

    def _add_product_labels(self, df: pd.DataFrame, hs_version: str) -> pd.DataFrame:
        """Add product labels to the data DataFrame

        Returns:
            DataFrame with product labels added
        """

        logger.info("Adding product labels")

        prod_mapping = (self._data[hs_version]._product_codes
                        .set_index(Fields.product_code)
                        [Fields.product_description]
                        .to_dict()
                        )

        return (df
                .assign(**{Fields.product_description: lambda d: d[Fields.product_code].map(prod_mapping)})
                )

    def _add_country_labels(self, df: pd.DataFrame, hs_version: str) -> pd.DataFrame:
        """Add country labels to the data DataFrame including country name and ISO3 code

        Returns:
            DataFrame with country labels added
        """

        logger.info("Adding country labels")

        country_name_mapping = (self._data[hs_version]._country_codes
                                .set_index(Fields.country_code)
                                [Fields.country_name]
                                .to_dict()
                                )

        iso3_mapping = (self._data[hs_version]._country_codes
                        .set_index(Fields.country_code)
                        [Fields.iso3_code]
                        .to_dict()
                        )

        return (df
                .assign(**{
                    Fields.exporter_name: lambda d: d[Fields.exporter_code].map(country_name_mapping),
                    Fields.importer_name: lambda d: d[Fields.importer_code].map(country_name_mapping),
                    Fields.exporter_iso3_code: lambda d: d[Fields.exporter_code].map(iso3_mapping),
                    Fields.importer_iso3_code: lambda d: d[Fields.importer_code].map(iso3_mapping),
                })
                )

    def get_data(self, hs_version: str = "HS22",
                 include_product_labels: bool = False,
                 include_country_labels: bool = False) -> pd.DataFrame:
        """ """

        # if data for the requested HS version is not loaded, load it
        if hs_version not in self._data:
            self._load_data(hs_version)

        df = self._data[hs_version]._data.copy(deep=True)

        if include_product_labels:
            df = self._add_product_labels(df, hs_version)

        if include_country_labels:
            df = self._add_country_labels(df, hs_version)

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

