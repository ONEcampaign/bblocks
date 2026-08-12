#CEPII-BACI Importer

The `BACI` importer allows you to access international trade data from the CEPII-BACI database. 

## About BACI

BACI provides harmonized and reconciled data on bilateral trade flows at the product level, 
with products classified using the Harmonized System (HS), the standard trade nomenclature used by most customs authorities 
worldwide. The BACI data uses data from the United Nations [Comtrade database](comtrade.un.org)

Visit the [CEPII-BACI website](https://www.cepii.fr/DATA_DOWNLOAD/baci/doc/baci_webpage.html#download-links) 
for more information.

## Basic Usage

To start using the importer, instantiate the importer and use the `get_data` method 
to fetch all trade data. 

```py
from bblocks.data_importers import BACI

# create an instance of the BACI importer
baci = BACI()

# fetch all trade data as a DataFrame
df = baci.get_data()
```

By default, the data is returned with product and country codes. To add labels for products and countries,
you can set the `include_product_labels` and `include_country_labels` parameters to `True` when calling the `get_data` method.

```py title="Fetch data with labels"
# fetch trade data with product and country labels
df_labeled = baci.get_data(include_product_labels=True, include_country_labels=True)
```

## Access different HS versions

BACI provides different versions of the Harmonized System (HS) classification for trade data.
To see the available HS versions, you can use the `available_hs_versions` method.

```py
hs_versions = baci.available_hs_versions()
```

By default, the `get_data` method fetches data using the latest HS version available (HS 2022).
You can specify the desired HS version by using the `hs_version` parameter when calling the `get_data` method.

```py title="Fetch specfic HS version"
# fetch trade data using HS 2012 version
df_hs2012 = baci.get_data(hs_version='HS12')
```

## Access metadata

The `BACI` importer also provides methods to access metadata including product and country information.

To get product codes and names for a specific HS version, you can use the `get_product_codes` method.

```py title="Get product codes and labels"
products = baci.get_product_codes()
```

You can also get country codes, names, and ISO3 codes using the `get_country_codes` method.

```py title="Get country codes and labels"
countries = baci.get_country_codes()
```

To get the metadata for the BACI data, use the `get_metadata` method.

```py title="Get metadata"
metadata = baci.get_metadata()
```

You can also get information on products, countries, and metadata for different HS versions
by specifying the `hs_version` parameter in the respective methods.

```py
products_hs2012 = baci.get_product_codes(hs_version='HS12')
countries_hs2012 = baci.get_country_codes(hs_version='HS12')
metadata_hs2012 = baci.get_metadata(hs_version='HS12')
```

## Caching

Data is cached to improve efficiency and avoid repeated downloads. 
The cached data persists for 48 hours and is not tied to the importer instance. Newly
instantiated importers will use the cached data if it is still valid.

To clear the cached data, you can use the `clear_cache` method.

```py title="Clear cache"
baci.clear_cache()
```

This will remove all cached data for the BACI importer, and disk cache data. Any other instantiated importers
may still have data in memory until they are deleted or the session ends.
