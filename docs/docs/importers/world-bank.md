# World Bank Importer

The `WorldBank` importer provides access to World Bank databases accessible through
the World Bank public API. 

## About World Bank data

The World Bank is one of the best known aggregators and publishers of 
development data on a broad range of topics including poverty, debt, health, 
environmental sustainability and many others. The World Bank makes its data
available through several databases such as the 
[World Development Indicators (WDI)](https://datatopics.worldbank.org/world-development-indicators/)
and the [International Debt Statistics (IDS)](https://www.worldbank.org/en/programs/debt-statistics/ids).

The `WorldBank` importer is built on top of the 
[wbgapi](https://pypi.org/project/wbgapi/) Python package, a powerful
and complete wrapper for the World Bank API. The `WorldBank` importer offers simple
access to World Bank data with a consistent interface. For more granular control of API
settings and additional functionality, consider using the 
[wbgapi](https://pypi.org/project/wbgapi/) package.

## Basic Usage

Before getting data you should know which database you want to access (there are several). Luckily 
you can easily get the available databases

```python
from bblocks.data_importers import get_wb_databases

print(get_wb_databases())
```

### WorldBank importer
This will return a dataframe with the available World Bank databases and their IDs. The ID is
necessary to specify which database you want to access.

To start accessing World Bank data, first instantiate a `WorldBank` importer.

```python
from bblocks.data_importers import WorldBank

wb = WorldBank()
```

The most commonly accessed database for development indicators is the World Development Indicators (WDI)
database with ID `2`. By default, the `WorldBank` importer connects to this database. You can specify 
a different database by passing the database ID when instantiating the importer.

```python
wb = WorldBank(database=1)  # Doing Business database
```

### Get available indicators and entities

To see which indicators are available in the selected database, use the `get_available_indicators` method.

```python
indicators = wb.get_available_indicators()
```

This will return a dataframe with the available indicators in the selected database, including 
their codes and names. The indicator codes are necessary to request specific data series.

Similarly, to see which entities (such as countries) are available in the selected database,
use the `get_available_entities` method.

```python
entities = wb.get_available_entities()
```


### Get indicator metadata

To get metadata for a specific indicator, use the `get_indicator_metadata` method.

```python
metadata = wb.get_indicator_metadata("NY.GDP.MKTP.CD")  # GDP indicator
```

This will return a dataframe with metadata for the specified indicator. Multiple 
indicators can be specified by passing a list of indicator codes.


### Access data
To access data use the `get_data` method. For example, to get GDP data (indicator code `NY.GDP.MKTP.CD`):

```python

df = wb.get_data(indicator_code="NY.GDP.MKTP.CD")

```

This will return a dataframe with GDP data for all available countries and years.

You can specify additional parameters such as entities (such as countries), years, whether to skip missing values and
aggregates, and whether to include labels for entities and indicators.

```python
df = wb.get_data(indicator_code="NY.GDP.MKTP.CD",
                 entity_code=["ZWE", "NGA"], # Zimbabwe and Nigeria
                 start_year=2000,
                 end_year=2020,
                 skip_aggs=True, # skip aggregates like "World"
                 skip_blanks=True, # skip missing values
                 labels=True # include labels
                 )
```

Multiple indicators can be specified by passing a list of indicator codes.

```python
df = wb.get_data(indicator_code=["NY.GDP.MKTP.CD", "SP.POP.TOTL"])  # GDP and population indicators
```

Since World Bank data can be large and the API requests can be slow, this importer batches
indicators and uses multi-threading to speed up data retrieval. By default, batches of 1 
indicator are used with 4 threads. You can adjust these settings by passing the `batch_size` and
`thread_num` parameters when instantiating the importer.

```python
df = wb.get_data(indicator_code=["NY.GDP.MKTP.CD", "SP.POP.TOTL"],
                    batch_size=5, # use batches of 5 indicators
                    thread_num=10 # use 10 threads
                 )
```

Other API parameters can be set in the params argument of the `get_data` method. For example,
to get the most recent value:

```python
df = wb.get_data(indicator_code=["NY.GDP.MKTP.CD", "SP.POP.TOTL"], params={"mrv": 1})
```

See the wbgapi documentation for more details on available parameters. NOTE that some of these
additional parameters may not work as expected with certain databases.

Pagination is handled automatically by the importer. By default, the importer will retrieve
50,000 records per request. You can adjust this by setting the `per_page` attribute.

```python

df = wb.get_data(indicator_code=["NY.GDP.MKTP.CD", "SP.POP.TOTL"], params={"per_page": 100000})

```

### Caching data

Data is cached for efficiency. Data is cached in disk up to 3 hours by default. 
The cache can be cleared in different ways but **NOTE** that clearing the cache will 
remove all cached World Bank data

If have instantiated a World Bank importer, you can clear the cache using
the `clear_cache` method.

```python
wb.clear_cache()
```

Alternatively, you can use the `clear_wb_cache` function.

```python
from bblocks.data_importers import clear_wb_cache

clear_wb_cache()
```

Either of these methods will clear all cached World Bank data.


## Convenience methods

If you want to quickly access available indicators, entities, and metadata without
instantiating a WorldBank object, you can use the following convenience functions:

```python
from bblocks.data_importers import (
    get_wb_entities,
    get_wb_indicator_metadata,
    get_wb_indicators,
)


inds = get_wb_indicators(db=2)  # Get indicators from WDI database
ents = get_wb_entities(db=2)     # Get entities from WDI database
meta = get_wb_indicator_metadata(indicator_code = "NY.GDP.MKTP.CD", db=2)  # Get metadata
```
To get all the entities or indicators in all databases, set `db=None`.

## Specialised World Bank Importers

For commonly accessed World Bank databases, specialised importers exist with additional
functionality. These include:
- InternationalDebtStatistics - Access International Debt Statistics (IDS) data on long-term debt


### International Debt Statistics Importer

To access data from the International Debt Statistics (IDS) database, use the
`InternationalDebtStatistics` importer.

```python
from bblocks.data_importers import InternationalDebtStatistics

ids = InternationalDebtStatistics()
```

The importer contains all the methods of the base `WorldBank` importer, plus
additional methods specific to the IDS database.

To see PPG debt stock indicators use the `debt_stock_indicators` property.

```python
ids.debt_stock_indicators
```

This will return a dataframe with all PPG debt stock indicators available in the IDS database and 
their metadata

Similarly to see all PPG debt service indicators use the `debt_service_indicators` property.

```python
ids.debt_service_indicators
```

To see the date the database was last updated, use the `last_updated` property.

```python
ids.last_updated
```

Caching works the same way as in the base `WorldBank` importer. To clear the cache,
use the `clear_cache` method.

```python
ids.clear_cache()
```

**NOTE** this will clear all cached World Bank data for any other `WorldBank` importers as well.