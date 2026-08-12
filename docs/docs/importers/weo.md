# World Economic Outlook (WEO) Importer

The `WEO` importer provides structured access to macroeconomic indicators from the World Economic Outlook
database published by the International Monetary Fund (IMF).

## About the WEO database

The World Economic Outlook (WEO) is a flagship publication of the International Monetary Fund (IMF), released twice
a year, generally in April and October. It provides historical data and forecasts for key economic indicators such as:

- GDP, growth rates, and deflators
- Inflation
- Trade balances
- Public debt and fiscal indicators
- Commodity prices

The WEO database includes data for over 190 countries and regions, making it a central resource for economic analysis,
forecasting, and global comparisons.

The data is made available as Excel files or in SDMX (Statistical Data and Metadata eXchange)
format. The `WEO` importer fetches the latest
data in the SDMX format. However, SDMX data releases begin in April 2017, so the importer only supports data from
that date onwards.

Visit the WEO database [here](https://www.imf.org/en/Publications/WEO/weo-database)

## Basic usage

To start using the importer, instantiate the importer and use the `get_data` method to get the latest WEO data.

```python
from bblocks import WEO

# Create an importer instance
weo = WEO()

# Get all data from the latest release
df = weo.get_data()

# Preview
print(df.head())

# Output:
#       entity_code indicator_code  year  value   unit   indicator_name                    entity_name  ...
# 0     USA         NGDP_D          1980  39.372  Index  Gross domestic product, deflator  United States  ...
# 1     USA         NGDP_D          1981  43.097  Index  Gross domestic product, deflator  United States  ...
# ...
```

## Entity codes

`entity_code` holds the ISO3 code for the 196 countries in the WEO database (e.g. `USA`). Aggregates, such as
the World or the euro area, get a synthetic code prefixed with `G` (e.g. `G001` for World, `G163` for the euro
area). `entity_code` is always populated by default.

Each row also carries `imf_code`, the legacy numeric IMF area code that `entity_code` used before this ISO3
convention (e.g. `111` for the United States). `imf_code` is null for the small number of entities that never
had a legacy code, such as Liechtenstein.

```python title="Look up entity_code and imf_code"
df = weo.get_data()
df[["entity_code", "imf_code", "entity_name"]].drop_duplicates()

# Output:
#     entity_code  imf_code  entity_name
# 0   USA          111       United States
# 1   G001         1         World
```

`imf_code` mirrors a compatibility column that the `imf-reader` library documents as due for removal in its
next major version, so treat it as a short-term convenience rather than a stable field.

### Restoring the old numeric codes

Pipelines built against the previous numeric `entity_code` can keep working with a one-line change. Pass
`legacy_entity_codes=True` when creating the importer, and `entity_code` is populated from the legacy numeric
code instead of the ISO3 code.

```python title="Restore the pre-3.0 numeric entity_code"
weo = WEO(legacy_entity_codes=True)
df = weo.get_data()

# entity_code is now the legacy numeric code, e.g. 111 for the United States
```

This flag is a migration aid built on the same `imf_code` column described above, so it stops working once
`imf-reader` removes that column. It also inherits `imf_code`'s gap: entities with no legacy numeric code, such
as Liechtenstein, get a null `entity_code` under this flag. The importer logs a warning naming the affected
entities.

## Specifying a version

By default, the `get_data` method will return the data from the latest released report. You can also specify a
specific particular release. Generally, the WEO report is released twice a year in April and October. Specify the
version by passing the month and year of the release as a tuple.

```python title="Get data from a specific release"
df = weo.get_data(version = ("April", 2023)) # (1)!
```

1.  Accepted values for the `version` parameter are tuples of the form `(month, year)`, where `month` is either
    `"April"` or `"October"` and `year` is a four-digit year. For example, to get data from April 2023, use
    `("April", 2023)`. You can also use `"latest"` to get the most recent data without specifying a version. By
    default, the importer fetches the latest available data, without needing to specify a version or "latest".

Supported versions include both April and October editions from past years, starting from April 2017 where SDMX
data is available.

## Data caching

The data is cached to avoid repeated downloads within a session. Cached data is tied to the importer instance
and cleared automatically when the session ends. You can also manually clear the cache whenever you need.

```python
weo.clear_cache()
```
