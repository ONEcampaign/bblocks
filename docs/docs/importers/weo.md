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
from bblocks.data_importers import WEO

# Create an importer instance
weo = WEO()

# Get all data from the latest release
df = weo.get_data()

# Preview
print(df.head())

# Output:
#       entity_code indicator_code  year  value   unit   indicator_name                    entity_name  ...
# 0     111         NGDP_D          1980  39.372  Index  Gross domestic product, deflator  United States  ...
# 1     111         NGDP_D          1981  43.097  Index  Gross domestic product, deflator  United States  ...
# ...
```


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