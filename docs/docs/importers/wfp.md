from bblocks.data_importers import WFPFoodSecurity

# World Food Programme (WFP) Importers

The `WFPInflation` and `WFPFoodSecurity` importers provide access to data on inflation and food security published by 
the World Food Programme (WFP).

## About the databases

**Inflation** data is available for various countries and indicators and can be accessed through the 
[VAM data portal](https://dataviz.vam.wfp.org/economic/inflation).

**Food security** data is available at the national and subnational levels and can be accessed through the WFP's 
[Hunger Map tool](https://hungermap.wfp.org).

## Basic usage

### Inflation

Instantiate the object and call the `get_data()` method to retrieve a pandas DataFrame. It is recommended to specify 
`indicators` and `countries` to reduce wait time. If no arguments are passed, the returned data will 
contain all indicators and countries.

```python
from bblocks.data_importers import WFPInflation

# Create an importer instance
wfp_infl = WFPInflation()

# Get the data for specific indicators and countries
df_infl = wfp_infl.get_data(
    indicators = "Headline inflation (YoY)", 
    countries = ["KEN", "UGA"] # use ISO3 codes to retrieve countries
)

# Preview
print(df_infl.head())

# Output:
#    data                 value  source             indicator_name            iso3_code  country_name  unit           
# 0  2025-05-31 00:00:00  3.8    Trading Economics  Headline inflation (YoY)  UGA        Uganda        percent
# 1  2025-04-30 00:00:00  3.5    Trading Economics  Headline inflation (YoY)  UGA        Uganda        percent
# ...
```

To view the available indicators, use the `available_indicators` property

```python
indicators_infl = wfp_infl.available_indicators
```

### Food security

Instantiate the object and call the `get_data()` method to retrieve a pandas DataFrame. You can optionally filter by 
country using ISO3 codes and chose data at the national or subnational level. 

```python
from bblocks.data_importers import WFPFoodSecurity

# Create an importer instance
wfp_fs = WFPFoodSecurity()

# Get the data for specific countries
df_fs = wfp_fs.get_data(
    countries = ["KEN", "UGA"], # use ISO3 codes to retrieve countries
    level = "subnational" # defaults to "national"
)

# Preview
print(df_fs.head())

# Output
#    date                 value     value_upper  value_lower  iso3_code  country_name  indicator_name                             source
# 0  2024-06-14 00:00:00  18095724  19991955     16601037     UGA        Uganda        people with insufficient food consumption  World Food Programme
# 1  2024-06-15 00:00:00  18111164  19992427     16605079     UGA        Uganda        people with insufficient food consumption  World Food Programme
# ...
```

To view the available countries, use the `available_countries` property

```python
countries_fs = wfp_fs.available_countries
```

## API request settings

### Inflation

You may set the timeout for specific requests (in seconds) when creating an importer instance. The default value is `20`.

```python
wfp_infl = WFPInflation(timeout = 30)
```

### Food security
You may set both `timeout` (in seconds) and `retries` (number of retry attempts on failure). Defaults are `20` and `2`
respectively.

```python
wfp_fs = WFPFoodSecurity(timeout = 30, retries = 3)
```

## Data caching

Data is cached per importer instance to prevent repeated downloads. The cache is cleared automatically at the end of the
session, but you can also clear it manually:

```python
wfp_infl.clear_cache()
wfp_fs.clear_cache()
```