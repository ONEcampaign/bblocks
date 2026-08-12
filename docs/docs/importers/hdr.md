# Human Development Index (HDI) Importer

The HumanDevelopmentIndex importer provides structured access to indicators in the United Nations Development 
Programme’s (UNDP) Human Development Report, including the Human Development Index (HDI) and its complementary measures.

## About the Human Development Report

The Human Development Report (HDR), published annually by UNDP offers key indicators tracking human development. 
At the center of the HDR is the Human Development Index (HDI) — a composite measure of average achievement in 
key dimensions of human development: a long and healthy life, being knowledgeable and having a decent standard of living. 

In addition to HDI, the HDR also publishes measures that capture broader dimensions of human 
development, identify groups falling behind in human progress and monitor the distribution of human development. These 
include Inequality-adjusted Human Development Index (IHDI), Gender Inequality Index (GII), Gender Development Index 
(GDI) and Planetary pressures-adjusted HDI (PHDI), among others.

For more information visit the [Human Development Reports website](https://hdr.undp.org/).

## Basic usage

To start using the importer, instantiate the importer and use the `get_data` method to get 
the latest data from the Human Development Report.

```python
from bblocks.data_importers import HumanDevelopmentIndex

# Create an importer instance
hdi = HumanDevelopmentIndex()

# Get the latest data
df = hdi.get_data()

# Preview
print(df.head())

# Output: 
#   entity_code  entity_name region_code  ...  value  year  indicator_name
# 0         AFG  Afghanistan          SA  ...  182.0  2022        HDI Rank
# 1         ALB      Albania         ECA  ...   74.0  2022        HDI Rank
# ...
```

This will return a DataFrame containing the data for all available indicators in the latest Human Development Resport
including HDI and all associated measures and components.

## Access metadata

The `get_metadata()` method provides information about the indicators and components featured in the data.

```python
metadata = hdi.get_metadata()
```

## Data caching
The data and metadata are cached to avoid repeated downloads within a session. Cached data is tied to the importer 
instance and cleared automatically when the session ends. You can also manually clear the cache if needed.

```python
hdi.clear_cache()
```