# UNAIDS importer

The `UNAIDS` importer fetches HIV/AIDS data from UNAIDS.

## About UNAIDS

UNAIDS leads the most extensive collection of data on HIV epidemiology, programme coverage, and finance, in 
collaboration with UNICEF and the WHO. It published the most authoritative data and information on the HIV epidemic.

Visit the [UNAIDS data portal](https://aidsinfo.unaids.org/) for more information and to explore the data.

## Basic usage

To start using the importer, instantiate the importer and use the `get_data` method to get the latest UNAIDS data.

```python
from bblocks.data_importers import UNAIDS

# Create an importer instance
unaids = UNAIDS()

# Get the data
df = unaids.get_data()

# Preview
print(df.head())

# Output:
#	indicator_name	                    unit	subgroup	        entity_name	    entity_code	year	source	value	    value_formatted	footnote
# 0	AIDS mortality per 1000 population	Rate	Females estimate	All countries	03M49WLD	1990	UNAIDS_Estimates_	0.070517	                                          0.070517	
# 1	AIDS mortality per 1000 population	Rate	Females estimate	All countries	03M49WLD	1991	UNAIDS_Estimates_	0.089257	                                          0.089257
```

!!! warning "SSL certificate verification"
    The UNAIDS data portal currently has an SSL certificate verification issue. By default 
    the `UNAIDS` importer will not verify the SSL certificate. If you want to enable SSL certificate verification, 
    you can set the `verify_ssl` parameter to `True` when creating the importer instance

## Selecting a dataset

UNAIDS provides multiple datasets. By default, the `UNAIDS` importer will fetch the most common dataset, which is the
HIV/AIDS Estimates dataset containing the latest estimates of HIV/AIDS indicators such as prevalence, incidence, 
and mortality. The available datasets include:
- `Estimates`
- `Laws and Policies`
- `Key Populations`
- `GAM` (Global AIDS Monitoring)

To fetch data for a specific dataset, you can pass the `dataset` parameter when calling the `get_data` method.

```python title="Get data from a specific dataset"
df = unaids.get_data(dataset="Laws and Policies")  # (1)!

# Output:
#	    indicator_name	                                                unit	            subgroup	                    entity_name	entity_code	year	source	        value
# 0	    3-test strategy/algorithm for an HIV-positive diagnosis used	SingleChoiceWithNo	From national authorities Total	Afghanistan	AFG	        2022	WHO_NCPI_2022	Yes	
# 1	    3-test strategy/algorithm for an HIV-positive diagnosis used	SingleChoiceWithNo	From national authorities Total	Afghanistan	AFG	        2024	WHO_NCPI_2024	Yes	
# ... 
```

1. Accepted values for the `dataset` parameter are: `Estimates`, `Laws and Policies`, `Key Populations`, `GAM`.


## Data caching

The `UNAIDS` importer caches the data to avoid unnecessary API calls. The cached data is tied to the importer instance 
and cleared automatically when the session ends. You can also manually clear the cache whenever you need.

```python
unaids.clear_cache()
```