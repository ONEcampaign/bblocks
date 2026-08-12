# Global Health Expenditure Database (GHED) importer

The `GHED` importer provides structured access to the Global Health Expenditure Database published and maintained
by the World Health Organization (WHO). 

## About GHED


The Global Health Expenditure Database (GHED) provides comprehensive data on health expenditures across
countries, allowing for detailed analysis and comparison of health spending patterns since 2000. The database
contains detailed breakdowns of health expenditure including by health care functions, 
by diseases and conditions, spending for the under 5-year-old population, and spending by provider type, as well as
capital investments.

Visit the [GHED website](https://apps.who.int/nha/database) for more information.

## Basic Usage

To start using the importer, instantiate the importer and use the get_data method to fetch all the data in
the database

```py
from bblocks.data_importers import GHED

# create an instance of the GHED importer
ghed = GHED()

# fetch all data from the database
data = ghed.get_data()

# preivew the first few rows of the data
print(data.head())

# Output:
#       country_name	iso3_code	year	indicator_code	value	indicator_name	                unit	
# 0	    Algeria	        DZA	        2000	che_gdp	        3.214	Current Health Expenditure ...	Percentage	
# 1	    Algeria	        DZA	        2001	che_gdp	        3.536	Current Health Expenditure ...	Percentage	
# ...
```

## Access metadata

To access the metadata of the GHED database, you can use the `get_metadata` method. This will return a DataFrame
containing the metadata of the database, including information about the indicators, countries, and years.

```py
metadata = ghed.get_metadata()

# preview the first few rows of the metadata
print(metadata.head())

# Output:
#	    country_name	iso3_code	indicator_name	                indicator_code	sources		                                data_type	                ....
# 0	    Algeria	        DZA	        Current health expenditure ... 	fs	            2019 - 2022: Sum of its components ...		2019 - 2022: Derived ...	                        
# 1	    Algeria	        DZA	        Transfers from government ...	fs1	            2000 - 2018: Calculated as the ...          2000 - 2018: Estimated...
# ...
```

You can also get information about the available indicators by calling the `get_indicators` method. This will return
a DataFrame containing the list of indicators available in the GHED database.

```py
indicators = ghed.get_indicators()

# preview the first few rows of the indicators
print(indicators.head())

# Output:
#	    indicator_code	indicator_name	                                                    indicator_long_code	category_1	category_2	unit	    currency	measurement_method
# 0	    che_gdp	        Current Health Expenditure (CHE) as % Gross Domestic Product (GDP)	CHE%GDP_SHA2011	    INDICATORS	AGGREGATES	Percentage		        Current Health Expenditure (CHE) / Gross Domestic Product (GDP)
# 1	    che_pc_usd	    Current Health Expenditure (CHE) per Capita in US$	                CHE_pc_US$_SHA2011	INDICATORS	AGGREGATES	Ones	    US$	        Current Health Expenditure (CHE) / Population / Exchange rate (NCU to USD)
# ...
```


## Data caching

Data is cached to avoid repeated downloads and to improve performance. The cached data is tied to the importer
instance and cleared automatically when the session ends. You can also manually clear the cache
using the `clear_cache` method:

```py
ghed.clear_cache()
```

## Use locally downloaded data

The Global Health Expenditure Database (GHED) platform allows users to bulk download the data as an Excel file. However,
this file might be too large to handle in Excel or other spreadsheet applications. You can load the downloaded file
into a `GHED` importer instance by providing the path to the file when creating the importer. This will allow you
to work with the data without needing to download it again.

```py
ghed = GHED(data_file='path/to/your/downloaded/ghed_data.xlsx')

# use the importer as usual
data = ghed.get_data()
metadata = ghed.get_metadata()
```

You can also export the data from the object to disk as an Excel format using the `export_raw_data` method:

```py
ghed.export_raw_data(directory = 'directory/to/save/ghed_data')
```

