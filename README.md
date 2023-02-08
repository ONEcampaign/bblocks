[![pypi](https://img.shields.io/pypi/v/bblocks.svg)](https://pypi.org/project/bblocks/)
[![python](https://img.shields.io/pypi/pyversions/bblocks.svg)](https://pypi.org/project/bblocks/)
[![codecov](https://codecov.io/gh/ONEcampaign/bblocks/branch/main/graph/badge.svg?token=YN8S1719NH)](https://codecov.io/gh/ONEcampaign/bblocks)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# The bblocks package

**bblocks** is a python package with tools to download and analyse
development data. These tools are meant to be the *building blocks* of
further analysis.

We have built **bblocks** to support our work at ONE, but we hope that
it will be useful to others working with development data. We welcome feedback,
feature requests, and collaboration.

**bblocks** is organised around the following main features:

- *Import tools* to help with import data from:
    - The World Bank (building on the wbgapi package)
    - The IMF World Economic outlook (building on the weo package)
    - The IMF data on Special Drawing Rights
    - The World Food Programme (WFP) data on food security and inflation
    - The FAO (notably the price index)
    - The UNDP Human Development Report data
    - UNAIDS
    - The WHO Government Health Expenditure data

- *Cleaning tools* to help with:
    - Cleaning numbers/numeric series
    - Transforming country identifiers (ISO2, ISO3, WB, UN, etc., building on the country_converter package)
    - Transforming text to datetime objects, and datetime objects to text
    - Formatting numbers as text (percentages, millions, billions, etc.)

- *Analysis tools* to help with:
    - Calculating period averages
    - Calculating the change from one period to another

- *DataFrame tools* to help with:
    - Adding a population column to a DataFrame
    - Adding a "share of population" / "per capita" column to a DataFrame
    - Adding a population density column to a DataFrame
    - Adding a GDP column to a DataFrame
    - Adding a "share of GDP" column to a DataFrame
    - Adding a poverty ratio column to a DataFrame
    - Adding a government expenditure column to a DataFrame
    - Adding a "share of government expenditure" column to a DataFrame
    - Adding a "World Bank income level" column to a DataFrame
    - Adding a column with short country names to a DataFrame
    - Adding a column with ISO3 codes to a DataFrame
    - Adding the median observation of a group
    - Adding a column with geojson geometries to a DataFrame

- *Other tools* like:
    - Dictionaries mapping ISO3 codes (and vice-versa) to
        - OECD DAC codes
        - WB income groups
        - geojson geometries
        - G7, EU27, G20 countries
        - Income levels
        - Life expectancy
        - Population

More information is available:

- Documentation: https://bblocks.readthedocs.io/
- GitHub: https://github.com/ONECampaign/bblocks
- PyPI: https://pypi.org/project/bblocks/

## Installation

bblocks can be installed from using pip

```bash
pip install bblocks --upgrade

```

The package is compatible with Python 3.10 and above.

## Basic usage

To get started, import the package. It is strongly recommended that you specify
the path to the folder where you want to store the data.

You only have to do this once per file/notebook.

```python
from bblocks import set_bblocks_data_path

# Set to the folder you want
set_bblocks_data_path("path/to/data/folder")
```

All the examples below assume that you have done this.

### Importing data from the World Bank

```python
from bblocks import WorldBankData

# create a WorldBankData object. This object will allow you
# to download indicators from the World Bank and get them as DataFrames
wb = WorldBankData()

# For example to get "primary completion rate" (SE.PRM.CMPT.ZS) from 2010 to 2020.
# If the data is not already in your data folder, it will be downloaded
wb.load_data(
    indicator="SE.PRM.CMPT.ZS",
    start_year=2010,
    end_year=2020
)

# Get the data as a DataFrame
df = wb.get_data()

# Print a sample of 10 rows
print(df.sample(10))
```

The above would return a DataFrame like this:

| date       | iso\_code | indicator\_code | value     |
|:-----------|:----------|:----------------|:----------|
| 2010-01-01 | LMC       | SE.PRM.CMPT.ZS  | 87.753189 |
| 2012-01-01 | SWZ       | SE.PRM.CMPT.ZS  | 84.697472 |
| 2013-01-01 | NAM       | SE.PRM.CMPT.ZS  | 93.020042 |
| 2012-01-01 | PAK       | SE.PRM.CMPT.ZS  | 63.486210 |
| 2015-01-01 | LIC       | SE.PRM.CMPT.ZS  | 63.463470 |
| 2016-01-01 | BGD       | SE.PRM.CMPT.ZS  | NaN       |
| 2019-01-01 | SYR       | SE.PRM.CMPT.ZS  | NaN       |
| 2013-01-01 | NAC       | SE.PRM.CMPT.ZS  | 99.025703 |
| 2011-01-01 | AND       | SE.PRM.CMPT.ZS  | NaN       |
| 2013-01-01 | GRL       | SE.PRM.CMPT.ZS  | NaN       |

You can also get the latest data (most recent non-empty observation) for one or more indicators:

```python
from bblocks import WorldBankData

# create a WorldBankData object.
wb_data = WorldBankData()

# Load the indicators. If they are not downloaded, they will be
wb_data.load_data(
    indicator=["SH.XPD.CHEX.PC.CD", "SH.XPD.CHEX.GD.ZS"],
    most_recent_only=True
)

# Get the data as a DataFrame
df = wb_data.get_data(indicators="all")

# Print a sample of the data
print(df.sample(10))
```

This would return a DataFrame like this:

| date       | iso\_code | indicator\_code   | value       |
|:-----------|:----------|:------------------|:------------|
| 2019-01-01 | HRV       | SH.XPD.CHEX.PC.CD | 1040.085693 |
| 2019-01-01 | ERI       | SH.XPD.CHEX.GD.ZS | 4.458767    |
| 2019-01-01 | JAM       | SH.XPD.CHEX.PC.CD | 327.403534  |
| 2019-01-01 | MYS       | SH.XPD.CHEX.PC.CD | 436.612030  |
| 2019-01-01 | BHS       | SH.XPD.CHEX.GD.ZS | 5.749775    |
| 2015-01-01 | YEM       | SH.XPD.CHEX.PC.CD | 73.176743   |
| 2019-01-01 | PER       | SH.XPD.CHEX.PC.CD | 370.109955  |
| 2019-01-01 | IDA       | SH.XPD.CHEX.PC.CD | 52.076285   |
| 2019-01-01 | ERI       | SH.XPD.CHEX.PC.CD | 25.267935   |
| 2019-01-01 | WLD       | SH.XPD.CHEX.PC.CD | 1115.008730 |

In all cases, if you had already downloaded the data and you want to update it
you can call `.update_data()` after loading the data in order to refresh it.

```python
wb_data.update_data(reload_data=True)
```

### Importing data from UNAIDS

```python
from bblocks import Aids

# create an Aids object. This object will allow you
# to download indicators from UNAIDS and get them as DataFrames
aids = Aids()

# To view all the indicators that can be downloaded using this tool
# you can use the `.available_indicators` property
aids.available_indicators
```

Her are the first 10 indicators, but over 50 are available:

|     | indicator                                       | category                    |
|:----|:------------------------------------------------|:----------------------------|
| 0   | Trend of new HIV infections                     | Epidemic transition metrics |
| 1   | Trend of AIDS-related deaths                    | Epidemic transition metrics |
| 2   | Incidence:prevalence ratio                      | Epidemic transition metrics |
| 3   | Incidence:mortality ratio                       | Epidemic transition metrics |
| 4   | People living with HIV - All ages               | People living with HIV      |
| 5   | People living with HIV - Children \(0-14\)      | People living with HIV      |
| 6   | People living with HIV - Adolescents \(10-19\)  | People living with HIV      |
| 7   | People living with HIV - Young people \(15-24\) | People living with HIV      |
| 8   | People living with HIV - Adults \(15+\)         | People living with HIV      |
| 9   | People living with HIV - Adults \(15-49\)       | People living with HIV      |

```python
# to load/download indicators, you can use the `.load_data` method
# you can also specify whether to download "country", "region", or "all"
aids.load_data(
    indicator="Trend of AIDS-related deaths",
    area_grouping="region"
)

# get the data as a DataFrame
df = aids.get_data()

# print a sample of 10 rows
print(df.sample(10))
```

| area\_name                                 | area\_id | year | indicator                    | dimension               | value        |
|:-------------------------------------------|:---------|:-----|:-----------------------------|:------------------------|:-------------|
| Global                                     | 03M49WLD | 2013 | Trend of AIDS-related deaths | All ages estimate       | 1.061395e+06 |
| Latin America                              | UNALA    | 2021 | Trend of AIDS-related deaths | All ages estimate       | 2.916500e+04 |
| Middle East and North Africa               | UNAMENA  | 2018 | Trend of AIDS-related deaths | All ages lower estimate | 4.089657e+03 |
| Western & Central Europe and North America | UNAWCENA | 2019 | Trend of AIDS-related deaths | All ages estimate       | 1.305140e+04 |
| Caribbean                                  | UNACAR   | 2021 | Trend of AIDS-related deaths | All ages lower estimate | 4.213485e+03 |
| Middle East and North Africa               | UNAMENA  | 2021 | Trend of AIDS-related deaths | All ages upper estimate | 6.867407e+03 |
| Western & Central Europe and North America | UNAWCENA | 2016 | Trend of AIDS-related deaths | All ages upper estimate | 1.771698e+04 |
| Western & Central Europe and North America | UNAWCENA | 2020 | Trend of AIDS-related deaths | All ages upper estimate | 1.632782e+04 |
| Eastern Europe and Central Asia            | UNAEECA  | 2017 | Trend of AIDS-related deaths | All ages upper estimate | 4.553729e+04 |
| Latin America                              | UNALA    | 2020 | Trend of AIDS-related deaths | All ages upper estimate | 4.577862e+04 |

As with other bblocks tools, you can also get multiple indicators at once
(see the WorldBank example).

In all cases, if you had already downloaded the data and you want to update it
you can call `.update_data()` after loading the data in order to refresh it.

```python
aids.update_data(reload_data=True)
```

### Importing SDR data from the IMF

```python
# Import the SDR object from the sdr module of "import_tools"
from bblocks.import_tools.sdr import SDR

# Create an SDR object
sdr = SDR()

# To view the latest date for which data is available,
# call the `.latest_date()` method
sdr.latest_date()

# To download the latest data
sdr.load_data(date="latest")

# To get the data as a DataFrame. You can specify getting a 
# specific indicator by using 'indicator'. In this case,
# we'll get holdings (allocations are also available)
df = sdr.get_data(indicator="holdings")

# Print a sample of 10 rows
print(df.sample(10))
```

| entity                             | indicator | value        | date       |
|:-----------------------------------|:----------|:-------------|:-----------|
| Samoa                              | holdings  | 1.584296e+07 | 2023-01-31 |
| Iraq                               | holdings  | 3.301367e+07 | 2023-01-31 |
| Lao People\\'s Democratic Republic | holdings  | 5.870183e+07 | 2023-01-31 |
| Haiti                              | holdings  | 9.169516e+07 | 2023-01-31 |
| Bahamas, The                       | holdings  | 1.245326e+08 | 2023-01-31 |
| Total                              | holdings  | 6.606989e+11 | 2023-01-31 |
| Libya                              | holdings  | 3.187335e+09 | 2023-01-31 |
| Namibia                            | holdings  | 1.783556e+08 | 2023-01-31 |
| Tajikistan, Republic of            | holdings  | 1.891507e+08 | 2023-01-31 |
| Malta                              | holdings  | 2.499760e+08 | 2023-01-31 |

In all cases, if you had already downloaded the data and you want to update it
you can call `.update_data()` after loading the data in order to refresh it.

```python
sdr.update_data(reload_data=True)
```

### Adding World Bank income levels to a DataFrame

For this example, we will continue using the SDR data as above.

```python
from bblocks import add_income_level_column

# We can add the column by passing the dataframe to the function

df = add_income_level_column(
    df=df,
    id_column="entity",
    id_type="regex",  # so the text can be matched to the right country
)
```

Which adds the income level column:

| entity                    | indicator | value        | date       | income\_level       |
|:--------------------------|:----------|:-------------|:-----------|:--------------------|
| Montenegro, Republic of   | holdings  | 7.404593e+07 | 2023-01-31 | Upper middle income |
| Gambia, The               | holdings  | 5.857020e+07 | 2023-01-31 | Low income          |
| Suriname                  | holdings  | 1.211070e+08 | 2023-01-31 | Upper middle income |
| Syrian Arab Republic      | holdings  | 5.636629e+08 | 2023-01-31 | Low income          |
| Iran, Islamic Republic of | holdings  | 4.976198e+09 | 2023-01-31 | Lower middle income |
| Uruguay                   | holdings  | 6.330507e+08 | 2023-01-31 | High income         |
| South Africa              | holdings  | 4.424154e+09 | 2023-01-31 | Upper middle income |
| Nigeria                   | holdings  | 3.755370e+09 | 2023-01-31 | Lower middle income |
| Dominican Republic        | holdings  | 4.498683e+08 | 2023-01-31 | Upper middle income |
| Trinidad and Tobago       | holdings  | 7.722810e+08 | 2023-01-31 | High income         |

An optional argument can be passed to the function to redownload the income classification
data from the World Bank.

```python
df = add_income_level_column(
    df=df,
    id_column="entity",
    id_type="regex",
    update_data=True,
)

```

### Adding a GDP share column to a DataFrame

For this example, we will continue working with data on military expenditure downloaded using
the World Bank tool.

```python
# First import the function from the `add` module of `dataframe_tools`
from bblocks.dataframe_tools.add import add_gdp_share_column
from bblocks import WorldBankData

# this data is in local currency units
df = WorldBankData().load_data(indicator="MS.MIL.XPND.CN", most_recent_only=True).get_data()

```

| date       | iso\_code | indicator\_code | value        |
|:-----------|:----------|:----------------|:-------------|
| 2021-01-01 | BDI       | MS.MIL.XPND.CN  | 1.351000e+11 |
| 2014-01-01 | YEM       | MS.MIL.XPND.CN  | 3.685000e+11 |
| 2021-01-01 | AFG       | MS.MIL.XPND.CN  | 2.304000e+10 |
| 2021-01-01 | PER       | MS.MIL.XPND.CN  | 9.086000e+09 |
| 2021-01-01 | AUS       | MS.MIL.XPND.CN  | 4.229595e+10 |

```python

# Then call the function, passing the DataFrame and the column name
df = add_gdp_share_column(
    df=df,
    id_column="iso_code",
    id_type="ISO3",
    date_column="date",  # to match the gdp values with the year of the data
    value_column="value",
    decimals=1,
    usd=False,  # since the data is in local currency units
    include_estimates=True,  # to include official data and IMF estimates for GDP
)

print(df.sample(10))
```

Which returns a dataframe with an extra column "gdp_share".

| date       | iso\_code | indicator\_code | value        | gdp\_share |
|:-----------|:----------|:----------------|:-------------|:-----------|
| 2021-01-01 | GIN       | MS.MIL.XPND.CN  | 2.406750e+12 | 1.5        |
| 2014-01-01 | ARE       | MS.MIL.XPND.CN  | 8.356800e+10 | 5.6        |
| 2021-01-01 | NGA       | MS.MIL.XPND.CN  | 1.783120e+12 | 1.0        |
| 2021-01-01 | GNQ       | MS.MIL.XPND.CN  | 9.439700e+10 | 1.4        |
| 2021-01-01 | ISL       | MS.MIL.XPND.CN  | 0.000000e+00 | 0.0        |
| 2021-01-01 | ESP       | MS.MIL.XPND.CN  | 1.652680e+10 | 1.4        |
| 2021-01-01 | BHR       | MS.MIL.XPND.CN  | 5.194000e+08 | 3.6        |
| 2021-01-01 | GEO       | MS.MIL.XPND.CN  | 9.723000e+08 | 1.6        |
| 2021-01-01 | MDA       | MS.MIL.XPND.CN  | 9.144000e+08 | 0.4        |
| 2013-01-01 | LAO       | MS.MIL.XPND.CN  | 1.782500e+11 | 0.2        |

### Cleaning a numeric series which contains numbers with text

Sometimes dataframes contain columns which don't have clean text. For example,
something like

|     | iso\_code | value   |
|:----|:----------|:--------|
| 0   | USA       | 10%     |
| 1   | GBR       | +12%    |
| 2   | FRA       | 13.4%   |
| 3   | DEU       | %14.3   |
| 4   | ITA       | 15.3  % |
| 5   | ESP       | 16%     |
| 6   | CAN       | 17%     |
| 7   | JPN       | 18%     |
| 8   | AUS       | 19%     |
| 9   | CHN       | 20%     |

bblocks can help clean that data.

```python
from bblocks import clean_numeric_series

df['value'] = clean_numeric_series(
    data=df['value'],
    to=float  # or if dealing with integers, use to=int
)

```

Returns a clean version of the data

|     | iso\_code | value |
|:----|:----------|:------|
| 0   | USA       | 10.0  |
| 1   | GBR       | 12.0  |
| 2   | FRA       | 13.4  |
| 3   | DEU       | 14.3  |
| 4   | ITA       | 15.3  |
| 5   | ESP       | 16.0  |
| 6   | CAN       | 17.0  |
| 7   | JPN       | 18.0  |
| 8   | AUS       | 19.0  |
| 9   | CHN       | 20.0  |


# Contributing
Interested in contributing to the package? Please reach out.