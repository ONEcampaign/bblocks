# International Debt Statistics (IDS) importer

The `InternationalDebtStatistics` importer is a specialized 
[`WorldBank` data importer](./world-bank.md)
for the IDS database (id=`6`) containing debt stock and flow data.

## About IDS

IDS provides comprehensive annual external debt stocks and flows data for low-
and middle-income countries that report public and publicly guaranteed external
debt to the World Bank's Debtor Reporting System (DRS). 

Visit the IDS [here](https://www.worldbank.org/en/programs/debt-statistics/ids).

## Basic Usage

To start using the IDS importer, instantiate an instance of the importer

```python
from bblocks.data_importers import InternationalDebtStatistics

ids = InternationalDebtStatistics()
```

This object is built on top of the `WorldBank` importer connected to the IDS database
(id=`6`). You can fetch data for indicators by calling the `get_data` method.

```python
principal_data = ids.get_data('DT.AMT.MLAT.CD', years=[2023])

print(principal_data.head())
# Output:
#       year   entity_code  entity_name indicator_code  value       counterpart_code ...
# 0     2023    ZWE         Zimbabwe    DT.AMT.MLAT.CD  0.0         913              ...
# 1     2023    ZMB         Zambia      DT.AMT.MLAT.CD  33223157.7  913              ...
# 2     2023    UGA         Uganda      DT.AMT.MLAT.CD  32692206.2  913              ...
# ...
```

## Get debt stock or service indicators

The import offers convenient functionality to get all indicators and data for debt stocks
and service. 

To see the indicators for debt stocks or service:

```python
# debt service indicators
debt_service_indicators = ids.debt_service_indicators()

# debt stock indicators
ids.debt_stocks_indicators()

print(debt_service_indicators)
# Output:
# {'DT.AMT.BLAT.CD': 'Bilateral',
# 'DT.AMT.MLAT.CD': 'Multilateral',
# 'DT.AMT.PBND.CD': 'Private',
# ...}
```

To fetch all data for debt stock or debt service indicators: 

```python
# get all debt service data
debt_service_data = ids.get_debt_service_data(years=[2022,2023], economies="GTM")

# get all debt stock data
ids.get_debt_stocks_data(detailed_category=True, years=[2023], economies="GTM")
```