# bblocks-data-importers

__Seamless Pythonic access to international development data__

[![PyPI](https://img.shields.io/pypi/v/bblocks_data_importers.svg)](https://pypi.org/project/bblocks_data_importers/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/bblocks_data_importers.svg)](https://pypi.org/project/bblocks_data_importers/)
[![Docs](https://img.shields.io/badge/docs-bblocks-blue)](https://docs.one.org/tools/bblocks/data-importers/)
[![codecov](https://codecov.io/gh/ONEcampaign/bblocks_data_importers/branch/main/graph/badge.svg?token=YN8S1719NH)](https://codecov.io/gh/ONEcampaign/bblocks_data_importers)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


In any data analysis workflow, the first and often most time-consuming step
is accessing the data—especially when it comes from multiple external sources, 
each with its own formats, APIs, and quirks. And international development data is
as messy as it comes.

The `bblocks-data-importers` package simplifies this process by providing a unified 
interface to download and work with datasets from major international development 
institutions such as the World Bank, IMF, UN agencies, WHO, and more. Instead of writing 
custom code for each source, you get consistent, analysis-ready outputs with minimal setup.

__Key features__:

- Access trusted global datasets from providers like the World Bank, IMF, WHO and others
- Get structured, tidy outputs ready for analysis in tools like pandas
- Avoid source-specific issues with built-in error handling and validation
- Use a consistent interface across all data sources for simpler, cleaner code

Whether you're building dashboards, developing policy briefs, or running large-scale analysis, 
`bblocks-data-importers` removes the friction from the very first step—getting the data—so 
you can focus on what matters most: understanding it.

Read the [documentation](https://docs.one.org/tools/bblocks/data-importers/) for more details. 

## Installation

The package can be installed in various ways

```bash
pip install bblocks-data-importers
```

Or install the main `bblocks` package with an extra:

```bash
pip install bblocks[data-importers]
```



## Usage

Once installed, using a data importer is straightforward. Each supported 
data source—such as the World Bank, IMF, or WHO—has its own dedicated importer 
class with a consistent interface.

Let’s walk through a basic example using the World Economic Outlook (WEO) importer.

### Step 1: Know the data you need

Before using an importer, it’s helpful to know what the dataset contains 
and where it comes from. In this case, the World Economic Outlook (WEO) is a 
flagship publication from the International Monetary Fund (IMF), released twice 
a year. It provides macroeconomic data and forecasts for countries and regions across
the globe, making it an essential resource for economists, researchers, and policy analysts.

Each bblocks importer includes documentation on the data source, the settings available 
for the importer (such as filters), and how to use the importer effectively. You can refer 
to the docstrings or the docs in the next page for guidance on each importer.

### Step 2: Import the package

Each dataset importer in `bblocks-data-importers` has its own dedicated class. 
To work with World Economic Outlook data, you’ll need to import the corresponding `WEO` importer:

```python
from bblocks.data_importers import WEO
```


### Step 3: Instantiate the importer

Now create an instance of the importer:

```python
weo = WEO()
```

At this stage, no data is downloaded yet. Importers are designed to load data lazily, meaning 
the dataset is only fetched when you explicitly request it—typically using `.get_data()`. 
This avoids unnecessary memory usage and ensures your code runs efficiently, especially when
working with large or multiple datasets.

### Step 4: Fetch the data

Use the get_data method to get all the data available from the WEO report

```python
df = weo.get_data()

# Preview the first few rows
df.head()

# Output:
#       entity_code indicator_code  year  value   unit   indicator_name                    entity_name    ...
# 0     111         NGDP_D          1980  39.372  Index  Gross domestic product, deflator  United States  ...
# 1     111         NGDP_D          1981  43.097  Index  Gross domestic product, deflator  United States  ...
# 2     111         NGDP_D          1982  45.760  Index  Gross domestic product, deflator  United States  ...
# 3     111         NGDP_D          1983  48.312  Index  Gross domestic product, deflator  United States  ...
# 4     111         NGDP_D          1984  50.920  Index  Gross domestic product, deflator  United States  ...
```

### Step 5: Clear the cache (optional)

Importers use caching during a session to avoid unnecessary downloads. 
To clear the cache manually:

```python
weo.clear_cache()
```

The cache is automatically cleared when the session ends.



You're now ready to explore global datasets using a clean, consistent 
interface—no scraping or manual downloads required. 

__[Read the documentation](https://docs.one.org/tools/bblocks/data-importers/)
to see all available importers and learn about all the functionality and our
design philosophy.__




## Contributing

Contributions are welcome! Please see the 
[CONTRIBUTING page ](https://github.com/ONEcampaign/bblocks_data_importers/blob/main/CONTRIBUTING.md)
for details on how to get started, report bugs, fix issues, and submit enhancements.