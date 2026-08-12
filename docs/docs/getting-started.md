# Getting started with `bblocks-data-importers`

This section walks you through the basic steps to install the `bblocks-data-importers` package, load your first dataset, 
and understand how importers work.

## Installation

You can install the data importers package as part of the broader bblocks distribution, or as a standalone package:

```bash title="Option 1: install via bblocks with extras"
pip install bblocks[data-importers]
```

```bash title="Option 2: standalone installation"
pip install bblocks-data-importers
```

## Import Your First Dataset

Once installed, using a data importer is straightforward. Each supported data source—such as the World Bank, IMF, or 
WHO—has its own dedicated importer class with a consistent interface.

Let’s walk through a basic example using the [World Economic Outlook (WEO)](https://www.imf.org/en/Publications/WEO) importer.

### Step 1: Know the data you need

Before using an importer, it’s helpful to know what the dataset contains and where it comes from.
In this case, the World Economic Outlook (WEO) is a flagship publication from the International Monetary Fund (IMF), 
released twice a year. It provides macroeconomic data and forecasts for countries and regions across the globe, making 
it an essential resource for economists, researchers, and policy analysts.

Each bblocks importer includes documentation on the data source, the settings available for the importer 
(such as filters), and how to use the importer effectively. You can refer to the docstrings or the docs in the next 
page for guidance on each importer.

### Step 2: Import package

Each dataset importer in `bblocks-data-importers` has its own dedicated class. To work with World Economic Outlook data,
you’ll need to import the corresponding WEO importer:

```python
from bblocks.data_importers import WEO
```

### Step 3: Instantiate the importer

Now create an instance of the importer:

```python
weo = WEO()
```

At this stage, no data is downloaded yet. Importers are designed to load data lazily, meaning the dataset is only 
fetched when you explicitly request it—typically using `.get_data()`.
This avoids unnecessary memory usage and ensures your code runs efficiently, especially when working 
with large or multiple datasets.

### Step 4: Fetch the data

Use the `get_data` method to get all the data available from the WEO report

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
Importers use caching during a session to avoid unnecessary downloads. To clear the cache manually:

```python
weo.clear_cache()
```
The cache is automatically cleared when the session ends.

<br>
<br>

You're now ready to explore global datasets using a clean, consistent interface—no scraping or manual downloads 
required. Next, see more details about the [data importers available](./importers/index.md) in the package or read about our
[design philosophy](design-philosophy.md).
