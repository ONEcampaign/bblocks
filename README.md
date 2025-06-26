# bblocks

__Building Blocks for development data work__

[![PyPI](https://img.shields.io/pypi/v/bblocks.svg)](https://pypi.org/project/bblocks/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/bblocks.svg)](https://pypi.org/project/bblocks/)
[![Docs](https://img.shields.io/badge/docs-bblocks-blue)](https://docs.one.org/tools/bblocks/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


`bblocks` is a growing collection of Python packages designed to simplify the everyday work of analysts and 
researchers in the international development sector. Built with usability and modularity in mind, `bblocks` 
provides reliable, reusable components that help standardize and streamline data workflows—from data cleaning 
and transformation to country concordance, dataset importation, and integration with ETL pipelines such as Data Commons.

Whether you're wrangling data for a quick analysis or developing a robust, production-ready 
pipeline, `bblocks` offers practical, tested tools that are easy to plug into your workflow and scale with your needs.

Read the [documentation](https://docs.one.org/tools/bblocks/) for more details on how to use `bblocks` packages 
and the motivation for their creation.

## Packages
`bblocks` is an umbrella package that includes several specialized sub-packages, 
each designed to address specific data tasks.

- [__`bblocks-places`__](https://github.com/ONEcampaign/bblocks-places): Resolve and standardize place names, including countries, regions,
and other geographic entities.
- [__`bblocks-data-importers`__](https://github.com/ONEcampaign/bblocks_data_importers): Tools to import data from different
international development sources such as IMF, World Bank and many others.

## Installation

It’s easy to get started with bblocks. Whether you want the full distribution or only specific 
tools, installation is flexible and straightforward.

### Install the entire `bblocks` toolkit
This is recommended for most users who want access to all the tools in the `bblocks` ecosystem.

```bash
pip install bblocks[all]
```

### Install specific `bblocks` packages

Install only the packages you need without unnecessary dependencies. For example, to install the `places` package:

```bash
pip install bblocks[places]
```

You can also install individual packages directly:

```bash
pip install bblocks-places
```

## Basic Usage

Once installed, you can start using `bblocks` packages in your analysis pipelines. Here's a
quick example of fetching some data using th `bblocks-data-importers` and resolving entity
names with `bblocks-places`:

```python
from bblocks.data_importers import WorldBank
from bblocks import places

# Fetch World Bank data for series "SI.POV.DDAY" (poverty headcount ratio at $3.00 a day)
wb = WorldBank()
df = wb.get_data(series="SI.POV.DDAY")

# Resolve entities to short names
df["country"] = places.resolve_places(df["entity_name"], to="name_short", not_found="ignore")

# filter for African countries
gdp_africa = places.filter_african_countries(df["country"])

# Keep only relevant columns
gdp_africa = gdp_africa.loc[:, ["year", "country", "value",]]

# preview the results
print(gdp_africa.head())
# Output:
#       year    country     value 
# 0     1988    Algeria     12.3
# 1     1995    Algeria     11.8
# 2     2011    Algeria     0
# 3     2000    Angola      27
# ...

```

## Contributing

We welcome contributions to `bblocks`! If you have ideas for new packages, improvements, or bug fixes, 
please check out our [contributing guidelines](https://github.com/ONEcampaign/bblocks/blob/main/CONTRIBUTING.md) 
for details on how to get involved.

