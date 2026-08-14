# bblocks

**Building Blocks for development data work**

[![PyPI](https://img.shields.io/pypi/v/bblocks.svg)](https://pypi.org/project/bblocks/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/bblocks.svg)](https://pypi.org/project/bblocks/)
[![Docs](https://img.shields.io/badge/docs-bblocks-blue)](https://docs.one.org/tools/bblocks/)

`bblocks` is a Python package that helps analysts and researchers in the international
development sector work with data. It provides tools to import data from major international
development sources such as the IMF, World Bank, WHO, and others, with a consistent interface
across sources.

Read the [documentation](https://docs.one.org/tools/bblocks/) for more details on how to use `bblocks`
and the motivation for its creation.

## If you're coming from `bblocks-data-importers` or `bblocks` 2.x

The data importers are moving into `bblocks` itself. In `bblocks` 3.0.0, `bblocks-data-importers`
will be retired, and its final release will be a shim that warns and points you here. `bblocks` 2.x,
which depends on `bblocks-data-importers` and `bblocks-places` as separate packages, is the currently
installable release; PyPI has not yet published `bblocks` 3.x.

Once `bblocks` 3.0.0 is published, update with `pip install -U "bblocks>=3"` and
`pip uninstall bblocks-data-importers`, then change any `from bblocks.data_importers import X` to
`from bblocks import X`. `bblocks` 3.x will no longer depend on `bblocks-data-importers`, so upgrading
alone will leave the old package in place at `bblocks/data_importers/`, where it shadows the importer
classes and warns on every fresh import.

For resolving and standardising place names, use [`resolvekit`](https://github.com/jm-rivera/resolvekit),
a separate package that `bblocks` does not depend on. `bblocks-places` is deprecated; its final release
is 0.0.6.

## Installation

```bash
pip install bblocks
```

The DSA importer (`get_dsa`) parses PDF tables and needs the `pdf` extra:

```bash
pip install "bblocks[pdf]"
```

## Basic Usage

Once installed, you can start using `bblocks` in your analysis pipelines. Here's a
quick example of fetching World Bank data:

```python
from bblocks import WorldBank

# Fetch World Bank data for indicator "SI.POV.DDAY" (poverty headcount ratio at $3.00 a day)
wb = WorldBank()
df = wb.get_data(indicator_code="SI.POV.DDAY", include_labels=True)

# preview the results
print(df.head())
```

To resolve entity names to a standardised form, use [`resolvekit`](https://github.com/jm-rivera/resolvekit),
a separate package not used internally by `bblocks`. `bulk()` needs its `pandas` extra:

```bash
pip install "resolvekit[pandas]"
```

```python
import resolvekit as rk

df["iso3"] = rk.bulk(values=df["entity_name"], to="iso3")
```

## Contributing

We welcome contributions to `bblocks`! If you have ideas for improvements or bug fixes,
please check out our [contributing guidelines](https://github.com/ONEcampaign/bblocks/blob/main/CONTRIBUTING.md)
for details on how to get involved.
