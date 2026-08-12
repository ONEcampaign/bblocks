# Design philosophy for `bblocks-data-importers`


Development data is uniquely challenging. While some sources provide structured access through APIs or databases, 
much of the most important data is locked away in hard-to-use formats—Excel files, inconsistent CSVs, PDFs, or legacy 
systems. Even when data is programmatically accessible, it often suffers from:

- Inconsistencies and unpredictability in data updates
- Non-standard country and region identifiers
- Poor or missing metadata
- Inconsistent naming conventions for common fields
- Structural differences in how data is organized (e.g., long vs. wide format, different grouping levels)
- 
These issues create friction and fragility in analysis workflows, forcing teams to repeatedly write one-off code 
to clean and standardize each dataset.

<h2> A predictable interface for unpredictable data </h2>

The `bblocks-data-importers` package is designed to tackle this complexity with a clear and consistent interface, 
while still allowing the flexibility needed to handle source-specific quirks.

Every data importer in the package follows a shared protocol that governs how data is accessed, cached, and returned.
This protocol ensures:

- __Familiarity__: All importers expose the same core methods, so once you’ve used one, you can use them all.
- __Lazy loading__: Data isn’t fetched or processed until explicitly requested. This avoids unnecessary downloads or memory use.
- __Intelligent caching__: Data is cached to avoid re-downloading, and the cache can be cleared when needed.
- __Consistent structure__: Data is returned as a tidy, analysis-ready pandas DataFrame, with standardized column names and types across importers.


Key fields — like country codes and names, time periods, values, and metadata—use harmonized naming conventions across 
sources. This consistency reduces the need for custom parsing or transformation logic in downstream analysis.


By following this design, `bblocks-data-importers` allows you to:

- Work confidently across diverse datasets
- Reuse the same downstream logic across sources
- Integrate external data into larger pipelines without brittle workarounds
- Spend less time on data wrangling and more time on analysis and insights

