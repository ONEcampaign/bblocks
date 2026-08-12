# Available Data importers

The bblocks-data-importers package provides structured access to a growing set of 
international development data sources. Each importer is designed to offer a consistent 
interface while adapting to the quirks of each source.

## Supported Data Sources

| Source                                                  | Description                                                                   |
|---------------------------------------------------------|-------------------------------------------------------------------------------|
| __IMF [World Economic Outlook](./weo)__                 | Macroeconomic indicators and forecasts from the IMF                           |
| __IMF [Debt Sustainability Analysis](./dsa)__           | Latest IMF LIC debt sustainability assessments in tidy tabular form           |
| __[World Bank](./world-bank)__                          | Comprehensive development data across sectors                                 |
| __World Bank - [International Debt Statistics](./ids)__ | Comprehensive external debt data from the World Bank                          |
| __WHO - [Global Health Expenditure Database](./ghed)__  | Comprehensive country health expenditure data                                 |
| __[UNAIDS](./unaids)__                                  | Extensive data on the HIV epidemic                                            |
| __UNDP - [Human Development Report](./hdi)__            | Insights on key dimensions of human development                               |
| __[WFP - Inflation and food security](./wfp)__          | Key inflation and food security data from WFP VAM and the HungerMap platform] |   


## Upcoming Importers

| Source             | Description                                                                                    |
|--------------------|------------------------------------------------------------------------------------------------|
| CEPII - BACI trade | Harmonized trade data                                                                          |


## See Also

There are many other tools being developed to import data from different development sources. We have also developed
standalone packages for some data sources which require more complex data processing.

See:

- [unesco-reader](https://github.com/lpicci96/unesco_reader) - A package to fetch UNESCO UIS data
- [oda-reader](https://github.com/ONEcampaign/oda_reader) - A package to fetch official development assistance (ODA) 
data from the OECD




## Suggest a new data source

Don’t See a Dataset You Need?
We're continuously expanding support for new sources. If you work with a dataset that isn’t yet covered—or if you’d 
like to help build support for it—we’d love to hear from you!

👉 __Open an [issue on GitHub](https://github.com/ONEcampaign/bblocks_data_importers/issues), and we can work 
together to develop the tooling you need.__

Your contributions and feedback help make bblocks better for everyone in the development data community.
