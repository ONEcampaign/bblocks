# Changelog

Releases of the data importers from before they became part of `bblocks`.
Releases of `bblocks` itself are recorded in the
[bblocks changelog](https://github.com/ONEcampaign/bblocks/blob/main/CHANGELOG.md).

## v0.5.0 (2026-01-30)
- Refactor of World Bank importers
- Fix World Bank cache not persisting across sessions (WAL not checkpointed on exit)
- Add cache hit logging for World Bank data requests
- New data importer:
  - `BACI` to access BACI harmonized international trade data
  - IMF Debt Sustainability Assessments (DSA). This importer function downloads and parses the DSA PDF from the IMF website into a Pandas DataFrame

## v0.4.1 (2025-07-16)
- Bug fix in utilities for dependency logging settings

## v0.4.0 (2025-06-26) (latest version)
- Refactor of the package as a namespace package in the `bblocks` namespace

## v0.3.0 (2025-04-07)
- New data importer - `HumanDevelopmentIndex` for UNDP Human Development Index data

## v0.2.1 (2024-12-24)
- Made pyarrow versions more flexible for broader compatibility

## v0.2.0 (2024-12-24)
- fix field names in WEO importer - changed misleading column name `estimates_start_year` to `last_actual_date`

## v0.1.1 (2024-12-06)
- Fix bug in `GHED` importer from changed column names in December 2024 GHED data update

## v0.1.0 (2024-12-06)
- Add importers for
  - World Economic Outlook (WEO) data
  - Global Health Expenditure Database (GHED) data
  - World Food Programme (WFP) data
  - World Bank data (general) and International Debt Statistics (IDS) data

## v0.0.1 (2024-08-22)

- First release of `bblocks_data_importers`!