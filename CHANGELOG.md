Changelog
=========
[1.0.0] - 2023-02-07
--------------------
- First major release of bblocks. This introduces changes to the importer classes
  API in order to make things more coherent. As a result, backwards compatibility is not
  guaranteed.

- This release introduces changes to how files are stored. No data is distributed with
  the package anymore. Instead, users are expected to set the working directory to a folder
  where they want to store the data.

[0.5.1] - 2023-01-12
--------------------
- Updated requirements

[0.5.0] - 2023-01-12
--------------------
- Fixed bugs in `unaids` module preventing instantiation of the parent class `ImportData` 
  and handling json responses for regional data.
- Updated the requirements

[0.4.3] - 2022-12-08
--------------------
- Fixed the `GHED` importer given changes in the underlying data structure


[0.4.2] - 2022-12-06
--------------------
- Added a new feature: `who` module in `import_tools` with an object 
  to extract data from GHED dataset.

[0.4.1] - 2022-11-22
--------------------
-  Improved the pink_sheets and sdr scripts.

[0.4.0] - 2022-11-07
--------------------
-  Added a new feature: can now import data from UNAIDS.
   The module is called `unaids` and is located in `import_tools`.

[0.3.0] - 2022-10-28
--------------------
-  Added a new feature: can now import data from the World Bank IDS
   database. The new module is called `ids` and it is part of
   `import_tools.debt`.

[0.2.10] - 2022-10-08
--------------------
-  Fix bug in how years added variables are named in DataFrame add

[0.2.9] - 2022-10-08
--------------------
-  Fix bug in how years are matched under DataFrame add

[0.2.8] - 2022-10-07
--------------------
-  Change how dates are handled when adding to a dataframe

[0.2.7] - 2022-10-04
--------------------
-  Update WFP data handling to append new data instead of replacing.

[0.2.6] - 2022-10-03
--------------------
-  WEO update file path bug fix.

[0.2.5] - 2022-10-03
--------------------
-  Minor bug with file management resolved.

[0.2.4] - 2022-10-03
--------------------
-  Added an optional parameter to importers in order to specify where the data should be stored.

[0.2.3] - 2022-10-01
--------------------
-  Fixed how the ``WorldEconomicOutlook`` object handles moving to the latest data
-  Added new optional parameters to ``WorldEconomicOutlook`` to allow for
   specifying the data version.
-  ``WorldEconomicOutlook`` now has an instance attribute ``version`` that
   stores the version of the data used to create the object.

[0.2.2] - 2022-09-09
--------------------
-  Added a new feature: ``date_to_str()``, which formats a date as an english string.
-  Added a new feature: ``format_number``, which formats a numeric series as a formatted string (e.g. 1,234,567.89
   instead of 1234567.89). Optionally can specify to format as percentage, millions, or billions.
-  Added a new feature: ``latest_sdr_exchange``, which extracts the latest SDR exchange rate information in a dictionary
-  Fixed bug on SDR object - when ``update_data = True`` indicators are loaded in the object when
   ``load_indicator`` is called.

[0.2.1] - 2022-08-16
--------------------
-  Fixed a bug with adding WEO columns to dataframes

[0.2.0] - 2022-08-10
--------------------

-  Added a new feature: ``period_avg()``, which calculates the average of a
   time series/DataFrame for a given period.
-  Added a new module: ``imf``, which contains functions and objects to extract IMF
   data (SDR holdings and allocations).
-  Added a new feature: ``get_dsa()``, which extracts DSA data from the IMF.
-  Added a new feature: ``change_from_date()``, which calculates the nominal
   or percentage change for a column (by a grouper) from/to a given date.
-  Added a new feature: ``append_new_data()``, to read a previously-saved
   DataFrame and append new data to it (removing duplicates by date).
-  Added a new feature: ``WFPData``, class, which is used to download
   *headline* and *food* inflation data, plus *people with insufficient
   food consumption* data, from WFP.
-  Added a new feature: ``get_fao_index()``, which downloads the FAO food
   index data.
-  Added a new feature: ``convert_id()``, which converts a Pandas Series containing
   country IDs (like names or ISO3 codes) to another format (like ISO3 codes).
-  Added a new feature: ``dac_codes``, which is a dictionary with DAC codes for DAC donor countries.
   A future version will add recipient codes.
-  Added a new feature: ``get_population_id()``, which returns a Pandas DataFrame with population data
   for all countries with available World Bank Data. A future version will add the option to choose a
   source for this data.
-  Added a new feature: ``add_population_column()``, which adds a new column containing population data to
   a DataFrame.
-  Added a new feature: ``add_short_names_column()``, which adds a new column containing short country names to
   a DataFrame.
-  Added a new feature: ``add_iso_codes_column()``, which adds a new column containing ISO3 codes to
   a DataFrame.
-  Added a new feature: ``filter_latest_by()`` which returns the latest value given a series of columns to group by.
-  Added a new feature: ``to_date_column()`` which converts a column to a date column.
-  Added a new feature: ``add_poverty_ratio_column()`` which adds a new column containing the poverty ratio to
   a DataFrame, using World Bank data. Note that yearly population data is missing for many countries in many years.
-  Added a new feature: ``add_population_density()`` which adds a new column containing the population density to
   a DataFrame, using World Bank data.
-  Added a new feature: ``add_population_share_column()``, which adds a new column transforming a column with numeric
   values into a new column which shows those values as a share of population.
-  Added a new feature: ``add_median_observation()`` to add the median observation for a given group, either as a
   new column or appended to the end of the dataframe.
-  Added a new feature: ``add_income_level_column()`` to add the World Bank income level for countries as a new column.
-  Added a new feature: ``add_gdp_column()`` and ``add_gdp_share_column()`` to add the GDP information from the
   IMF World Economic Outlook to a DataFrame.
-  Added a new feature: ``add_gov_expenditure_column()`` and ``add_gov_exp_share_column()`` to add government expenditure
   data from IMF World Economic Outlook to a DataFrame.
-  Added a new feature: ``add_flourish_geometries()`` to map the geometries used by Flourish maps to countries, added to
   a DataFrame as a new column.
-  Added a new feature: ``add_value_as_share()`` to add a new column with a value as a share of another column.
-  Added new features: A ``_filter_by()`` helper function to filter a DataFrame based on membership to a specific
   grouping. Additionally ``filter_african_countries()`` to filter a DataFrame to keep only African countries,
   ``filter_eu_countries()`` to filter to keep ony EU members, ``filter_by_un_region()`` to filter by UN regions,
   and ``filter_by_continent()`` to filter countries based on their continent.



[0.1.3] - 2022-07-29
--------------------

-  Added a new feature: ``clean_number()`` which cleans a string and
   returns a float or int.
-  Added a new feature: ``clean_numeric_series()`` which cleans the
   numbers in a pd.Series (or list of series)
-  improved documentation


[0.0.3] - 2022-06-27
--------------------

-  First release on PyPI.


[0.0.2] - 2022-06-27
--------------------

-  Preparation for first release


[0.0.1] - 2022-06-27
--------------------

-  First release on test PyPI.