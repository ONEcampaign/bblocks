Changelog
=========

[0.2.0] - 2022
--------------

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
-  Added a new feature: ``latest_by()`` which returns the latest value given a series of columns to group by.
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




[0.1.3] - 2022-07-29
--------------

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
