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
