"""Protocol for data importers.

All importers in the bblocks.data_importers package should implement this protocol.

Data Importer design is based on the following principles:

- Importers should have a similar interface to make it easy to use them interchangeably, with
similar methods and parameters, but with customization for each data source when needed
- Data should be loaded lazily, meaning that the data should not be loaded until it is required,
i.e., when the get_data() or a similar getter method is called. This is to avoid loading large datasets into memory
when they are not needed.
- Data should be cached to avoid downloading it multiple times. Caching strategy should cater to the source and
the data size. Preferably, no data should be cached in memory and the cache should clear when the session ends.
When the disk is used for caching, it should be made clear to the user.
- Data should be returned in a structured format, with minimal changes to the original data, but with standardized
column names and pyarrow dtypes. The data should be returned as a pandas DataFrame.
"""

from typing import Protocol, runtime_checkable
import pandas as pd


@runtime_checkable
class DataImporter(Protocol):
    """Protocol for data importers.

    Data importers are classes that are responsible for importing data from
    external sources and providing it to user in a structured format. All importer
    classes in the bblocks.data_importers package should implement this protocol.

    The protocol defines the following methods:
    - get_data: get the data from the source as a pandas DataFrame.
    - clear_cache: clear the cache of the data importer.
    """

    def get_data(self) -> pd.DataFrame:
        """Method to return data as a pandas DataFrame."""
        ...

    def clear_cache(self) -> None:
        """Method to clear the cache of the data importer."""
        ...
