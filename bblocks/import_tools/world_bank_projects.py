"""World Bank Projects Database Importer"""

import pandas as pd
import numpy as np
import requests
import json
from dataclasses import dataclass

from bblocks.logger import logger
from bblocks.import_tools.common import ImportData
from bblocks.config import BBPaths


class EmptyDataException(Exception):
    """Exception raised when the API response does not contain any data."""

    pass


BASE_API_URL = "http://search.worldbank.org/api/v2/projects"


class QueryAPI:
    """Helper class for querying the World Bank Projects API"""

    def __init__(
            self, response_format: str = 'json', max_rows_per_response: int = 500,
            start_date: str | None = None, end_date: str | None = None
            ):
        """Initialize QueryAPI object"""

        self.response_format = response_format
        self.max_rows_per_response = max_rows_per_response
        self.start_date = start_date
        self.end_date = end_date

        self._params = {
            'format': self.response_format,
            'rows': self.max_rows_per_response,
            # 'os': 0, # offset
            'strdate': self.start_date,
            'enddate': self.end_date
        }

        self._check_params()

        self.response_data = {}  # initialize response_data as empty dict

    def _check_params(self) -> None:
        """Check parameters"""

        # if end_date is before start_date, raise error
        if self._params['strdate'] is not None and self._params['enddate'] is not None:
            if self._params['enddate'] < self._params['strdate']:
                raise ValueError("end date must be after start date")

        # if max_rows is greater than 1000, raise error
        if self._params['rows'] > 1000:
            raise ValueError("max_rows must be less than or equal to 1000")

        # if dates are None, drop them from params
        if self._params['strdate'] is None:
            # drop start_date from params
            self._params.pop('strdate')

        if self._params['enddate'] is None:
            # drop end_date from params
            self._params.pop('enddate')

    def _request(self) -> dict:
        """Single request to API. Returns the rsponse json."""

        try:
            response = requests.get(BASE_API_URL, params=self._params)
            response.raise_for_status()
            data = response.json()['projects']  # keep only the projects data

            return data

        except Exception as e:
            raise Exception(f"Failed to get data: {e}")

    def request_data(self) -> 'QueryAPI':
        """Request data from API

        This method will request all the data from the API
        and store it in the response_data attribute.
        It will automatically determine the request to make
        based on the offset and number of rows parameters.

        Returns:
            'QueryAPI' to allow chaining of methods
        """

        self._params['os'] = 0  # reset offset to 0

        while True:

            # request data
            data = self._request()

            # if there are no more projects, break
            if len(data) == 0:
                break

            # add data to response_data
            self.response_data.update(data)

            # update offset
            self._params['os'] += self._params['rows']

        # Log if no data was returned from API
        if len(self.response_data) == 0:
            raise EmptyDataException("No data was returned from API")

        return self

    def get_data(self) -> dict[dict]:
        """Get the data, or request it if it hasn't been requested yet"""

        if len(self.response_data) == 0:
            self.request_data()

        return self.response_data


fields = {
    'id': 'id',
    'regionname': 'region',
    'project_name': 'project name',
    'countryshortname': 'country',
    'projectstatusdisplay': 'project status',




    'curr_total_commitment': 'total commitment',
    'curr_ibrd_commitment': 'IBRD commitment',
    'curr_ida_commitment': 'IDA commitment',

}


# df = pd.DataFrame.from_dict(proj._raw_data, orient='index')


@dataclass
class WorldBankProjects(ImportData):
    """World Bank Projects Database Importer"""

    start_date: str | None = None
    end_date: str | None = None

    @property
    def _path(self):
        """Generate path based on version"""

        start_date = f'_{self.start_date}' if self.start_date is not None else ''
        end_date = f'_{self.end_date}' if self.end_date is not None else ''

        return BBPaths.raw_data / f"world_bank_projects{start_date}{end_date}.json"

    def load_data(self, project_codes: str | list = 'all') -> ImportData:
        """ """

        # if file does not exist, download it and save it as a json file
        if not self._path.exists():
            with open(self._path, 'w') as file:
                data = (QueryAPI(start_date=self.start_date, end_date=self.end_date)
                        .request_data()
                        .get_data()
                        )
                json.dump(data, file)
                logger.info(f"Successfully downloaded World Bank Projects")

        with open(self._path, "r") as file:
            self._raw_data = json.load(file)

        if project_codes == 'all':
            self._data = self._raw_data

        if isinstance(project_codes, str):
            project_codes = [project_codes]

        if isinstance(project_codes, list):
            self._data = {k: v for k, v in self._raw_data.items()
                          if k in project_codes}

        if self._data == {}:
            raise ValueError("No projects found with the given project codes")
        logger.info(f"Successfully loaded World Bank Projects")

        return self

    def update_data(self, reload: bool = True) -> ImportData:
        """ """

        pass

    def get_data(self):
        """ """

        print('test')
