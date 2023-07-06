"""World Bank Projects Database Importer"""

import pandas as pd
import numpy as np
import requests
import json
from dataclasses import dataclass

from bblocks.logger import logger
from bblocks.import_tools.common import ImportData
from bblocks.config import BBPaths
from bblocks.cleaning_tools import clean


class EmptyDataException(Exception):
    """Exception raised when the API response does not contain any data."""

    pass


BASE_API_URL = "https://search.worldbank.org/api/v2/projects"


class QueryAPI:
    """Helper class for querying the World Bank Projects API"""

    def __init__(
        self,
        response_format: str = "json",
        max_rows_per_response: int = 500,
        start_date: str | None = None,
        end_date: str | None = None,
    ):
        """Initialize QueryAPI object"""

        self.response_format = response_format
        self.max_rows_per_response = max_rows_per_response
        self.start_date = start_date
        self.end_date = end_date

        self._params = {
            "format": self.response_format,
            "rows": self.max_rows_per_response,
            # 'os': 0, # offset
            "strdate": self.start_date,
            "enddate": self.end_date,
        }

        self._check_params()

        self.response_data = {}  # initialize response_data as empty dict

    def _check_params(self) -> None:
        """Check parameters"""

        # if end_date is before start_date, raise error.
        if self._params["strdate"] is not None and self._params["enddate"] is not None:
            if self._params["enddate"] < self._params["strdate"]:
                raise ValueError("end date must be after start date")

        # if max_rows is greater than 1000, raise error
        if self._params["rows"] > 1000:
            raise ValueError("max_rows must be less than or equal to 1000")

        # if dates are None, drop them from params
        if self._params["strdate"] is None:
            # drop start_date from params
            self._params.pop("strdate")

        if self._params["enddate"] is None:
            # drop end_date from params
            self._params.pop("enddate")

    def _request(self) -> dict:
        """Single request to API. Returns the response json."""

        try:
            response = requests.get(BASE_API_URL, params=self._params)
            response.raise_for_status()
            data = response.json()["projects"]  # keep only the projects data

            return data

        except Exception as e:
            raise Exception(f"Failed to get data: {e}")

    def request_data(self) -> "QueryAPI":
        """Request data from API

        This method will request all the data from the API
        and store it in the response_data attribute.
        It will automatically determine the request to make
        based on the offset and number of rows parameters.

        Returns:
            'QueryAPI' to allow chaining of methods
        """

        self._params["os"] = 0  # reset offset to 0

        while True:

            # request data
            data = self._request()

            # if there are no more projects, break
            if len(data) == 0:
                break

            # add data to response_data
            self.response_data.update(data)

            # update offset
            self._params["os"] += self._params["rows"]

        # Log if no data was returned from API
        if len(self.response_data) == 0:
            raise EmptyDataException("No data was returned from API")

        return self

    def get_data(self) -> dict[dict]:
        """Get the data, or request it if it hasn't been requested yet."""

        if len(self.response_data) == 0:
            self.request_data()

        return self.response_data


def clean_theme(data: dict) -> list[dict] | list:
    """Clean theme data from a nested list to a list of dictionaries with theme names and
    percentages.
    If there are no themes, an empty list will be returned.

    Args:
        data: data from API

    Returns:
        list of dictionaries with theme names and percentages
    """

    # if there are no themes, return an empty list
    if "theme_list" not in data.keys():
        # return [{'project ID': proj_id}]
        return []

    theme_list = []
    proj_id = data["id"]
    for theme1 in data["theme_list"]:

        # get first theme
        name = theme1["name"]
        theme_list.append(
            {
                "project ID": proj_id,
                "theme1": name,
                "percent": clean.clean_number(theme1["percent"]),
            }
        )

        # get 2nd theme
        if "theme2" in theme1.keys():
            for theme2 in theme1["theme2"]:
                name_2 = theme2["name"]
                theme_list.append(
                    {
                        "project ID": proj_id,
                        "theme1": name,
                        "theme2": name_2,
                        "percent": clean.clean_number(theme2["percent"]),
                    }
                )

                # get 3rd theme
                if "theme3" in theme2.keys():
                    for theme3 in theme2["theme3"]:
                        name_3 = theme3["name"]
                        theme_list.append(
                            {
                                "project ID": proj_id,
                                "theme1": name,
                                "theme2": name_2,
                                "theme3": name_3,
                                "percent": clean.clean_number(theme3["percent"]),
                            }
                        )
    return theme_list


def clean_sector(sector_series: pd.Series) -> pd.Series:
    """Format sector data from a nested list to a string separating sectors by ' | '
    If there are no sectors, np.nan will be placed in the series row.

    Args:
        sector_series: series of sector data

    Returns:
        series of sector data as a string separated by ' | '
    """

    return sector_series.apply(
        lambda x: " | ".join([item["Name"] for item in x])
        if isinstance(x, list)
        else np.nan
    )


general_fields = {  # general info
    "id": "project ID",
    "project_name": "project name",
    "countryshortname": "country",
    "regionname": "region name",
    "url": "url",
    "teamleadname": "team leader",
    "status": "status",
    "envassesmentcategorycode": "environmental assesment category",
    # dates
    "approvalfy": "fiscal year",
    "boardapprovaldate": "board approval date",
    "closingdate": "closing date",
    "p2a_updated_date": "update date",
    # lending
    "lendinginstr": "lending instrument",
    "borrower": "borrower",
    "impagency": "implementing agency",
    "lendprojectcost": "project cost",
    "totalcommamt": "total commitment",
    "grantamt": "grant amount",
    "idacommamt": "IDA commitment amount",
    "ibrdcommamt": "IBRD commitment amount",
    "curr_total_commitment": "current total IBRD and IDA commitment",
    "curr_ibrd_commitment": "current IBRD commitment",
    "curr_ida_commitment": "current IDA commitment",
    # sectors
    "sector": "sectors",
}


@dataclass
class WorldBankProjects(ImportData):
    """World Bank Projects Database Importer

    This object will import the World Bank Projects database from the World Bank API.
    To use, create an instance of the class. Optionally, you can specify the start and end dates
    of the data to import. If no dates are specified, all data will be imported.
    To import the data, call the load_data method. If the data has already downloaded, it will
    be loaded to the object from disk, otherwise it will be downloaded from the API.
    To retrieve the data, call the get_data method. You can specify the type of data to retrieve,
    either 'general' or 'theme'. If no type is specified, 'general' data will be returned.
    To update the data, call the update_data method. This will download the data from the API.
    If 'reload' is set to True, the data will be reloaded to the object.

    Parameters:
        start_date: start date of data to import, in DD-MM-YYYY format
        end_date: end date of data to import, in DD-MM-YYYY format.
    """

    start_date: str | None = None
    end_date: str | None = None

    @property
    def _path(self):
        """Generate path based on version"""

        start_date = f"_{self.start_date}" if self.start_date is not None else ""
        end_date = f"_{self.end_date}" if self.end_date is not None else ""

        return BBPaths.raw_data / f"world_bank_projects{start_date}{end_date}.json"

    def _format_general_data(self) -> None:
        """Clean and format general data and store it in _data attribute with key 'general_data'"""

        numeric_cols = [
            "lendprojectcost",
            "totalcommamt",
            "grantamt",
            "idacommamt",
            "ibrdcommamt",
            "curr_total_commitment",
            "curr_ibrd_commitment",
            "curr_ida_commitment",
        ]

        self._data["general_data"] = (
            pd.DataFrame.from_dict(self._raw_data, orient="index")
            .reset_index(drop=True)
            .loc[:, general_fields.keys()]
            # change the fiscal year to int
            .assign(
                approvalfy=lambda d: clean.clean_numeric_series(d["approvalfy"], to=int)
            )
            # change numeric columns to float
            .pipe(clean.clean_numeric_series, series_columns=numeric_cols)
            .assign(  # format dates
                boardapprovaldate=lambda d: clean.to_date_column(
                    d["boardapprovaldate"]
                ),
                closingdate=lambda d: clean.to_date_column(d["closingdate"]),
                p2a_updated_date=lambda d: clean.to_date_column(d["p2a_updated_date"]),
                # format sectors
                sector=lambda d: clean_sector(d["sector"]),
            )
            # rename columns
            .rename(columns=general_fields)
        )

    def _format_theme_data(self) -> None:
        """Format theme data and store it as a dataframe in _data attribute with key 'theme_data'"""

        theme_data = []
        for _, proj_data in self._raw_data.items():
            theme_data.extend(clean_theme(proj_data))

        self._data["theme_data"] = pd.DataFrame(theme_data)

    def _download(self) -> None:
        """Download data from World Bank Projects API and save it as a json file."""

        with open(self._path, "w") as file:
            data = (
                QueryAPI(start_date=self.start_date, end_date=self.end_date)
                .request_data()
                .get_data()
            )
            json.dump(data, file)

        logger.info(f"Successfully downloaded World Bank Projects")

    def load_data(self) -> ImportData:
        """Load data to the object

        This method will load the World Bank Project data to the object.
        If the data has already downloaded, it will be loaded to the object from disk,
        otherwise it will be downloaded from the API and saved as a json file and  loaded
        to the object.

        Returns:
            object with loaded data
        """

        # if file does not exist, download it and save it as a json file
        if not self._path.exists():
            self._download()

        # load data from json file
        with open(self._path, "r") as file:
            self._raw_data = json.load(file)

        if self._raw_data is None:
            raise EmptyDataException("No data was retrieved")

        # set data
        self._format_general_data()
        self._format_theme_data()

        logger.info(f"Successfully loaded World Bank Projects")
        return self

    def update_data(self, reload: bool = True) -> ImportData:
        """Force update of data

        This method will download the data from the API.
        If 'reload' is set to True, the data will be reloaded to the object.

        Args:
            reload: if True, reload data to object after downloading it.

        Returns:
            object with updated data
        """

        self._download()
        if reload:
            self.load_data()

        return self

    def get_data(
        self, project_codes: str | list = "all", data_type: str = "general", **kwargs
    ) -> pd.DataFrame:
        """Get the data as a dataframe

        Get the general data, or the theme data for World Bank Projects as a dataframe.
        Optionally, you can specify the project codes to retrieve data for. If no project codes
        are specified, data for all projects will be returned.

        Args:
            project_codes: project codes to retrieve data for. If 'all', data for all projects
            will be returned
            data_type: type of data to retrieve. Either 'general' or 'theme'
        """

        if data_type == "general":
            df = self._data["general_data"]
        elif data_type == "theme":
            df = self._data["theme_data"]
        else:
            raise ValueError("data_type must be either 'general' or 'theme'")

        if project_codes != "all":
            if isinstance(project_codes, str):
                project_codes = [project_codes]
            df = df[df["project ID"].isin(project_codes)]

        return df

    def get_json(self) -> dict:
        """Return the raw data as a dictionary"""

        return self._raw_data
