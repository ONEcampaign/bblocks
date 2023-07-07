"""World Bank Projects Database Importer"""

import pandas as pd
import requests
import json
from dataclasses import dataclass
import re

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
        max_rows_per_response: int = 500,
        start_date: str | None = None,
        end_date: str | None = None,
        fields: list[str] | str = "*",
    ):
        """Initialize QueryAPI object

        Args:
            max_rows_per_response: maximum number of rows to return per request.
                                Must be less than or equal to 1000.
            start_date: start date of projects to return. Format: YYYY-MM-DD
            end_date: end date of projects to return. Format: YYYY-MM-DD
            fields: fields to return. Can be a list of strings or a single string.
                    By default, all fields are returned ('*').
        """

        self.max_rows_per_response = max_rows_per_response
        self.start_date = start_date
        self.end_date = end_date
        self.fields = fields

        self._params = {
            "format": "json",
            "rows": self.max_rows_per_response,
            # 'os': 0, # offset
            "strdate": self.start_date,
            "enddate": self.end_date,
            "fl": self.fields,
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

        logger.info(f"Retrieved {len(self.response_data)} projects from API")
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


def _get_sector_data(d: dict) -> dict:
    """Get sector percentages from a project dictionary

    the function first finds all available sectors
    It then finds all  fields from the json starting with 'sector' and ending with a number
    and gets a dictionary of the sector name and percentage
    If there are any sectors missing from the dict and the total percentage is less than 100
    the missing sector is added with the remaining percentage.
    If the total is still not 100, it will raise an error to indicate a
    problem with the data.

    args:
        d: project dictionary
    """

    sectors_dict = {}  # empty dict to store sector data as {sector_name: percent}
    sector_names = [v["Name"] for v in d["sector"]]  # get list of sector names

    # get sectors fields which should contain percentages
    sectors = {key: value for key, value in d.items() if re.search(r"^sector\d+$", key)}

    # get available sector percentages
    for _, v in sectors.items():
        if isinstance(v, dict):
            sectors_dict[v["Name"]] = v["Percent"]

    # check if there are missing sectors from the dict
    if (len(sectors_dict) == len(sectors) - 1) and (sum(sectors_dict.values()) < 100):

        # loop through all the available sectors
        for s in sector_names:
            # if a sectors has not been picked up it must be the missing sector
            if s not in sectors_dict.keys():
                sectors_dict[s] = 100 - sum(sectors_dict.values())

    if sum(sectors_dict.values()) != 100:
        raise ValueError("Sector percentages don't add up to 100%")

    return sectors_dict


general_fields = {  # general info
    "id": "project ID",
    "project_name": "project name",
    "countryshortname": "country",
    "regionname": "region name",
    "url": "url",
    "teamleadname": "team leader",
    "status": "status",
    "last_stage_reached_name": "last stage reached",
    "pdo": "project development objective",
    "cons_serv_reqd_ind": "consulting services required",
    "envassesmentcategorycode": "environmental assesment category",
    "esrc_ovrl_risk_rate": "environmental and social risk",
    # dates
    "approvalfy": "fiscal year",
    "boardapprovaldate": "board approval date",
    "closingdate": "closing date",
    "p2a_updated_date": "update date",
    # lending
    "lendinginstr": "lending instrument",
    "projectfinancialtype": "financing type",
    "borrower": "borrower",
    "impagency": "implementing agency",
    "lendprojectcost": "project cost",
    "totalcommamt": "total commitment",
    "grantamt": "grant amount",
    "idacommamt": "IDA commitment amount",
    "ibrdcommamt": "IBRD commitment amount",
    "curr_project_cost": "current project cost",
    "curr_total_commitment": "current total IBRD and IDA commitment",
    "curr_ibrd_commitment": "current IBRD commitment",
    "curr_ida_commitment": "current IDA commitment",
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
        start_date: start date of data to import, in YYYY-MM-DD format
        end_date: end date of data to import, in YYYY-MM-DD format.
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
            "curr_project_cost",
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

    def _format_sector_data(self) -> None:
        """Format sector data and store it as a dataframe in _data attribute
        with key 'sector_data'"""

        sector_data = []
        for _, proj_data in self._raw_data.items():
            if "sector" in proj_data.keys():
                proj_id = proj_data["id"]

                sectors = _get_sector_data(proj_data)
                sector_data.extend(
                    [
                        {"project ID": proj_id, "sector": s, "percent": p}
                        for s, p in sectors.items()
                    ]
                )

        self._data["sector_data"] = pd.DataFrame(sector_data)

    def _download(self) -> None:
        """Download data from World Bank Projects API and save it as a json file."""

        logger.info(f"Starting download of World Bank Projects")

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
        self._format_sector_data()

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
            data_type: type of data to retrieve. Either 'general', 'sector' or 'theme'

        Returns:
            dataframe with the requested data
        """

        # check if data has been loaded
        if len(self._data) == 0:
            raise EmptyDataException("Data has not been loaded. Run load_data() first.")

        if data_type == "general":
            df = self._data["general_data"]
        elif data_type == "theme":
            df = self._data["theme_data"]
        elif data_type == "sector":
            df = self._data["sector_data"]
        else:
            raise ValueError("data_type must be either 'general', 'theme' or 'sector'")

        if project_codes != "all":
            if isinstance(project_codes, str):
                project_codes = [project_codes]
            df = df[df["project ID"].isin(project_codes)]

        return df

    def get_json(self) -> dict:
        """Return the raw data as a dictionary"""

        return self._raw_data
