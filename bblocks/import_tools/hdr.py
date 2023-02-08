"""Tools to import UNDP Human Development Report data.

`get_hdr_data` returns a dataframe with all HDR data.
Use the `HDR` object to extract and interact with the data
"""

import pandas as pd
import requests
import io
from bs4 import BeautifulSoup
from dataclasses import dataclass

from bblocks.config import BBPaths
from bblocks.import_tools.common import ImportData, get_response
from bblocks.logger import logger


BASE_URL: str = "https://hdr.undp.org/data-center/documentation-and-downloads"

# dictionary of composite indices with their variables
HDR_INDICATORS: dict[str, list] = {
    "hdi": ["hdi_rank", "hdi", "le", "eys", "mys", "gnipc"],
    "gdi": [
        "gdi_group",
        "gdi",
        "hdi_f",
        "le_f",
        "eys_f",
        "mys_f",
        "gni_pc_f",
        "hdi_m",
        "le_m",
        "eys_m",
        "mys_m",
        "gni_pc_m",
    ],
    "ihdi": ["ihdi", "coef_ineq", "loss", "ineq_le", "ineq_edu", "ineq_inc"],
    "gii": [
        "gii_rank",
        "gii",
        "mmr",
        "abr",
        "se_f",
        "se_m",
        "pr_f",
        "pr_m",
        "lfpr_f",
        "lfpr_m",
    ],
    "phdi": ["rankdiff_hdi_phdi", "phdi", "diff_hdi_phdi", "co2_prod", "mf"],
}


def _parse_html(soup: BeautifulSoup) -> tuple[str, str]:
    """Parses html for data and metadata links"""

    data_section = soup.find_all("div", {"class": "section data-links-files"})
    links = data_section[1].find_all("a")

    return links[0].get("href"), links[1].get("href")


def get_data_links() -> dict[str, str]:
    """returns links for data and metadata"""

    response = get_response(BASE_URL)
    soup = BeautifulSoup(response.content, "html.parser")
    data_url, metadata_url = _parse_html(soup)

    return {"data_url": data_url, "metadata_url": metadata_url}


def read_data(response: requests.Response) -> pd.DataFrame:
    """Read UNDP Human Development Report data from a URL.

    Returns a dataframe with either the data or metadata. The function checks
    the content type of the response and returns the appropriate dataframe.

    Args:
        response: Response object from a request to a UNDP HDR data URL.
    """

    # check content type, use read_csv for csv and read_excel for xlsx
    if response.headers["Content-Type"] == "text/csv":
        return pd.read_csv(io.BytesIO(response.content))

    elif (
        response.headers["Content-Type"]
        == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    ):
        return pd.read_excel(
            response.content,
            sheet_name="codebook",
            engine="openpyxl",
        )

    else:
        raise ValueError(f"Could not read data")


def create_code_dict(metadata_df: pd.DataFrame) -> dict[str, str]:
    """Create a dictionary of variable codes and names.

    This function takes the metadata dataframe and returns a dictionary
    to be used to map variable codes to variable names.

    Args:
        metadata_df (pd.DataFrame): Metadata dataframe.
    """

    return (
        metadata_df.dropna()
        .loc[:, ["Full name", "Short name"]]
        .set_index("Short name")
        .loc[:, "Full name"]
        .to_dict()
    )


def format_data(data_df: pd.DataFrame, code_dict: dict[str, str]) -> pd.DataFrame:
    """Format HDR data.

    This function takes the raw dataframe and:
    - melts the data to long format
    - creates a year columnand new variable column by splitting on the last underscore
        (variables contain year such as "hdi_rank_2019")
    - creates a variable_name column by mapping the variable to the code_dict

    Args:
        data_df (pd.DataFrame): Raw dataframe to be formatted.
        code_dict (dict): Dictionary of variable codes and names.
    """

    return data_df.melt(id_vars=["iso3", "country", "hdicode", "region"]).assign(
        year=lambda d: d["variable"].str.rsplit("_", n=1).str[-1].astype(int),
        variable=lambda d: d["variable"].str.rsplit("_", n=1).str[0],
        variable_name=lambda d: d["variable"].map(code_dict),
    )


def get_hdr_data() -> pd.DataFrame:
    """Get UNDP Human Development Report data.

    This function returns the formatted HDR data as a dataframe.
    """

    links = get_data_links()  # get links to data and metadata

    # read metadata and create dictionary
    code_dict = read_data(get_response(links["metadata_url"])).pipe(create_code_dict)

    # read data and format it
    data_df = read_data(get_response(links["data_url"])).pipe(format_data, code_dict)

    return data_df


def available_indicators(composite_index: str = "all") -> list[str]:
    """See available indicators

    Args:
        composite_index (str): Composite index to see available indicators for. If None, all
    """

    if composite_index == "all":
        return [val for sublist in HDR_INDICATORS.values() for val in sublist]

    if composite_index not in HDR_INDICATORS:
        raise ValueError(f"Composite index not found: {composite_index}")

    return HDR_INDICATORS[composite_index]


def available_composite_indices() -> list[str]:
    """See available composite indices"""

    return list(HDR_INDICATORS.keys())


@dataclass(repr=False)
class HDR(ImportData):
    """An object to help download and use UNDP Human Development Report data
    To use, create an instance of the class. Call `load_data` to load the data for
    desired indicators. If the data is not already downloaded, it will be downloaded,
    then the selected indicators will be loaded to the object.
    To see available indicators, call the function `available_indicators`. To see available composite
    indices, call the function `available_composite_indices`.
    To get the data as a dataframe, call `get_data` method. You can force an update by calling `update_data`
    """

    def load_data(self, indicators: str | list = "all") -> ImportData:
        """Load data to the object

        When called this method checks if the data has been downloaded, if not it downloads it.
        It then loads the data into the object for selected indicators and returns the object.

        Args:
            indicators: Indicator(s) to load. If "all", load all indicators.

        Returns:
            same object with loaded indicators to allow chaining
        """

        # download data if it does not exist
        path = BBPaths.raw_data / "HDR.csv"
        if not path.exists():
            get_hdr_data().to_csv(path, index=False)

        # load entire dataset to _raw_data if it does not exist
        if self._raw_data is None:
            self._raw_data = get_hdr_data()

        # if indicators is "all", load all indicators
        if indicators == "all":
            indicators = available_indicators()
        # if indicators is a string, convert to list
        if isinstance(indicators, str):
            indicators = [indicators]

        # check if indicators are valid
        for indicator in indicators:
            if indicator not in available_indicators():
                raise ValueError(f"Indicator {indicator} not found.")

        # load data
        self._data.update(
            {
                indicator: (
                    self._raw_data[self._raw_data["variable"] == indicator].reset_index(
                        drop=True
                    )
                )
                for indicator in indicators
            }
        )
        logger.info(f"Data loaded successfully.")
        return self

    def update_data(self, reload_data: bool = True) -> ImportData:
        """Update the data

        When called it will update the data used to load indicators.

        Args:
            reload_data: If True, reload the indicators after updating the data. Default is True.

        Returns:
            same object with updated data to allow chaining
        """

        if len(self._data) == 0:
            raise RuntimeError("No indicators loaded")

        self._raw_data = get_hdr_data()  # update data store in _raw_data attribute
        self._raw_data.to_csv(BBPaths.raw_data / "HDR.csv", index=False)
        if reload_data:
            self.load_data(indicators=list(self._data.keys()))

        logger.info("Data updated successfully.")
        return self
