""" """
import io
from zipfile import ZipFile

import pandas as pd
import requests


def read_zipped_csv(url: str, path: str) -> pd.DataFrame:
    """Reads a CSV from a zipped file on the web

    Args:
        url (str): url link to the zipped file
        path (str): path to the csv in the folder

    Returns:
        pd.DataFrame
    """

    # request url and unzip file
    try:
        response = requests.get(url)
        folder = ZipFile(io.BytesIO(response.content))

    except ConnectionError:
        raise ConnectionError("invalid url")

    # read csv
    if path not in list(folder.NameToInfo.keys()):
        raise ValueError("invalid file path")
    else:
        return pd.read_csv(folder.open(path), low_memory=False)
