""" """
import pandas as pd
import requests
import io
from zipfile import ZipFile

def read_zipped_csv(url: str, path: str) -> pd.DataFrame:
    """ """

    #request url and unzip file
    try:
        response = requests.get(url)
        folder = ZipFile(io.BytesIO(response.content))

    except ConnectionError:
        raise ConnectionError('invalid url')

    #read csv
    if path not in list(folder.NameToInfo.keys()):
        raise ValueError('invalid file path')
    else:
        return pd.read_csv(folder.open(path), low_memory=False)