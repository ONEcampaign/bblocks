"""Tests for BACI data_manager module."""

import io
from contextlib import nullcontext
from zipfile import ZipFile
from unittest import mock

import pandas as pd
import pyarrow as pa
import pytest
import requests

from bblocks.importers.baci.data_manager import (
    BaciDataManager,
    BACI_DATA_COLUMNS,
    _parse_readme,
)
from bblocks.importers.config import Fields, DataExtractionError


def _build_zip_bytes(file_map: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with ZipFile(buffer, mode="w") as zf:
        for name, content in file_map.items():
            zf.writestr(name, content)
    return buffer.getvalue()


def test_parse_readme_extracts_metadata():
    readme = (
        "Title: BACI data\r\n"
        "Notes line 1\r\n"
        "Notes line 2\r\n\r\n"
        "List of Variables:\r\n"
        "Should be ignored\r\n\r\n"
        "Source: CEPII\r\n"
        "More details"
    )

    metadata = _parse_readme(readme)

    assert metadata == {
        "Title": "BACI data Notes line 1 Notes line 2",
        "Source": "CEPII More details",
    }


def test_extract_zip_file_success():
    zip_bytes = _build_zip_bytes({"dummy.txt": b"content"})
    manager = BaciDataManager(hs_version="HS22", url="http://example.com")

    mock_response = mock.Mock()
    mock_response.content = zip_bytes
    mock_response.raise_for_status = mock.Mock()

    with mock.patch("requests.get", return_value=mock_response) as mock_get:
        manager.extract_zip_file()

    mock_get.assert_called_once_with("http://example.com")
    assert manager._zip_file is not None
    assert isinstance(manager._zip_file, ZipFile)


def test_extract_zip_file_failure():
    manager = BaciDataManager(hs_version="HS22", url="http://example.com")

    with mock.patch("requests.get", side_effect=requests.RequestException("boom")):
        with pytest.raises(DataExtractionError, match="Failed to extract BACI data"):
            manager.extract_zip_file()


def test_list_data_files_filters_by_version():
    manager = BaciDataManager(hs_version="HS22", url="http://example.com")
    manager._zip_file = mock.Mock()
    manager._zip_file.namelist.return_value = [
        "BACI_HS22_2020.csv",
        "BACI_HS12_2020.csv",
        "notes.txt",
    ]

    files = manager._list_data_files()

    assert files == ["BACI_HS22_2020.csv"]


def test_list_data_files_raises_when_missing():
    manager = BaciDataManager(hs_version="HS22", url="http://example.com")
    manager._zip_file = mock.Mock()
    manager._zip_file.namelist.return_value = ["BACI_HS12_2020.csv"]

    with pytest.raises(FileNotFoundError, match="No BACI data files found"):
        manager._list_data_files()


def test_read_data_files_renames_and_converts():
    manager = BaciDataManager(hs_version="HS22", url="http://example.com")
    manager._zip_file = mock.Mock()
    manager._zip_file.open.return_value = nullcontext(io.BytesIO(b"ignored"))

    table1 = pa.table(
        {
            "t": [2020],
            "i": [1],
            "j": [2],
            "k": [100],
            "v": [1.5],
            "q": [2.5],
        }
    )
    table2 = pa.table(
        {
            "t": [2021],
            "i": [3],
            "j": [4],
            "k": [101],
            "v": [3.5],
            "q": [4.5],
        }
    )

    with (
        mock.patch.object(manager, "_list_data_files", return_value=["a.csv", "b.csv"]),
        mock.patch(
            "bblocks.importers.baci.data_manager.pv.read_csv",
            side_effect=[table1, table2],
        ),
    ):
        manager._read_data_files()

    expected_columns = [BACI_DATA_COLUMNS[c] for c in ["t", "i", "j", "k", "v", "q"]]
    assert list(manager.data.columns) == expected_columns
    assert all(isinstance(dtype, pd.ArrowDtype) for dtype in manager.data.dtypes)


def test_read_product_codes_success():
    zip_bytes = _build_zip_bytes(
        {
            "product_codes.csv": (
                "code,description\n100,Widgets\n200,Gadgets\n"
            ).encode("utf-8")
        }
    )

    manager = BaciDataManager(hs_version="HS22", url="http://example.com")
    manager._zip_file = ZipFile(io.BytesIO(zip_bytes))

    manager._read_product_codes()

    assert list(manager.product_codes.columns) == [
        Fields.product_code,
        Fields.product_description,
    ]
    assert all(
        isinstance(dtype, pd.ArrowDtype) for dtype in manager.product_codes.dtypes
    )


def test_read_product_codes_missing():
    zip_bytes = _build_zip_bytes({"other.csv": b"x"})
    manager = BaciDataManager(hs_version="HS22", url="http://example.com")
    manager._zip_file = ZipFile(io.BytesIO(zip_bytes))

    with pytest.raises(FileNotFoundError, match="No product codes found"):
        manager._read_product_codes()


def test_read_country_codes_success():
    zip_bytes = _build_zip_bytes(
        {
            "country_codes.csv": (
                "country_code,country_name,country_iso3,country_iso2\n1,Alpha,AAA,AA\n"
            ).encode("utf-8")
        }
    )

    manager = BaciDataManager(hs_version="HS22", url="http://example.com")
    manager._zip_file = ZipFile(io.BytesIO(zip_bytes))

    manager._read_country_codes()

    assert list(manager.country_codes.columns) == [
        Fields.country_code,
        Fields.country_name,
        Fields.iso3_code,
    ]
    assert all(
        isinstance(dtype, pd.ArrowDtype) for dtype in manager.country_codes.dtypes
    )


def test_read_country_codes_missing():
    zip_bytes = _build_zip_bytes({"other.csv": b"x"})
    manager = BaciDataManager(hs_version="HS22", url="http://example.com")
    manager._zip_file = ZipFile(io.BytesIO(zip_bytes))

    with pytest.raises(FileNotFoundError, match="No country codes file found"):
        manager._read_country_codes()


def test_read_readme_success():
    zip_bytes = _build_zip_bytes(
        {
            "Readme.txt": ("Title: BACI data\n\nNotes: Line one\nLine two").encode(
                "utf-8"
            )
        }
    )

    manager = BaciDataManager(hs_version="HS22", url="http://example.com")
    manager._zip_file = ZipFile(io.BytesIO(zip_bytes))

    manager._read_readme()

    assert manager.metadata == {
        "Title": "BACI data",
        "Notes": "Line one Line two",
    }


def test_read_readme_missing():
    zip_bytes = _build_zip_bytes({"other.txt": b"x"})
    manager = BaciDataManager(hs_version="HS22", url="http://example.com")
    manager._zip_file = ZipFile(io.BytesIO(zip_bytes))

    with pytest.raises(FileNotFoundError, match="No metadata found"):
        manager._read_readme()


def test_read_readme_empty_metadata():
    zip_bytes = _build_zip_bytes({"Readme.txt": b"List of Variables:\n"})
    manager = BaciDataManager(hs_version="HS22", url="http://example.com")
    manager._zip_file = ZipFile(io.BytesIO(zip_bytes))

    with pytest.raises(DataExtractionError, match="No metadata found"):
        manager._read_readme()


def test_read_data_calls_all_methods():
    manager = BaciDataManager(hs_version="HS22", url="http://example.com")

    with (
        mock.patch.object(manager, "_read_data_files") as mock_data,
        mock.patch.object(manager, "_read_product_codes") as mock_products,
        mock.patch.object(manager, "_read_country_codes") as mock_countries,
        mock.patch.object(manager, "_read_readme") as mock_readme,
    ):
        manager.read_data()

    mock_data.assert_called_once()
    mock_products.assert_called_once()
    mock_countries.assert_called_once()
    mock_readme.assert_called_once()


def test_extract_closes_zip_file():
    manager = BaciDataManager(hs_version="HS22", url="http://example.com")
    zip_mock = mock.Mock()

    def _set_zip():
        manager._zip_file = zip_mock

    with (
        mock.patch.object(manager, "extract_zip_file", side_effect=_set_zip),
        mock.patch.object(manager, "read_data"),
    ):
        manager.extract()

    zip_mock.close.assert_called_once()
    assert manager._zip_file is None
