"""Tests for the IMF DSA importer."""

from io import BytesIO
from types import SimpleNamespace
from unittest import mock

import httpx
import pandas as pd
import pandas.testing as pdt
import pytest

from bblocks.data_importers.config import (
    DataExtractionError,
    DataFormattingError,
    Fields,
)
from bblocks.data_importers.imf import dsa


@pytest.fixture
def sample_raw_table():
    """Mimic the table returned from camelot before cleaning."""

    return pd.DataFrame(
        {
            0: ["#", "1", "2"],
            1: ["Country", "Country One 1/ ", "Country Two"],
            2: ["Latest publication", "2024-01-15", "2024-04-01"],
            3: ["Risk of debt distress", " in debt distress ", "low"],
            4: ["ignored", "ignored", "ignored"],
            5: ["Debt sustainability", "SUSTAINABLE", "unsustainable 2/"],
            6: ["Joint", "Yes", "No"],
            7: ["Latest DSA discussed", "2024-02-01", ""],
        }
    )


@pytest.fixture
def raw_table_without_header(sample_raw_table):
    """Sample table without the header row."""

    return sample_raw_table.iloc[1:].reset_index(drop=True)


@pytest.fixture
def expected_clean_df():
    df = pd.DataFrame(
        {
            Fields.country_name: ["Country One", "Country Two"],
            "latest_publication": pd.to_datetime(["2024-01-15", "2024-04-01"]),
            "risk_of_debt_distress": ["In debt distress", "Low"],
            "debt_sustainability_assessment": ["Sustainable", "Unsustainable"],
            "joint_with_world_bank": [True, False],
            "latest_dsa_discussed": pd.to_datetime(["2024-02-01", None]),
        }
    )
    return df.convert_dtypes(dtype_backend="pyarrow")


def test_strip_footnote_trailer_removes_trailing_marker():
    assert dsa.__strip_footnote_trailer("Example 3/  ") == "Example"


def test_strip_footnote_trailer_passthrough_for_non_strings():
    assert dsa.__strip_footnote_trailer(None) is None
    assert dsa.__strip_footnote_trailer(123) == 123


def test_download_pdf_uses_httpx_client():
    fake_content = b"%PDF"
    mock_client = mock.MagicMock()
    mock_response = mock.MagicMock()
    mock_response.content = fake_content
    mock_client.get.return_value = mock_response
    mock_client.__enter__.return_value = mock_client
    mock_client.__exit__.return_value = False

    with mock.patch("httpx.Client", return_value=mock_client) as client_cls:
        result = dsa._download_pdf("https://example.com/dsa.pdf")

    assert result == fake_content
    client_kwargs = client_cls.call_args.kwargs
    assert client_kwargs["follow_redirects"] is True
    assert client_kwargs["timeout"] == httpx.Timeout(30.0)
    mock_client.get.assert_called_once_with(
        "https://example.com/dsa.pdf",
        headers={
            "User-Agent": "bblocks data importers @ https://data.one.org",
            "Accept": "application/pdf",
        },
    )
    mock_response.raise_for_status.assert_called_once()


def test_pdf_to_df_returns_single_table():
    fake_df = pd.DataFrame({"a": [1, 2]})
    table = SimpleNamespace(df=fake_df)

    with mock.patch("camelot.read_pdf", return_value=[table]) as read_pdf:
        result = dsa._pdf_to_df(b"pdf-bytes")

    assert result.equals(fake_df)
    read_args, read_kwargs = read_pdf.call_args
    assert isinstance(read_args[0], BytesIO)
    assert read_kwargs == {"flavor": "stream"}


def test_pdf_to_df_raises_when_pdf_invalid():
    with mock.patch("camelot.read_pdf", return_value=[]):
        with pytest.raises(DataExtractionError, match="Invalid PDF format"):
            dsa._pdf_to_df(b"broken")


def test_clean_headers_selects_expected_columns(raw_table_without_header):
    cleaned = dsa.__clean_headers(raw_table_without_header)

    assert list(cleaned.columns) == [
        Fields.country_name,
        "latest_publication",
        "risk_of_debt_distress",
        "debt_sustainability_assessment",
        "joint_with_world_bank",
        "latest_dsa_discussed",
    ]


def test_normalise_country_names_strips_footnotes(raw_table_without_header):
    df = dsa.__clean_headers(raw_table_without_header)
    result = dsa.__normalise_country_names(df)
    assert result[Fields.country_name].tolist() == ["Country One", "Country Two"]


def test_normalise_country_names_raises_on_nulls():
    df = pd.DataFrame({Fields.country_name: ["Valid Country", None]})
    with pytest.raises(DataFormattingError, match="Null values"):
        dsa.__normalise_country_names(df)


def test_normalise_booleans(raw_table_without_header):
    df = dsa.__clean_headers(raw_table_without_header)
    result = dsa.__normalise_booleans(df, "joint_with_world_bank")
    assert result["joint_with_world_bank"].tolist() == [True, False]


def test_normalise_debt_distress_standardises_labels(raw_table_without_header):
    df = dsa.__clean_headers(raw_table_without_header)
    result = dsa.__normalise_debt_distress(df)
    assert result["risk_of_debt_distress"].tolist() == ["In debt distress", "Low"]


def test_normalise_debt_sustainability_standardises_labels(raw_table_without_header):
    df = dsa.__clean_headers(raw_table_without_header)
    result = dsa.__normalise_debt_sustainability(df)
    assert result["debt_sustainability_assessment"].tolist() == [
        "Sustainable",
        "Unsustainable",
    ]


def test_normalise_date_parses_columns(raw_table_without_header):
    df = dsa.__clean_headers(raw_table_without_header)
    result = dsa.__normalise_date(df, "latest_publication")
    assert pd.api.types.is_datetime64_any_dtype(result["latest_publication"])
    assert result.loc[0, "latest_publication"].date().isoformat() == "2024-01-15"

    result = dsa.__normalise_date(result, "latest_dsa_discussed")
    assert pd.api.types.is_datetime64_any_dtype(result["latest_dsa_discussed"])
    assert result.loc[0, "latest_dsa_discussed"].date().isoformat() == "2024-02-01"
    assert pd.isna(result.loc[1, "latest_dsa_discussed"])


def test_clean_df_applies_full_pipeline(sample_raw_table, expected_clean_df):
    result = dsa._clean_df(sample_raw_table)
    pdt.assert_frame_equal(result, expected_clean_df)


def test_get_dsa_returns_expected_dataframe(sample_raw_table, expected_clean_df):
    with (
        mock.patch("bblocks.data_importers.imf.dsa._download_pdf", return_value=b"pdf"),
        mock.patch(
            "bblocks.data_importers.imf.dsa._pdf_to_df", return_value=sample_raw_table
        ),
    ):
        result = dsa.get_dsa()

    pdt.assert_frame_equal(result, expected_clean_df)
