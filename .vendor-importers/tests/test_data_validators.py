"""Tests for the validator class"""

from bblocks.data_importers.data_validators import DataFrameValidator
from bblocks.data_importers.data_validators import DataValidationError

import pytest
import pandas as pd


@pytest.fixture
def valid_pyarrow_df():
    """Fixture for a valid DataFrame converted to pyarrow dtypes."""
    data = {"col1": [1, 2, 3], "col2": [4.5, 5.6, 6.7], "col3": ["a", "b", "c"]}
    df = pd.DataFrame(data)

    # Convert all columns to pyarrow dtypes
    return df.convert_dtypes(dtype_backend="pyarrow")


@pytest.fixture
def empty_df():
    """Fixture for an empty DataFrame."""
    return pd.DataFrame()


@pytest.fixture
def df_missing_columns():
    """Fixture for a DataFrame missing required columns."""
    data = {
        "col1": pd.Series([1, 2, 3], dtype="int64[pyarrow]"),
        "col2": pd.Series([4.5, 5.6, 6.7], dtype="float64[pyarrow]"),
        # Missing 'col3'
    }
    return pd.DataFrame(data)


@pytest.fixture
def df_non_pyarrow():
    """Fixture for a DataFrame with non-pyarrow dtypes."""
    data = {
        "col1": pd.Series([1, 2, 3], dtype="int64"),  # Regular int64, not pyarrow
        "col2": pd.Series([4.5, 5.6, 6.7], dtype="float64"),  # Regular float64
        "col3": pd.Series(["a", "b", "c"], dtype="string"),
    }
    return pd.DataFrame(data)


# TESTS


def test_validate_valid_dataframe(valid_pyarrow_df):
    """Test DataFrameValidator with a valid DataFrame (no errors expected)"""

    validator = DataFrameValidator()

    # Simply call the validate method without expecting any exceptions
    validator.validate(valid_pyarrow_df)


def test_validate_empty_dataframe(empty_df):
    """Test DataFrameValidator raises an error when the DataFrame is empty"""

    validator = DataFrameValidator()

    # Ensure that validating an empty DataFrame raises a DataValidationError
    with pytest.raises(
        DataValidationError, match="Data validation failed.*DataFrame is empty"
    ):
        validator.validate(empty_df)


def test_validate_missing_columns(df_missing_columns):
    """Test DataFrameValidator raises an error when required columns are missing"""

    validator = DataFrameValidator()

    # Define the required columns, including one that is missing
    required_columns = ["col1", "col2", "col3"]  # 'col3' is missing

    # Ensure that validating a DataFrame with missing columns raises DataValidationError
    with pytest.raises(
        DataValidationError,
        match=r"Data validation failed.*Missing columns: \['col3'\]",
    ):
        validator.validate(df_missing_columns, required_cols=required_columns)


def test_validate_non_pyarrow_dtypes(df_non_pyarrow):
    """Test DataFrameValidator raises an error when columns do not have pyarrow dtypes"""

    validator = DataFrameValidator()

    # Ensure that validating a DataFrame with non-pyarrow dtypes raises DataValidationError
    with pytest.raises(
        DataValidationError,
        match=r"Data validation failed.*Column 'col1' does not have a pyarrow dtype.",
    ):
        validator.validate(df_non_pyarrow)
