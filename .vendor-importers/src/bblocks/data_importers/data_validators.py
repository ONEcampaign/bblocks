"""Module for data validators."""

import pandas as pd

from bblocks.data_importers.config import logger, DataValidationError


class DataFrameValidator:
    """General validator for pandas DataFrame."""

    def validate(self, df: pd.DataFrame, required_cols: list[str] = None) -> None:
        """Validate the DataFrame."""

        try:
            self.check_empty_df(df)
            if required_cols:
                self.check_required_cols(df, required_cols)
            self.check_pyarrow_dtypes(df)

            logger.debug("DataFrame validation successful.")

        except ValueError as e:
            raise DataValidationError(
                f"Data validation failed. There may be an issue with the original data source or the data transformation process. Error: {e}"
            )

    @staticmethod
    def check_empty_df(df: pd.DataFrame) -> None:
        """Check if the DataFrame is empty. If it is raise an error."""

        if df.empty:
            raise ValueError("DataFrame is empty.")

    @staticmethod
    def check_required_cols(df: pd.DataFrame, required_cols: list[str]) -> None:
        """Check if the DataFrame has the required columns. If not raise an error."""

        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing columns: {missing_cols}")

    @staticmethod
    def check_pyarrow_dtypes(df: pd.DataFrame) -> None:
        """Check if the DataFrame has pyarrow-compatible dtypes. If not raise an error."""

        for col in df.columns:
            if not isinstance(df[col].dtype, pd.ArrowDtype):
                raise ValueError(f"Column '{col}' does not have a pyarrow dtype.")
