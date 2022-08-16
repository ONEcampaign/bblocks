from typing import Optional

import pandas as pd

from bblocks.cleaning_tools.clean import to_date_column, convert_id


def _resample_group(
    group,
    *,
    group_by: list,
    date_column: str,
    freq: str,
    aggregation: str | None,
    keep_latest: int | None = None,
):
    """Helper function which take the data in a given 'group' (a Pandas dataframe) and
    resample it to the specified frequency."""

    # If keep latest not None, then an offset by latest N values.
    len_ = len(group) - 1

    offset = (
        list(range(len_, len_ - keep_latest, -1)) if keep_latest is not None else []
    )

    # Ensure correct sorting
    group = group.sort_values(by=[date_column]).reset_index(drop=True)

    # Split into latest and timeseries
    mask = group.reset_index(drop=True).index.isin(offset)
    df_latest, df_other = group.loc[mask], group.loc[~mask]

    df_latest = df_latest.drop(columns=group_by)

    if aggregation is None:
        df_r = (
            df_other.set_index(date_column)
            .asfreq(freq=freq)
            .drop(columns=group_by)
            .reset_index(drop=False)
        )

    else:
        df_r = df_other.resample(rule=freq, on=date_column).agg(aggregation)
        try:
            df_r = df_r.reset_index(drop=False)
        except ValueError:
            df_r = df_r.reset_index(drop=True).drop(columns=group_by)

    return pd.concat([df_r, df_latest], ignore_index=True).drop_duplicates(keep="first")


def date_resample_to(
    df: pd.DataFrame,
    date_column: str,
    group_by: list,
    to_freq: str = "W-MON",
    aggregation: Optional[str | None] = None,
    keep_latest: int | None = None,
) -> pd.DataFrame:
    """Resample a dataframe to a specified frequency."""

    # Check if date format, if not, try converting it
    if not pd.api.types.is_datetime64_any_dtype(df[date_column]):
        df[date_column] = to_date_column(df[date_column])

    # Save list of original columns
    columns = df.columns

    # Create analytical groups
    df_ = (
        df.copy(deep=True)
        .groupby(group_by)
        .apply(
            _resample_group,
            group_by=group_by,
            date_column=date_column,
            freq=to_freq,
            aggregation=aggregation,
            keep_latest=keep_latest,
        )
        .reset_index(drop=False)
        .filter(columns, axis=1)
    )
    # Return reordering columns and sorting by date
    return df_.sort_values(by=[date_column] + group_by).reset_index(drop=True)
