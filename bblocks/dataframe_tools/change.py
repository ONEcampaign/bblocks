import pandas as pd

from bblocks.cleaning_tools.clean import to_date_column


def _resample_group(
    group,
    *,
    date_column: str,
    freq: str,
    keep_latest: int | None = None,
):
    """Helper function to take the data in a given 'group' (a Pandas dataframe) and
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

    # Resample other data
    df_r = df_other.set_index(date_column).asfreq(freq=freq).reset_index(drop=False)

    return pd.concat([df_r, df_latest], ignore_index=True).drop_duplicates(keep="first")


def date_resample_to(
    df: pd.DataFrame,
    date_column: str,
    group_by: list,
    to_freq: str = "W-MON",
    keep_latest: int | None = None,
) -> pd.DataFrame:
    """Resample a dataframe to a specified frequency."""

    # Check if date format, if not, try converting it
    if not pd.api.types.is_datetime64_any_dtype(df[date_column]):
        df[date_column] = to_date_column(df[date_column])

    # Create analytical groups
    df_ = (
        df.copy(deep=True)
        .groupby(group_by, as_index=False)
        .apply(
            _resample_group,
            date_column=date_column,
            freq=to_freq,
            keep_latest=keep_latest,
        )
    )
    # Return reordering columns and sorting by date
    return df_.sort_values(by=[date_column] + group_by).reset_index(drop=True)
