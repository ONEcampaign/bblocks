import pandas as pd


def _resample_group(
    group,
    *,
    date_column: str,
    freq: str,
    keep_latest: int | None = None,
):
    """Helper function to take the data in a given 'group' (a Pandas dataframe) and
    resample it to the specified frequency."""

    # If keep latest is not None, then an offset will be created to keep the
    # latest N values
    len_ = len(group) - 1

    offset = (
        list(range(len_, len_ - keep_latest, -1)) if keep_latest is not None else []
    )

    # Split into latest and timeseries
    df_latest = group.loc[lambda d: d.reset_index(drop=True).index.isin(offset)]
    df = group.loc[lambda d: ~d.reset_index(drop=True).index.isin(offset)]

    # Resample other data
    df = df.set_index(date_column).asfreq(freq=freq).reset_index(drop=False)

    return pd.concat([df_latest, df], ignore_index=True).drop_duplicates(keep="first")


def date_resample_to(
    df: pd.DataFrame,
    date_column: str,
    group_by: list,
    to_freq: str = "W-MON",
    keep_latest: int | None = None,
) -> pd.DataFrame:
    """Resample a dataframe to a specified frequency."""

    # Save column order
    columns = df.columns

    # Create analytical groups
    groups = df.copy(deep=True).groupby(group_by)

    # Empty list to hold resampled data
    dfs = []

    # Resample each group with the option to keep the latest N values
    for group in groups.groups:
        _ = groups.get_group(group)
        dfs.append(
            _resample_group(
                _,
                date_column=date_column,
                freq=to_freq,
                keep_latest=keep_latest,
            )
        )
    # Append all resampled data to reform original dataset (resampled)
    df_ = pd.concat(dfs, ignore_index=True)

    # Return reordering columns and sorting by date
    return (
        df_.filter(columns, axis=1)
        .sort_values(by=[date_column] + group_by)
        .reset_index(drop=True)
    )
