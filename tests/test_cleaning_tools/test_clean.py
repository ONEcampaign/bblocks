import pandas as pd

from bblocks.cleaning_tools.clean import clean_number, clean_numeric_series


def test_clean_number() -> None:

    # Test several types of inputs
    n1 = "1.2"
    n2 = "1,000.23"
    n3 = "5,000,000"
    n5 = "$45.50"

    assert clean_number(n1) == 1.2
    assert clean_number(n1, to=int) == 1
    assert clean_number(n2) == 1000.23
    assert clean_number(n2, to=int) == 1000
    assert clean_number(n3, to=float) == 5_000_000
    assert clean_number(n3, to=int) == 5_000_000
    assert clean_number(n5) == 45.50
    assert clean_number(n5, to=int) == 46


def test_clean_numeric_series() -> None:

    # test a dataframe
    df = pd.DataFrame(
        {"a": ['1', '$2.5', '3,000.43'], "b": ['456,234.1', '$5,000,000',  'strong',]}
    )

    # test df with a single column as string
    assert [1.0, 2.5, 3000.43] == clean_numeric_series(df, series_columns="a")[
        "a"
    ].to_list()

    # test df with two columns
    assert [456234.1, 5000000.0] == clean_numeric_series(
        df, series_columns=["a", "b"]
    )["b"].to_list()[:2]

    # test a series
    s = pd.Series(['1', '$2.5', '3,000.43'])
    assert [1, 2, 3000] == clean_numeric_series(s, to=int).to_list()
