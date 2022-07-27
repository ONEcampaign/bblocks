from bblocks.cleaning_tools.common import clean_number


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
