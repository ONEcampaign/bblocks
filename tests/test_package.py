import bblocks


def test_version_is_non_empty_string():
    assert isinstance(bblocks.__version__, str)
    assert bblocks.__version__ != ""
