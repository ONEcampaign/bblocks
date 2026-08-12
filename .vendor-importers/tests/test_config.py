"""Tests for the config module."""

from bblocks.data_importers.config import Paths, set_data_path


def test_paths():
    """Test the Paths class."""
    assert Paths.data.name == ".data"


def test_set_raw_data_path():
    """Test the set_raw_data_path function."""
    set_data_path("new_path")
    assert Paths.data.name == "new_path"
