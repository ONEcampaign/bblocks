from pathlib import Path


class BBPaths:
    """Class to store the paths to the _data and output folders."""

    project = Path(__file__).resolve().parent.parent
    scripts = project / "bblocks"
    raw_data = scripts / ".raw_data"
    imported_data = raw_data
    pydeflate_data = raw_data / ".pydeflate"
    wfp_data = raw_data / "wfp_raw"
    import_settings = scripts / "import_tools/settings"
    tests = project / "tests"
    import_tools = scripts / "import_tools"
    debt_settings = import_tools / "debt/settings"
    tests_data = project / "tests/.tests_data"
