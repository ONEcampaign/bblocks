import subprocess
import sys
import warnings

import pytest

import bblocks
import bblocks.importers as importers

_PUBLIC_IMPORTER_NAMES = [
    "GHED",
    "WEO",
    "get_dsa",
    "WFPFoodSecurity",
    "WFPInflation",
    "WorldBank",
    "get_wb_databases",
    "clear_wb_cache",
    "InternationalDebtStatistics",
    "HumanDevelopmentIndex",
    "UNAIDS",
    "BACI",
]


def test_version_is_non_empty_string():
    assert isinstance(bblocks.__version__, str)
    assert bblocks.__version__ != ""


@pytest.mark.parametrize("name", _PUBLIC_IMPORTER_NAMES)
def test_public_name_reexported_from_importers(name):
    assert getattr(bblocks, name) is getattr(importers, name)


def test_config_submodule_resolves():
    assert bblocks.config is importers.config


def test_unknown_attribute_raises_attribute_error():
    with pytest.raises(
        AttributeError, match=r"module 'bblocks' has no attribute 'nope'"
    ):
        bblocks.nope


def test_dir_includes_public_names():
    exported = dir(bblocks)
    for name in [*_PUBLIC_IMPORTER_NAMES, "config"]:
        assert name in exported


def test_import_is_lazy_subprocess():
    """bblocks.importers must not be imported until a lazy name is touched.

    Run out-of-process: an in-process assertion is worthless here because
    pytest collection has already imported every test module (including ones
    that import bblocks.importers), which would make bblocks.importers
    "already imported" for reasons that have nothing to do with laziness.
    """
    script = (
        "import sys\n"
        "import bblocks\n"
        "assert 'bblocks.importers' not in sys.modules, "
        "'bblocks.importers imported eagerly'\n"
        "bblocks.WEO\n"
        "assert 'bblocks.importers' in sys.modules, "
        "'bblocks.importers not imported after touching bblocks.WEO'\n"
        "print('OK')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "OK"


def test_touching_weo_does_not_import_unrelated_importers_subprocess():
    """Touching bblocks.WEO must import only WEO's own dependency tree.

    Run out-of-process for the same reason as test_import_is_lazy_subprocess:
    pytest collection has already imported every importer module in-process,
    so an in-process sys.modules check would be worthless. This specifically
    guards against bblocks.importers.__init__ re-becoming an eager importer
    of all twelve public names (e.g. World Bank, BACI) just because one of
    them (WEO) was touched.
    """
    script = (
        "import sys\n"
        "import bblocks\n"
        "bblocks.WEO\n"
        "assert 'bblocks.importers.world_bank.world_bank' not in sys.modules, "
        "'touching bblocks.WEO pulled in the World Bank importer'\n"
        "assert 'bblocks.importers.baci.baci' not in sys.modules, "
        "'touching bblocks.WEO pulled in the BACI importer'\n"
        "print('OK')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "OK"


def test_weo_survives_unwritable_importer_cache_dir_subprocess():
    """Reproduces the reviewer's scenario: a cache dir that can't be created
    for one importer (World Bank) must not break an unrelated importer
    (WEO).

    Before bblocks.importers became lazy, `from bblocks import WEO` imported
    the whole bblocks.importers package eagerly, which constructed the World
    Bank and BACI diskcache databases at import time. If that directory could
    not be created, importing WEO failed with NotADirectoryError even though
    WEO never touches that cache.

    Run out-of-process because it patches platformdirs.user_cache_dir before
    any importer submodule has been imported, which must happen in a fresh
    interpreter.
    """
    script = (
        "import os, tempfile\n"
        "import platformdirs\n"
        "f = tempfile.NamedTemporaryFile(delete=False)\n"
        "f.close()\n"
        "unwritable = os.path.join(f.name, 'cachedir')\n"
        "platformdirs.user_cache_dir = lambda *a, **k: unwritable\n"
        "from bblocks import WEO\n"
        "print('OK')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "OK"


def test_stale_data_importers_sibling_warns(monkeypatch, tmp_path):
    (tmp_path / "data_importers" / "who").mkdir(parents=True)
    monkeypatch.setattr(bblocks, "__file__", str(tmp_path / "__init__.py"))

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        bblocks._warn_if_stale_data_importers_sibling()

    assert len(caught) == 1
    assert "bblocks-data-importers" in str(caught[0].message)


def test_no_stale_data_importers_sibling_stays_silent(monkeypatch, tmp_path):
    # tmp_path has no data_importers/who at all.
    monkeypatch.setattr(bblocks, "__file__", str(tmp_path / "__init__.py"))

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        bblocks._warn_if_stale_data_importers_sibling()

    assert caught == []


def test_shim_without_who_stays_silent(monkeypatch, tmp_path):
    # The retiring 0.6.0 shim ships data_importers/__init__.py but no who/
    # subdirectory; that must not trigger the warning.
    (tmp_path / "data_importers").mkdir(parents=True)
    (tmp_path / "data_importers" / "__init__.py").write_text("")
    monkeypatch.setattr(bblocks, "__file__", str(tmp_path / "__init__.py"))

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        bblocks._warn_if_stale_data_importers_sibling()

    assert caught == []
