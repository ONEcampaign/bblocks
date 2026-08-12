import warnings
from importlib.metadata import version
from pathlib import Path

__version__ = version("bblocks")

# Names re-exported lazily from bblocks.importers (see __getattr__ below), plus
# the config submodule. Keep this list and bblocks.importers's public surface
# in sync.
_IMPORTER_NAMES = frozenset(
    {
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
    }
)

__all__ = sorted(_IMPORTER_NAMES | {"config"})


def __getattr__(name: str):
    """Lazily resolve public names from bblocks.importers (PEP 562).

    `bblocks.importers` pulls in the full importer dependency tree (pandas,
    pyarrow, camelot, ...), which is expensive to import. Deferring the import
    until a name is actually touched keeps `import bblocks` cheap for callers
    who only want e.g. `bblocks.__version__`.
    """
    if name == "config":
        from bblocks.importers import config

        globals()["config"] = config
        return config

    if name in _IMPORTER_NAMES:
        import bblocks.importers as _importers

        value = getattr(_importers, name)
        globals()[name] = value
        return value

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(set(globals()) | set(__all__))


def _warn_if_stale_data_importers_sibling() -> None:
    """Detect a stale `bblocks-data-importers` <=0.5.0 install alongside this one.

    That old distribution vendored the whole importer codebase under
    `bblocks/data_importers/`. When both it and this distribution are
    installed, its `data_importers/who/` subdirectory ends up sitting next to
    this package on disk and silently serves stale, duplicate classes with no
    warning of its own. The retiring 0.6.0 shim ships only
    `data_importers/__init__.py` (no `who/` subdirectory), so probing for
    `who/` distinguishes the old vendored tree from the shim.

    This is a filesystem probe rather than a metadata/version lookup so it
    stays cheap and doesn't require importing anything.
    """
    probe = Path(__file__).parent / "data_importers" / "who"
    if probe.is_dir():
        warnings.warn(
            "Detected a stale 'bblocks-data-importers' install (<=0.5.0) "
            "alongside 'bblocks'. It vendors its own copy of the importer "
            "code at 'bblocks/data_importers/', which will shadow and "
            "silently serve outdated classes. Upgrade 'bblocks-data-importers' "
            "to the latest version (or uninstall it) to fix this.",
            stacklevel=2,
        )


_warn_if_stale_data_importers_sibling()
