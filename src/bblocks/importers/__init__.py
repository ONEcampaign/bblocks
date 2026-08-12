from importlib import import_module
from importlib.metadata import version

__version__ = version("bblocks")

# Public name -> dotted path of the submodule that defines it. Resolved
# lazily via __getattr__ below (PEP 562), mirroring bblocks/__init__.py.
#
# Each importer pulls in its own dependency tree (pandas, pyarrow, requests,
# diskcache, ...) and some construct on-import state (e.g. the World Bank and
# BACI importers open a diskcache database at import time). Importing this
# subpackage eagerly would mean using any single importer, such as
# bblocks.WEO, imports every other importer too - so a broken cache
# directory for one importer would break importers that never use it. Keep
# this mapping in sync with bblocks._IMPORTER_NAMES at the top level.
_IMPORTER_MODULES = {
    "GHED": "bblocks.importers.who.ghed",
    "WEO": "bblocks.importers.imf.weo",
    "get_dsa": "bblocks.importers.imf.dsa",
    "WFPFoodSecurity": "bblocks.importers.wfp.wfp",
    "WFPInflation": "bblocks.importers.wfp.wfp",
    "WorldBank": "bblocks.importers.world_bank.world_bank",
    "get_wb_databases": "bblocks.importers.world_bank.world_bank",
    "clear_wb_cache": "bblocks.importers.world_bank.world_bank",
    "InternationalDebtStatistics": (
        "bblocks.importers.world_bank.international_debt_statistics"
    ),
    "HumanDevelopmentIndex": "bblocks.importers.undp.hdi",
    "UNAIDS": "bblocks.importers.unaids.unaids",
    "BACI": "bblocks.importers.baci.baci",
}

__all__ = sorted(_IMPORTER_MODULES)


def __getattr__(name: str):
    """Lazily resolve a public name from its defining submodule (PEP 562)."""
    module_path = _IMPORTER_MODULES.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = import_module(module_path)
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
