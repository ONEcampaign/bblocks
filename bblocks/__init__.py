__version__ = "1.2.0"

# Easy access to importers
from bblocks.import_tools.world_bank import WorldBankData
from bblocks.import_tools.who import GHED
from bblocks.import_tools.wfp import WFPData
from bblocks.import_tools.imf import WorldEconomicOutlook
from bblocks.import_tools.unaids import Aids
from bblocks.import_tools.debt import DebtIDS, get_dsa

# Easy access to add tools
from bblocks.dataframe_tools.add import (
    add_iso_codes_column,
    add_income_level_column,
    add_short_names_column,
)

# Easy access to cleaning tools
from bblocks.cleaning_tools.clean import (
    clean_number,
    clean_numeric_series,
    to_date_column,
    convert_id,
    date_to_str,
    format_number,
)

# Easy access to filter tools
from bblocks.cleaning_tools.filter import (
    filter_by_continent,
    filter_by_un_region,
    filter_eu_countries,
    filter_african_countries,
    filter_latest_by,
)


def set_bblocks_data_path(path):
    from pathlib import Path
    from bblocks.config import BBPaths

    """Set the path to the _data folder."""
    global BBPaths

    BBPaths.raw_data = Path(path).resolve()
    BBPaths.wfp_data = Path(path).resolve() / "wfp_raw"
    BBPaths.imported_data = Path(path).resolve()
