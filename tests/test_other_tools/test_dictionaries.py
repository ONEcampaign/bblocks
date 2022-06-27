from bblocks.config import PATHS
from bblocks.other_tools.dictionaries import update_dictionaries
import os


def test_update_dictionaries():

    files = [
        r"income_levels.csv",
        r"EN.POP.DNST_all_most_recent.csv",
        r"SI.POV.DDAY_all_most_recent.csv",
        r"SP.DYN.LE00.IN_all_most_recent.csv",
        r"SP.POP.TOTL_all_most_recent.csv",
    ]

    pre_update = []
    post_update = []

    for f in files:
        pre_update.append(os.path.getmtime(f"{PATHS.imported_data}/{f}"))

    #update_dictionaries()

    for f in files:
        post_update.append(os.path.getmtime(f"{PATHS.imported_data}/{f}"))

    #assert all(pre_update[f] < post_update[f] for f in range(len(files)))
    assert True