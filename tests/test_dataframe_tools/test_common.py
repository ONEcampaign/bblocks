from bblocks.dataframe_tools.common import __get_wb_ind, get_population_df
from bblocks import set_bblocks_data_path, config

set_bblocks_data_path(config.BBPaths.tests_data)


def test__get_wb_ind():

    data = __get_wb_ind(
        ind_code="SP.POP.TOTL", ind_name="test_population", update=False, mrnev=False
    )

    assert list(data.columns) == ["year", "iso_code", "test_population"]
    assert len(data.columns) > 0

    mrnev_data = __get_wb_ind(
        ind_code="SP.POP.TOTL", ind_name="test_population", update=False, mrnev=True
    )

    assert len(mrnev_data) < len(data)

    assert list(mrnev_data.columns) == ["year", "iso_code", "test_population"]


def test_get_population_df():

    data = get_population_df(most_recent_only=True, update=False)

    assert list(data.columns) == ["year", "iso_code", "population"]
    assert len(data) > 0
