import pandas as pd
import pytest
import os

from bblocks.config import PATHS
from bblocks.import_tools.imf import SDR, WorldEconomicOutlook


def test_sdr_load_indicator():
    sdr_obj = SDR()

    sdr_obj.load_indicator()
    assert "holdings" and "allocations" in sdr_obj.indicators.keys()

    sdr_obj.load_indicator("holdings")
    assert (
        "holdings" in sdr_obj.indicators.keys()
        and "allocations" not in sdr_obj.indicators.keys()
    )
    assert isinstance(sdr_obj.indicators["holdings"], pd.DataFrame)

    sdr_obj.load_indicator("allocations")
    assert "holdings" not in sdr_obj.indicators and "allocations" in sdr_obj.indicators
    assert isinstance(sdr_obj.indicators["allocations"], pd.DataFrame)

    invalid_indicator = "invalid"
    with pytest.raises(ValueError) as error:
        sdr_obj.load_indicator(invalid_indicator)
    assert invalid_indicator in str(error.value)


def test_sdr_update():
    sdr_obj = SDR()

    sdr_obj.load_indicator()
    file_path = f"{PATHS.imported_data}/{sdr_obj.file_name}"
    time = os.path.getmtime(file_path)

    sdr_obj.update()
    assert os.path.getmtime(file_path) > time


def test_sdr_get_data():
    sdr_obj = SDR()

    sdr_obj.load_indicator()
    df = sdr_obj.get_data()
    assert isinstance(df, pd.DataFrame)
    assert df.indicator.nunique() == 2

    sdr_obj.load_indicator()
    df = sdr_obj.get_data(indicators="holdings")
    assert df.indicator.nunique() == 1

    sdr_obj.load_indicator()
    df = sdr_obj.get_data(members="Zimbabwe")
    assert df.member.nunique() == 1

    invalid_member = "invalid"
    with pytest.raises(ValueError) as error:
        sdr_obj.load_indicator()
        sdr_obj.get_data(members=invalid_member)
    assert "No members found" in str(error.value)

    invalid_list = ["Zimbabwe", "invalid"]
    with pytest.warns(UserWarning) as record:
        sdr_obj.load_indicator()
        df = sdr_obj.get_data(members=invalid_list)
    assert len(record) == 1
    assert record[0].message.args[0] == "member not found: invalid"


def test_weo_load_indicator():
    obj = WorldEconomicOutlook()

    valid_indicators = ["NGSD_NGDP", "NGDP", "TX_RPCH"]

    for _ in valid_indicators:
        obj.load_indicator(_)
        assert _ in obj.indicators.keys()

    invalid_indicators = ["nonsense", "invalid"]

    for _ in invalid_indicators:
        with pytest.raises(ValueError) as error:
            obj.load_indicator(_)
        assert _ in str(error.value)


def test_weo_update():
    obj = WorldEconomicOutlook()

    obj.update()

    obj2 = WorldEconomicOutlook(update_data=True)
    obj2.load_indicator("NGDP")

    # the object only wraps functionality contained in weo package.
    # if no errors are raised, then assert True
    assert True


def test_available_indicators():
    obj = WorldEconomicOutlook()

    # All this indicator does is print.
    obj.available_indicators()

    # If no errors are raised, then assert True
    assert True


def test_get_data():
    obj = WorldEconomicOutlook()

    # load indicators
    valid_indicators = ["NGSD_NGDP", "NGDP", "TX_RPCH"]

    for _ in valid_indicators:
        obj.load_indicator(_)
        assert _ in obj.indicators.keys()

    # get data for single indicator
    df_ngdp = obj.get_data(indicators="NGDP")
    assert len(df_ngdp) > 0
    assert df_ngdp.indicator.nunique() == 1
    assert df_ngdp.indicator.unique()[0] == "NGDP"

    # get data for multiple indicators
    df_two = obj.get_data(indicators=["NGSD_NGDP", "NGDP"])
    assert len(df_two) > 0
    assert df_two.indicator.nunique() == 2
    assert list(df_two.indicator.unique()) == ["NGSD_NGDP", "NGDP"]

    # get data for all indicators at once
    df_all = obj.get_data()
    assert len(df_all) > 0
    assert df_all.indicator.nunique() == 3
    assert list(df_all.indicator.unique()) == valid_indicators

    # test getting data for an indicator that hasn't been loaded
    with pytest.raises(ValueError):
        _ = obj.get_data(indicators="BCA")

    # get data including metadata
    df_meta = obj.get_data("NGDP", keep_metadata=True)

    assert len(df_meta.columns) > len(df_ngdp.columns)
    assert df_meta.estimate.sum() > 1
