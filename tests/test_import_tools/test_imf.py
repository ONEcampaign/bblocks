import pytest

from bblocks.import_tools.imf import, WorldEconomicOutlook, latest_sdr_exchange



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


def test_latest_sdr_exchange():

    usd_ex = latest_sdr_exchange()
    assert isinstance(usd_ex, dict)
    assert ['date', 'value'] == list(usd_ex)
    usd_ex_2 = latest_sdr_exchange('USD')
    assert usd_ex['value'] == usd_ex_2['value']

    sdr_ex = latest_sdr_exchange('SDR')
    assert isinstance(sdr_ex, dict)
    assert ['date', 'value'] == list(sdr_ex)

    assert round(sdr_ex['value'], 3) == round(1/usd_ex['value'], 3)

    with pytest.raises(ValueError) as error:
        latest_sdr_exchange('invalid_currency')

    assert 'Invalid currency' in str(error.value)
