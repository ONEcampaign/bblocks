import importlib
import sys
from unittest.mock import patch

import pandas as pd
import pytest


@pytest.fixture
def ids_modules(tmp_path):
    """Reload the world_bank modules with a writable cache directory."""

    module_names = [
        "bblocks.data_importers.world_bank.world_bank",
        "bblocks.data_importers.world_bank.international_debt_statistics",
    ]
    for module in module_names:
        sys.modules.pop(module, None)

    cache_dir = tmp_path / "cache"
    with patch("platformdirs.user_cache_dir", lambda *_, **__: str(cache_dir)):
        world_bank = importlib.import_module(module_names[0])
        ids_module = importlib.import_module(module_names[1])

    world_bank.clear_wb_cache()

    yield world_bank, ids_module

    world_bank.clear_wb_cache()


def test_initializes_with_ids_database(ids_modules, monkeypatch):
    world_bank, ids_module = ids_modules
    captured = []
    monkeypatch.setattr(world_bank, "_check_valid_db", lambda db: captured.append(db))

    importer = ids_module.InternationalDebtStatistics()

    assert importer.db == 6
    assert captured == [6]


def test_repr_includes_database(ids_modules, monkeypatch):
    world_bank, ids_module = ids_modules
    monkeypatch.setattr(world_bank, "_check_valid_db", lambda db: None)

    importer = ids_module.InternationalDebtStatistics()

    assert repr(importer) == "InternationalDebtStatistics(db=6)"


def test_last_updated_reads_from_database_table(ids_modules, monkeypatch):
    world_bank, ids_module = ids_modules
    monkeypatch.setattr(world_bank, "_check_valid_db", lambda db: None)
    sample = pd.DataFrame(
        {
            "id": [6, 7],
            "last_updated": [
                pd.Timestamp("2024-04-01"),
                pd.Timestamp("2020-01-01"),
            ],
        }
    )
    monkeypatch.setattr(ids_module, "get_wb_databases", lambda: sample)

    importer = ids_module.InternationalDebtStatistics()

    assert importer.last_updated == pd.Timestamp("2024-04-01")


def test_debt_stock_indicators_fetch_metadata(ids_modules, monkeypatch):
    world_bank, ids_module = ids_modules
    monkeypatch.setattr(world_bank, "_check_valid_db", lambda db: None)
    captured = {}

    def fake_get_indicator_metadata(self, indicator_code):
        captured["indicator_code"] = indicator_code
        return pd.DataFrame({"indicator_code": indicator_code})

    monkeypatch.setattr(
        ids_module.WorldBank, "get_indicator_metadata", fake_get_indicator_metadata
    )

    importer = ids_module.InternationalDebtStatistics()
    df = importer.debt_stock_indicators

    expected = [
        "DT.DOD.BLAT.CD",
        "DT.DOD.MLAT.CD",
        "DT.DOD.PBND.CD",
        "DT.DOD.PCBK.CD",
        "DT.DOD.PROP.CD",
    ]

    assert captured["indicator_code"] == expected
    assert df["indicator_code"].tolist() == expected


def test_debt_service_indicators_fetch_metadata(ids_modules, monkeypatch):
    world_bank, ids_module = ids_modules
    monkeypatch.setattr(world_bank, "_check_valid_db", lambda db: None)
    captured = {}

    def fake_get_indicator_metadata(self, indicator_code):
        captured["indicator_code"] = indicator_code
        return pd.DataFrame({"indicator_code": indicator_code})

    monkeypatch.setattr(
        ids_module.WorldBank, "get_indicator_metadata", fake_get_indicator_metadata
    )

    importer = ids_module.InternationalDebtStatistics()
    df = importer.debt_service_indicators

    expected = [
        "DT.AMT.BLAT.CD",
        "DT.AMT.MLAT.CD",
        "DT.AMT.PBND.CD",
        "DT.AMT.PCBK.CD",
        "DT.AMT.PROP.CD",
        "DT.INT.BLAT.CD",
        "DT.INT.MLAT.CD",
        "DT.INT.PBND.CD",
        "DT.INT.PCBK.CD",
        "DT.INT.PROP.CD",
    ]

    assert captured["indicator_code"] == expected
    assert df["indicator_code"].tolist() == expected
