from __future__ import annotations

import polars as pl
import pytest

from app.services import ext_presets
from app.services.ext_data import ExtConfig, ExtConfigStore, ExtField
from app.services.ext_pull import _apply_preset_flatten


SAMPLE_ROW = {
    "symbol": "601398.SH",
    "name": "工商银行",
    "net": 1_342_702_909.74,
    "inflow": 2_955_340_965.51,
    "outflow": 1_612_638_055.77,
    "rank": 1,
    "change_pct": 3.1047865459,
}


def test_money_flow_preset_declares_latest_snapshot_contract():
    config = ext_presets.get_preset("ext_money_flow")

    assert config is not None
    assert config.mode == "snapshot"
    assert config.pull.url.endswith("/market_flow/exports/money-flow")
    assert config.pull.enabled is True
    assert [(field.name, field.dtype) for field in config.fields] == [
        ("symbol", "string"),
        ("code", "string"),
        ("name", "string"),
        ("net", "float"),
        ("inflow", "float"),
        ("outflow", "float"),
        ("rank", "int"),
        ("change_pct", "float"),
    ]


def test_money_flow_flatten_preserves_units_and_drops_rows_without_symbol():
    rows = ext_presets._flatten_money_flow_rows([
        SAMPLE_ROW,
        {**SAMPLE_ROW, "symbol": ""},
    ])

    assert rows == [{**SAMPLE_ROW, "code": "601398"}]
    assert rows[0]["net"] == 1_342_702_909.74
    assert rows[0]["change_pct"] == 3.1047865459


def test_scheduled_pull_uses_money_flow_preset_transform():
    rows = _apply_preset_flatten("ext_money_flow", [SAMPLE_ROW])

    assert rows[0]["code"] == "601398"
    assert rows[0]["rank"] == 1


@pytest.mark.asyncio
async def test_builtin_presets_create_money_flow_config(tmp_path):
    await ext_presets.ensure_builtin_presets(tmp_path)

    config = ExtConfigStore(tmp_path).get("ext_money_flow")
    assert config is not None
    assert config.label == "资金流向"
    assert config.pull.url.endswith("/market_flow/exports/money-flow")


@pytest.mark.asyncio
async def test_builtin_presets_do_not_replace_existing_money_flow_config(tmp_path):
    store = ExtConfigStore(tmp_path)
    store.upsert(ExtConfig(
        id="ext_money_flow",
        label="用户资金流",
        mode="snapshot",
        fields=[ExtField("symbol", "string", "标的代码")],
    ))

    await ext_presets.ensure_builtin_presets(tmp_path)

    config = store.get("ext_money_flow")
    assert config is not None
    assert config.label == "用户资金流"
    assert config.pull is None


@pytest.mark.asyncio
async def test_money_flow_fetch_writes_snapshot_with_declared_schema(tmp_path, monkeypatch):
    async def fake_fetch_json(url: str) -> list[dict]:
        assert url.endswith("/market_flow/exports/money-flow")
        return [SAMPLE_ROW]

    monkeypatch.setattr(ext_presets, "_fetch_json", fake_fetch_json)

    rows = await ext_presets.fetch_preset("ext_money_flow", tmp_path)

    assert rows == 1
    stored = pl.read_parquet(tmp_path / "ext_data" / "ext_money_flow" / "part.parquet")
    assert stored.to_dicts() == [{**SAMPLE_ROW, "code": "601398"}]
    assert stored.schema == {
        "symbol": pl.String,
        "code": pl.String,
        "name": pl.String,
        "net": pl.Float64,
        "inflow": pl.Float64,
        "outflow": pl.Float64,
        "rank": pl.Int64,
        "change_pct": pl.Float64,
    }


@pytest.mark.asyncio
async def test_money_flow_fetch_rejects_empty_response(tmp_path, monkeypatch):
    async def fake_fetch_json(url: str) -> list[dict]:
        return []

    monkeypatch.setattr(ext_presets, "_fetch_json", fake_fetch_json)

    with pytest.raises(ValueError, match="接口返回 0 行"):
        await ext_presets.fetch_preset("ext_money_flow", tmp_path)

    assert ExtConfigStore(tmp_path).get("ext_money_flow") is not None
    assert not (tmp_path / "ext_data" / "ext_money_flow" / "part.parquet").exists()
