from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import polars as pl

from app.services.ext_data import ExtConfigStore
from app.services.ext_presets import (
    _flatten_concept_rows,
    _flatten_industry_rows,
    _industry_preset,
)
from app.services.market_overview_builder import _dimension_rank, _dimension_values
from app.plugins.easy_tdx.storage import _config as _easy_tdx_industry_config


def test_concept_flatten_drops_missing_value_placeholders():
    rows = _flatten_concept_rows([{
        "symbol": "600000.SH",
        "name": "浦发银行",
        "concepts": ["银行", "nan", None, float("nan"), " null ", "金融科技"],
    }])

    assert rows[0]["所属概念"] == "银行;金融科技"


def test_industry_flatten_drops_missing_value_placeholders():
    rows = _flatten_industry_rows([{
        "symbol": "600000.SH",
        "name": "浦发银行",
        "industries": ["金融", "None", "银行"],
    }])

    assert rows[0]["所属同花顺行业"] == "金融-银行"


def test_overview_dimension_values_ignore_legacy_nan_group():
    assert _dimension_values("人工智能;nan;芯片;NULL") == ["人工智能", "芯片"]


def _write_industry_source(data_dir: Path, config, rows: list[dict]) -> None:
    ExtConfigStore(data_dir).upsert(config)
    path = data_dir / "ext_data" / config.id / "part.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)


def _industry_rank(data_dir: Path) -> dict:
    repo = SimpleNamespace(store=SimpleNamespace(data_dir=data_dir))
    rows = [{
        "symbol": "300979.SZ",
        "name": "华利集团",
        "change_pct": 0.058,
        "amount": 100.0,
    }]
    return _dimension_rank(
        rows,
        repo,
        "industry",
        level=2,
    )


def test_overview_industry_rank_uses_only_readable_industry_source(tmp_path: Path):
    _write_industry_source(tmp_path, _industry_preset(), [{
        "symbol": "300979.SZ",
        "所属同花顺行业": "纺织服饰-纺织制造-其他纺织",
    }])
    _write_industry_source(tmp_path, _easy_tdx_industry_config(), [{
        "symbol": "300979.SZ",
        "industry_sw": "X220105",
    }])

    rank = _industry_rank(tmp_path)

    assert [item["name"] for item in rank["leading"]] == ["纺织制造"]
    assert [item["name"] for item in rank["lagging"]] == ["纺织制造"]


def test_overview_industry_rank_does_not_fallback_to_code_source(tmp_path: Path):
    _write_industry_source(tmp_path, _easy_tdx_industry_config(), [{
        "symbol": "300979.SZ",
        "industry_sw": "X220105",
    }])

    assert _industry_rank(tmp_path) == {"leading": [], "lagging": []}
