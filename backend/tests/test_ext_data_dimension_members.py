import polars as pl
import pytest
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi.testclient import TestClient
from types import SimpleNamespace

from app.api.ext_data import _filter_dimension_member_rows, router
from app.services.ext_data import ExtConfig, ExtConfigStore, ExtField


def test_filter_dimension_member_rows_matches_complete_tags() -> None:
    rows = pl.DataFrame({
        "symbol": ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"],
        "所属概念": ["人工智能;芯片", "人工智能体;机器人", "芯片 / 人工智能", None],
    })

    result = _filter_dimension_member_rows(rows, "所属概念", "人工智能")

    assert result.get_column("symbol").to_list() == ["000001.SZ", "000003.SZ"]


def test_filter_dimension_member_rows_matches_industry_hierarchy() -> None:
    rows = pl.DataFrame({
        "symbol": ["000001.SZ", "000002.SZ", "000003.SZ"],
        "所属行业": ["金融-银行-股份制银行", "电子-半导体-数字芯片", "电子元件"],
    })

    result = _filter_dimension_member_rows(rows, "所属行业", "电子")

    assert result.get_column("symbol").to_list() == ["000002.SZ"]


def test_filter_dimension_member_rows_rejects_unknown_field() -> None:
    rows = pl.DataFrame({"symbol": ["000001.SZ"]})

    with pytest.raises(HTTPException, match="字段 '所属行业' 不存在"):
        _filter_dimension_member_rows(rows, "所属行业", "银行")


def test_ext_rows_can_filter_auction_checkpoint_and_symbols(tmp_path) -> None:
    config = ExtConfig(
        id="ext_fuyao_auction",
        label="扶摇集合竞价",
        mode="timeseries",
        fields=[ExtField("checkpoint", "string"), ExtField("auction_pct", "float")],
    )
    ExtConfigStore(tmp_path).upsert(config)
    partition = tmp_path / "ext_data" / config.id / "timeseries" / "date=2026-08-28" / "part.parquet"
    partition.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["000001.SZ", "000001.SZ", "600000.SH"],
        "code": ["000001", "000001", "600000"],
        "checkpoint": ["0915", "0925", "0925"],
        "auction_pct": [1.0, 2.0, 3.0],
    }).write_parquet(partition)

    app = FastAPI()
    app.state.repo = SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path))
    app.include_router(router)

    response = TestClient(app).get(
        "/api/ext-data/ext_fuyao_auction/rows",
        params={"date": "2026-08-28", "checkpoint": "0925", "symbols": "000001.SZ"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["rows"] == [{
        "symbol": "000001.SZ",
        "code": "000001",
        "checkpoint": "0925",
        "auction_pct": 2.0,
    }]
