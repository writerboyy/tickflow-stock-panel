from __future__ import annotations

from scripts import backfill_tushare_history as cli

from app.services.tushare_history import save_tushare_key


def test_cli_datasets_and_asset_samples_reach_backfill_config(tmp_path, monkeypatch):
    save_tushare_key("secret", data_dir=tmp_path)
    seen = {}

    class Runner:
        def __init__(self, config, _client):
            seen["config"] = config

        def run(self):
            return {"status": "completed"}

    monkeypatch.setattr(cli, "TushareProxyClient", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(cli, "TushareHistoryBackfill", Runner)

    code = cli.main([
        "--data-dir", str(tmp_path),
        "--run-id", "sample",
        "--datasets", "daily,moneyflow",
        "--symbols", "000001.SZ,000002.SZ",
        "--etfs", "510300.SH",
        "--indexes", "000300.SH",
        "--start", "2025-01-01",
        "--end", "2025-01-03",
    ])

    assert code == 0
    config = seen["config"]
    assert config.phases == ("universe",)
    assert config.datasets == ("daily", "moneyflow")
    assert config.symbols == ("000001.SZ", "000002.SZ")
    assert config.etfs == ("510300.SH",)
    assert config.indexes == ("000300.SH",)


def test_cli_can_publish_completed_staging_without_tushare_key(tmp_path, monkeypatch):
    seen = {}

    class Publisher:
        def __init__(self, config, client):
            seen["config"] = config
            seen["client"] = client

        def publish(self, specs):
            seen["specs"] = tuple(spec.api_name for spec in specs)
            return {name: {"status": "published"} for name in seen["specs"]}

    monkeypatch.setattr(cli, "TushareDatasetIngestion", Publisher)
    monkeypatch.setattr(
        cli,
        "load_tushare_key",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("offline publish must not load a Tushare key")
        ),
    )

    code = cli.main([
        "--data-dir", str(tmp_path),
        "--run-id", "completed-run",
        "--datasets", "top_list,margin_detail",
        "--publish-staged",
    ])

    assert code == 0
    assert seen["client"] is None
    assert seen["config"].publish is True
    assert seen["specs"] == ("top_list", "margin_detail")
