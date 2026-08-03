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
