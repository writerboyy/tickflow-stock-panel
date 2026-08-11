from __future__ import annotations

from scripts import import_stockdata_etf as cli


def test_cli_is_dry_run_unless_publish_is_explicit(tmp_path, monkeypatch):
    seen = {}

    def run(config, *, progress):
        seen["config"] = config
        progress("audit")
        return {"publish": {"status": "dry_run"}}

    monkeypatch.setattr(cli, "run_stockdata_etf_import", run)

    assert cli.main([str(tmp_path), "--data-dir", str(tmp_path)]) == 0
    assert seen["config"].publish is False

    assert cli.main([str(tmp_path), "--data-dir", str(tmp_path), "--publish"]) == 0
    assert seen["config"].publish is True
