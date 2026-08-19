import json

from app.free_strategy.store import FreeStrategyStore, PaperAccountStore


def test_strategy_revisions_and_source_are_persisted(tmp_path):
    store = FreeStrategyStore(tmp_path)
    first = store.save(None, "测试", "def on_bar(context, bars):\n    pass\n", {"timeframe": "1d"})
    second = store.save(first["id"], "测试", "def on_bar(context, bars):\n    context.log('v2')\n", {"timeframe": "5m"})
    loaded = store.get(first["id"])
    assert second["revision"] == 2
    assert loaded["source"].endswith("context.log('v2')\n")
    assert (tmp_path / "free_strategies" / first["id"] / "revisions" / "0001.py").exists()
    assert (tmp_path / "free_strategies" / first["id"] / "revisions" / "0002.py").exists()


def test_strategy_rename_keeps_revision_files_unchanged(tmp_path):
    store = FreeStrategyStore(tmp_path)
    saved = store.save(None, "原名", "def on_bar(context, bars):\n    pass\n", {})

    renamed = store.rename(saved["id"], "新名")

    assert renamed["name"] == "新名"
    assert renamed["revision"] == 1
    revisions = list((tmp_path / "free_strategies" / saved["id"] / "revisions").iterdir())
    assert [path.name for path in revisions] == ["0001.py"]


def test_strategy_store_defaults_legacy_manifest_to_native_dialect(tmp_path):
    root = tmp_path / "free_strategies" / "legacy"
    root.mkdir(parents=True)
    (root / "manifest.json").write_text(json.dumps({
        "id": "legacy",
        "name": "旧策略",
        "config": {},
        "revision": 1,
    }), encoding="utf-8")
    (root / "strategy.py").write_text("def on_bar(context, bars):\n    pass\n", encoding="utf-8")

    loaded = FreeStrategyStore(tmp_path).get("legacy")

    assert loaded["dialect"] == "native"


def test_strategy_store_rebuilds_missing_joinquant_report_from_source(tmp_path):
    root = tmp_path / "free_strategies" / "legacy-jq"
    root.mkdir(parents=True)
    source = "from jqdata import *\n\ndef handle_data(context, data):\n    pass\n"
    (root / "manifest.json").write_text(json.dumps({
        "id": "legacy-jq",
        "name": "旧聚宽策略",
        "config": {},
        "dialect": "joinquant",
        "revision": 1,
    }), encoding="utf-8")
    (root / "strategy.py").write_text(source, encoding="utf-8")

    loaded = FreeStrategyStore(tmp_path).get("legacy-jq")

    assert loaded["compatibility_report"]["dialect"] == "joinquant"
    assert loaded["compatibility_report"]["summary_status"] == "supported"


def test_joinquant_dialect_and_report_are_saved_and_preserved(tmp_path):
    source = "from jqdata import *\n\ndef handle_data(context, data):\n    pass\n"
    store = FreeStrategyStore(tmp_path)
    saved = store.save(None, "聚宽策略", source, {}, dialect="joinquant")

    updated = store.save(saved["id"], "聚宽策略 v2", source, {})

    assert updated["dialect"] == "joinquant"
    assert updated["compatibility_report"]["dialect"] == "joinquant"
    assert updated["compatibility_report"]["summary_status"] == "supported"


def test_paper_account_ledger_is_append_only(tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({"id": "paper-1", "status": "stopped"})
    store.append_event("paper-1", {"type": "created"})
    store.append_event("paper-1", {"type": "fill", "symbol": "510300.SH"})
    assert [event["type"] for event in store.events("paper-1")] == ["created", "fill"]
