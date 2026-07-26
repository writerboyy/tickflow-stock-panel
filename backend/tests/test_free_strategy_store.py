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


def test_paper_account_ledger_is_append_only(tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({"id": "paper-1", "status": "stopped"})
    store.append_event("paper-1", {"type": "created"})
    store.append_event("paper-1", {"type": "fill", "symbol": "510300.SH"})
    assert [event["type"] for event in store.events("paper-1")] == ["created", "fill"]
