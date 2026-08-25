from __future__ import annotations

from datetime import date
import sys
from types import SimpleNamespace

from app.services import backup_data_sources


def test_baostock_index_volume_is_normalized_from_shares_to_lots(monkeypatch):
    class Query:
        error_code = "0"

        def __init__(self):
            self.done = False

        def next(self):
            self.done = not self.done
            return self.done

        def get_row_data(self):
            return ["2026-07-30", "10", "12", "9", "11", "1200", "3456"]

    fake = SimpleNamespace(
        login=lambda: SimpleNamespace(error_code="0"),
        logout=lambda: None,
        query_history_k_data_plus=lambda *_args, **_kwargs: Query(),
    )
    monkeypatch.setitem(sys.modules, "baostock", fake)

    result = backup_data_sources.fetch_baostock_index_daily(
        ["000001.SH"], date(2026, 7, 30), date(2026, 7, 30)
    )

    assert result["volume"].to_list() == [12.0]
    assert result["amount"].to_list() == [3456.0]
