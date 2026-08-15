from __future__ import annotations

import pandas as pd

from app.services.backtest import _add_max_hold_exits


def test_add_max_hold_exits_marks_each_entry_without_chained_assignment():
    entries = pd.DataFrame(
        {
            "000001.SZ": [True, False, False, False],
            "600000.SH": [False, False, True, False],
        }
    )

    result = _add_max_hold_exits(entries, max_hold_days=2)

    expected = pd.DataFrame(
        {
            "000001.SZ": [True, False, True, False],
            "600000.SH": [False, False, True, True],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_add_max_hold_exits_preserves_entry_rows_at_the_end():
    entries = pd.DataFrame({"000001.SZ": [False, False, True]})

    result = _add_max_hold_exits(entries, max_hold_days=5)

    pd.testing.assert_frame_equal(result, entries)
