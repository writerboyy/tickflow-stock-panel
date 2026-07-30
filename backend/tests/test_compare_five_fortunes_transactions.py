from decimal import Decimal

from scripts.compare_five_fortunes_transactions import _displayed


def test_displayed_fee_uses_export_half_even_rounding():
    assert _displayed(2.685, Decimal("2.68")) == Decimal("2.68")
