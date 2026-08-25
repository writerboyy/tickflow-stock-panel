from __future__ import annotations

from app.market_time import cn_today
from app.services.quote_service import QuoteService


def test_quote_polling_excludes_auction_phases():
    service = QuoteService()

    assert service._should_poll_for_phase("preopen") is False
    assert service._should_poll_for_phase("pre_afternoon") is False
    assert service._should_poll_for_phase("morning") is True
    assert service._should_poll_for_phase("afternoon") is True


def test_quote_polling_keeps_one_final_sync_per_boundary():
    service = QuoteService()

    assert service._should_poll_for_phase("morning_final") is True
    service._final_sync_done.add((cn_today(), "morning"))
    assert service._should_poll_for_phase("morning_final") is False
