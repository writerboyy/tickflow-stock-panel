# PIT history builder

TickFlow can build strict reference-history tables from cached public-data
exports without registering a long-running external data provider.

The builder publishes three canonical tables under pit_reference/history:

| Table | Meaning | Required timing fields |
| --- | --- | --- |
| index_membership_history | Daily index constituent snapshots | snapshot_date |
| industry_membership_history | Per-stock industry classification intervals | effective_from, derived effective_to |
| instrument_lifecycle_events | Listing, suspension, delisting and delisting-period events | event_date, event_type |

Important boundaries:

- Runtime consumers read only `index_membership_history`; provider-specific
  constituent directories are not runtime tables.
- BaoStock historical CSI 300/500 responses include `updateDate`. The backfill
  expands an exact response only across local trading dates from that source
  update date through the queried date, then jumps backward to the prior local
  trading date. It never invents a change date.
- CSI 800 history is published only when the exact same-day CSI 300 and CSI 500
  union contains 800 unique members.
- CSI 1000 can be supplemented temporarily from exact Tushare monthly
  `index_weight` snapshots. Monthly snapshots are never expanded to daily dates,
  and BaoStock remains the default historical source for CSI 300/500.
- HiThink supplies forward daily snapshots for CSI 300/500/800/1000. BaoStock
  cross-checks CSI 300/500; a same-date disagreement rejects the whole incoming
  snapshot without changing the canonical table.
- BaoStock `query_stock_basic` can supplement recent stock listing/delisting
  dates into `instrument_lifecycle_events` and `instruments`; it still lacks
  delisting decision, delisting-period and reason fields, so lifecycle
  completeness remains partial.
- Industry `effective_to` is treated as an exclusive upper bound by backtests:
  effective_from <= trade_date < effective_to.
- The lifecycle table records only events present in the raw file. A row with
  only 上市日期 and 终止上市日期 is useful, but it is not a complete delisting
  lifecycle unless the source also includes decision dates, delisting-period
  dates, and reasons.
- `provenance=dated_snapshot` means an index row is valid only on its stored
  date. `provenance=historical_event` is reserved for actual event timing.
- Every stored index date must have the exact expected count: CSI 300/500/800/
  1000 require 300/500/800/1000 unique members. Incomplete dates are rejected
  from the canonical table.
- Industry history is a multi-standard table. Always filter exactly one
  `industry_standard` before joining it to a daily PIT panel.

Examples:

    cd backend

    # Backfill all local trading dates from BaoStock CSI 300/500 and derive CSI 800.
    uv run python scripts/backfill_index_membership_history.py

    # A cached dated snapshot export must include a snapshot_date/快照日期 field.
    uv run python scripts/build_pit_history_from_raw.py \
      --index-history-file ../raw/hs300_snapshots.csv \
      --index-symbol 000300.SH \
      --index-source manual_export

    # Temporary exact monthly CSI 1000 supplement. This does not change the
    # default BaoStock historical path.
    uv run python scripts/supplement_tushare_index_membership.py \
      --indices 000852.SH

    # Inspect the local range without network calls or publication.
    uv run python scripts/backfill_index_membership_history.py --dry-run

    # BaoStock stock lifecycle supplement for latest five years.
    # This also applies list_date/delist_date/status to data/instruments.
    uv run python scripts/collect_baostock_lifecycle.py --years 5

    # Restrict a repair to a bounded local trading-date range.
    uv run python scripts/backfill_index_membership_history.py \
      --start-date 2020-01-02 \
      --end-date 2026-08-07

    # Cached AKShare/Cninfo industry-change export and exchange delisting export.
    uv run python scripts/build_pit_history_from_raw.py \
      --industry-history-file ../raw/cninfo_industry_changes.csv \
      --industry-source cninfo \
      --lifecycle-file ../raw/exchange_delist.csv \
      --lifecycle-source exchange

The scripts archive raw envelopes under `ext_data`, write ingestion manifests,
back up an existing canonical membership file before a historical run, and
publish the single table under `pit_reference/history`.
