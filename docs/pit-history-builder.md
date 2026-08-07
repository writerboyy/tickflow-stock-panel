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

- An index snapshot proves membership only on its `snapshot_date`. It cannot be
  expanded into effective intervals or used to fill other dates.
- The current canonical seed is migrated from dated JoinQuant snapshots, but
  runtime consumers read only `index_membership_history`; they do not depend on
  a provider directory or source label.
- BaoStock `query_hs300_stocks(date=...)` is accepted only as a dated CSI 300
  constituent candidate snapshot under `pit_reference/baostock`. It remains
  supplemental evidence and never overwrites canonical JoinQuant rows.
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
  1000 require 300/500/800/1000 unique members. The builder rejects incomplete
  dates by default; `--allow-incomplete-index` is only for archived,
  non-backtest reference data.
- Industry history is a multi-standard table. Always filter exactly one
  `industry_standard` before joining it to a daily PIT panel.

Examples:

    cd backend

    # Default: consolidate dated JoinQuant snapshots into the canonical table.
    uv run python scripts/migrate_index_membership_history.py

    # A cached dated snapshot export must include a snapshot_date/快照日期 field.
    uv run python scripts/build_pit_history_from_raw.py \
      --index-history-file ../raw/hs300_snapshots.csv \
      --index-symbol 000300.SH \
      --index-source joinquant

    # BaoStock HS300 candidate snapshots for latest five years of local trading dates.
    # Existing snapshot partitions are skipped by default; keep this single-process.
    uv run python scripts/collect_baostock_hs300_candidates.py --years 5 --sleep-seconds 1

    # BaoStock stock lifecycle supplement for latest five years.
    # This also applies list_date/delist_date/status to data/instruments.
    uv run python scripts/collect_baostock_lifecycle.py --years 5

    # If direct BaoStock TCP is blocked, use an HTTP CONNECT proxy.
    uv run python scripts/collect_baostock_hs300_candidates.py \
      --years 5 \
      --max-dates 20 \
      --proxy-url http://127.0.0.1:7890 \
      --force-proxy

    # Cached AKShare/Cninfo industry-change export and exchange delisting export.
    uv run python scripts/build_pit_history_from_raw.py \
      --industry-history-file ../raw/cninfo_industry_changes.csv \
      --industry-source cninfo \
      --lifecycle-file ../raw/exchange_delist.csv \
      --lifecycle-source exchange

The script archives raw envelopes under ext_data/_pit_history_raw, writes
ingestion manifests under ext_data/_ingestion/pit_history, and publishes
Parquet tables under pit_reference/history.
