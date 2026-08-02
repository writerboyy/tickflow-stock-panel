# HiThink supplemental snapshots

HiThink/Fuyao is an optional supplemental source. TickFlow remains the primary
lake for OHLCV, financials, corporate actions, and derived enriched outputs.

The HiThink integration freezes current reference facts into dated tables:

| Table | Meaning | Provenance |
| --- | --- | --- |
| pit_reference/hithink/index_constituents_snapshots | Current standard-index constituents, e.g. 000300.SH, frozen by collection date | snapshot_frozen |
| pit_reference/hithink/ths_sector_constituents_snapshots | Current THS industry/concept constituents, frozen by collection date | snapshot_frozen |
| pit_reference/hithink/instrument_lifecycle_observed | Observable lifecycle derived from current ticker list and local daily K coverage | observed |

Important boundaries:

- HiThink index constituents are current snapshots, not official historical PIT
  adjustment records. Backtests before the first frozen snapshot must fail
  closed unless the user explicitly accepts a proxy or snapshot-frozen source.
- THS sector snapshots do not backfill historical industry membership. A target
  date can only use a snapshot collected on or before that date.
- Observed lifecycle is not an official delisting event feed. It can block
  impossible trades from local history, but it cannot provide delisting reasons
  or full delisting-board status.

Manual collection:

    cd backend
    HITHINK_FINANCE_API_KEY=... uv run python scripts/collect_hithink_snapshots.py \
      --indices 000300.SH,000905.SH \
      --sector-tags industry,cn_concept \
      --lifecycle

The collector writes ingestion manifests under ext_data/_ingestion/hithink
and raw envelopes under ext_data/_hithink_raw, but published reference tables
stay under pit_reference/hithink.
