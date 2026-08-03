# Temporary Tushare History Backfill

`backend/scripts/backfill_tushare_history.py` is an offline, resumable import
tool for the Tushare-compatible proxy at `https://teajoin.com/`. It is not a
runtime provider and does not add the Tushare SDK.

The key is accepted only through stdin and stored as
`data/user_data/secrets.json` with mode `0600`:

```bash
DATA_DIR=/path/to/data uv run --project backend \
  python backend/scripts/backfill_tushare_history.py \
  --data-dir /path/to/data --run-id history-20260803 --key-stdin --preflight
```

After preflight, resume the same run with the required phases. Publishing is
explicit and disabled unless `--publish` is supplied:

```bash
DATA_DIR=/path/to/data uv run --project backend \
  python backend/scripts/backfill_tushare_history.py \
  --data-dir /path/to/data --run-id history-20260803 --resume \
  --phases universe,adjustment,stock_minute,etf_minute,publish_minute,p0,research \
  --publish
```

State is written to
`data/backfill_state/tushare_proxy/<run-id>/manifest.json`. Raw provider
responses are archived below `ext_data/_tushare_proxy_raw`; normalized raw
minute bars are stored in `tushare_archive/minute_stock_raw` and
`tushare_archive/minute_etf_raw`. Existing TickFlow rows win on overlapping
primary keys. Any overlap outside the configured tolerance blocks the whole
publication pass.

Use `--status --run-id <run-id>` to inspect progress without network access and
`--clear-key` to remove the temporary credential after the import is complete.
The script enforces a 50 GiB free-space reserve and marks permission or
provider errors as blocked instead of treating them as valid empty history.
