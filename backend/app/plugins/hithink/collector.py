"""HiThink supplemental snapshot collection.

The collector freezes current HiThink facts as dated reference snapshots. It
does not backfill official historical PIT membership and does not replace
TickFlow primary OHLCV, financial, or corporate-action datasets.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from app.plugins.hithink.client import HiThinkClient
from app.plugins.hithink.storage import (
    INDEX_CONSTITUENTS_TABLE,
    INSTRUMENT_LIFECYCLE_TABLE,
    PARSER_VERSION,
    SOURCE,
    THS_SECTOR_CONSTITUENTS_TABLE,
    normalize_index_constituents,
    normalize_lifecycle_observed,
    normalize_sector_constituents,
    publish_snapshot,
)
from app.plugins.pit_history.storage import (
    merge_index_membership_history,
    validate_index_membership_history,
)
from app.services.ingestion_manifest import (
    archive_source_payload,
    stable_content_hash,
    update_ingestion_manifest,
)


logger = logging.getLogger(__name__)


class HiThinkSnapshotCollector:
    def __init__(self, data_dir: Path, client: HiThinkClient | None = None) -> None:
        self.data_dir = Path(data_dir)
        self.client = client or HiThinkClient()

    def collect_index_constituents(
        self,
        indices: Iterable[str],
        *,
        snapshot_date: date,
        index_names: dict[str, str] | None = None,
    ) -> int:
        frame = self.fetch_index_constituents(
            indices,
            snapshot_date=snapshot_date,
            index_names=index_names,
        )
        result = merge_index_membership_history(self.data_dir, frame)
        return int(result["added_rows"])

    def fetch_index_constituents(
        self,
        indices: Iterable[str],
        *,
        snapshot_date: date,
        index_names: dict[str, str] | None = None,
    ) -> pl.DataFrame:
        frames: list[pl.DataFrame] = []
        raw_payloads: dict[str, Any] = {}
        names = {key.upper(): value for key, value in (index_names or {}).items()}
        for index_symbol in sorted({item.upper() for item in indices if item}):
            payload = self.client.get_index_constituents(index_symbol)
            raw_payloads[index_symbol] = payload
            frame = normalize_index_constituents(
                index_symbol=index_symbol,
                index_name=names.get(index_symbol, ""),
                payload=payload,
                snapshot_date=snapshot_date,
            )
            if not frame.is_empty():
                frames.append(frame)

        logical_snapshot = snapshot_date.isoformat()
        _, source_hash = archive_source_payload(
            self.data_dir,
            SOURCE,
            INDEX_CONSTITUENTS_TABLE,
            logical_snapshot,
            "all-indices",
            raw_payloads,
            parser_version=PARSER_VERSION,
        )
        if not frames:
            update_ingestion_manifest(
                self.data_dir,
                SOURCE,
                INDEX_CONSTITUENTS_TABLE,
                logical_snapshot,
                status="valid_empty",
                parser_version=PARSER_VERSION,
                source_content_hash=source_hash,
                published_rows=0,
                empty_reason="source_empty",
                provenance="snapshot_frozen",
            )
            return pl.DataFrame()

        merged = pl.concat(frames, how="diagonal_relaxed").unique(
            subset=["index_symbol", "member_symbol"],
            keep="last",
        ).sort(["index_symbol", "member_symbol"])
        validation = validate_index_membership_history(merged)
        if not validation["usable"]:
            update_ingestion_manifest(
                self.data_dir,
                SOURCE,
                INDEX_CONSTITUENTS_TABLE,
                logical_snapshot,
                status="rejected",
                parser_version=PARSER_VERSION,
                schema_version=1,
                source_content_hash=source_hash,
                content_hash=stable_content_hash(merged.to_dicts()),
                published_rows=0,
                provenance="snapshot_frozen",
            )
            raise ValueError(f"HiThink index membership failed strict validation: {validation}")
        update_ingestion_manifest(
            self.data_dir,
            SOURCE,
            INDEX_CONSTITUENTS_TABLE,
            logical_snapshot,
            status="published",
            parser_version=PARSER_VERSION,
            schema_version=1,
            source_content_hash=source_hash,
            content_hash=stable_content_hash(merged.to_dicts()),
            published_rows=merged.height,
            provenance="snapshot_frozen",
        )
        logger.info("HiThink index constituent snapshot fetched: %d rows", merged.height)
        return merged

    def collect_sector_constituents(
        self,
        tags: Iterable[str],
        *,
        snapshot_date: date,
        sector_limit: int | None = None,
    ) -> int:
        frames: list[pl.DataFrame] = []
        raw_payloads: dict[str, Any] = {}
        for tag in [item.strip().lower() for item in tags if item.strip()]:
            catalog = self.client.get_ths_index_list(tag)
            sectors = catalog.get("item") or []
            if not isinstance(sectors, list):
                raise ValueError("HiThink sector catalog item must be a list")
            selected = sectors[:sector_limit] if sector_limit else sectors
            raw_payloads[f"catalog:{tag}"] = catalog
            for sector in selected:
                if not isinstance(sector, dict):
                    continue
                sector_symbol = str(sector.get("thscode") or "").strip().upper()
                if not sector_symbol:
                    continue
                payload = self.client.get_index_constituents(sector_symbol)
                raw_payloads[f"{tag}:{sector_symbol}"] = payload
                frame = normalize_sector_constituents(
                    sector_symbol=sector_symbol,
                    sector_name=str(sector.get("name") or ""),
                    sector_tag=tag,
                    payload=payload,
                    snapshot_date=snapshot_date,
                )
                if not frame.is_empty():
                    frames.append(frame)

        logical_snapshot = snapshot_date.isoformat()
        _, source_hash = archive_source_payload(
            self.data_dir,
            SOURCE,
            THS_SECTOR_CONSTITUENTS_TABLE,
            logical_snapshot,
            "all-sectors",
            raw_payloads,
            parser_version=PARSER_VERSION,
        )
        if not frames:
            update_ingestion_manifest(
                self.data_dir,
                SOURCE,
                THS_SECTOR_CONSTITUENTS_TABLE,
                logical_snapshot,
                status="valid_empty",
                parser_version=PARSER_VERSION,
                source_content_hash=source_hash,
                published_rows=0,
                empty_reason="source_empty",
                provenance="snapshot_frozen",
            )
            return 0

        merged = pl.concat(frames, how="diagonal_relaxed").unique(
            subset=["sector_symbol", "member_symbol"],
            keep="last",
        ).sort(["sector_tag", "sector_symbol", "member_symbol"])
        count = publish_snapshot(
            self.data_dir,
            THS_SECTOR_CONSTITUENTS_TABLE,
            snapshot_date,
            merged,
        )
        update_ingestion_manifest(
            self.data_dir,
            SOURCE,
            THS_SECTOR_CONSTITUENTS_TABLE,
            logical_snapshot,
            status="published",
            parser_version=PARSER_VERSION,
            schema_version=1,
            source_content_hash=source_hash,
            content_hash=stable_content_hash(merged.to_dicts()),
            published_rows=count,
            provenance="snapshot_frozen",
        )
        logger.info("HiThink sector constituent snapshot published: %d rows", count)
        return count

    def collect_lifecycle_observed(
        self,
        *,
        observed_as_of: date,
        daily_rows: pl.DataFrame,
    ) -> int:
        current_tickers = self.client.list_tickers(exchange="SH,SZ,BJ", asset_type="a-share")
        frame = normalize_lifecycle_observed(
            current_tickers=current_tickers,
            daily_rows=daily_rows,
            observed_as_of=observed_as_of,
        )
        logical_snapshot = observed_as_of.isoformat()
        _, source_hash = archive_source_payload(
            self.data_dir,
            SOURCE,
            INSTRUMENT_LIFECYCLE_TABLE,
            logical_snapshot,
            "current-tickers",
            current_tickers,
            parser_version=PARSER_VERSION,
        )
        count = publish_snapshot(
            self.data_dir,
            INSTRUMENT_LIFECYCLE_TABLE,
            observed_as_of,
            frame,
        )
        update_ingestion_manifest(
            self.data_dir,
            SOURCE,
            INSTRUMENT_LIFECYCLE_TABLE,
            logical_snapshot,
            status="published" if count else "valid_empty",
            parser_version=PARSER_VERSION,
            schema_version=1,
            source_content_hash=source_hash,
            content_hash=stable_content_hash(frame.to_dicts()) if count else None,
            published_rows=count,
            provenance="observed",
            empty_reason=None if count else "source_empty",
        )
        logger.info("HiThink observed lifecycle snapshot published: %d rows", count)
        return count
