#!/usr/bin/env python3
"""Serve a small live dashboard for a Tushare backfill manifest."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from typing import Any


FAILED_STATUSES = {"blocked", "error", "failed", "source_error"}


def _phase_snapshot(state: dict[str, Any], total: int) -> dict[str, Any]:
    items = state.get("items") or {}
    counts = Counter(str(item.get("status", "unknown")) for item in items.values())
    completed = counts["completed"]
    running = counts["running"]
    failed = sum(counts[name] for name in FAILED_STATUSES)
    explicit_pending = counts["pending"]
    untouched = max(0, total - len(items))
    pending = explicit_pending + untouched
    percent = round(completed / total * 100, 1) if total else 0.0
    return {
        "completed": completed,
        "failed": failed,
        "pending": pending,
        "percent": percent,
        "running": running,
        "status": state.get("status", "pending"),
        "total": total,
    }


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def build_snapshot(manifest_path: Path, *, now: datetime | None = None) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    phases = manifest.get("phases_state") or {}
    universe = phases.get("universe") or {}

    stock_state = phases.get("stock_minute") or {}
    etf_state = phases.get("etf_minute") or {}
    adjustment_state = phases.get("adjustment") or {}
    stock_total = int(universe.get("stock_count") or len(stock_state.get("items") or {}))
    etf_total = int(universe.get("etf_count") or len(etf_state.get("items") or {}))
    adjustment_total = len(adjustment_state.get("items") or {})

    stock = _phase_snapshot(stock_state, stock_total)
    etf = _phase_snapshot(etf_state, etf_total)
    adjustment = _phase_snapshot(adjustment_state, adjustment_total)
    minute_total = stock_total + etf_total
    minute_completed = stock["completed"] + etf["completed"]
    minute_running = stock["running"] + etf["running"]
    minute_failed = stock["failed"] + etf["failed"]

    checked_at = now or datetime.now(timezone.utc)
    updated_at = _parse_timestamp(manifest.get("updated_at"))
    age_seconds = max(0, int((checked_at - updated_at).total_seconds())) if updated_at else None
    minute_percent = round(minute_completed / minute_total * 100, 1) if minute_total else 0.0

    if minute_failed:
        activity = "failed"
        activity_label = "存在失败项"
    elif minute_total and minute_completed >= minute_total:
        activity = "completed"
        activity_label = "分钟数据已完成"
    elif minute_running and age_seconds is not None and age_seconds > 120:
        activity = "waiting"
        activity_label = "等待接口响应"
    elif minute_running:
        activity = "running"
        activity_label = "正在下载"
    else:
        activity = "pending"
        activity_label = "等待开始"

    return {
        "activity": activity,
        "activity_label": activity_label,
        "age_seconds": age_seconds,
        "history_end": manifest.get("history_end"),
        "history_start": manifest.get("history_start"),
        "minute": {
            "completed": minute_completed,
            "failed": minute_failed,
            "percent": minute_percent,
            "running": minute_running,
            "total": minute_total,
        },
        "phases": {
            "adjustment": adjustment,
            "etf_minute": etf,
            "stock_minute": stock,
        },
        "run_id": manifest.get("run_id") or manifest_path.parent.name,
        "status": manifest.get("status", "unknown"),
        "updated_at": manifest.get("updated_at"),
    }


INDEX_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="icon" href="data:,">
  <title>TickFlow 数据进度</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f3f4f1;
      --surface: #ffffff;
      --ink: #171a18;
      --muted: #6d736e;
      --line: #dfe2dd;
      --track: #e8ebe6;
      --accent: #138a5b;
      --accent-soft: #dff2e9;
      --danger: #c4473d;
      --waiting: #b36b22;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      min-width: 320px;
      min-height: 100vh;
      background: var(--bg);
      color: var(--ink);
      font-family: Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      letter-spacing: 0;
    }

    main {
      width: min(100% - 40px, 920px);
      margin: 0 auto;
      padding: 48px 0 36px;
      animation: enter 360ms ease-out both;
    }

    header {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 24px;
      padding-bottom: 28px;
      border-bottom: 1px solid var(--line);
    }

    .eyebrow {
      margin: 0 0 8px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
    }

    h1 {
      margin: 0;
      font-size: 30px;
      line-height: 1.2;
      font-weight: 720;
    }

    .scope {
      margin: 8px 0 0;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.5;
    }

    .live-status {
      display: inline-flex;
      align-items: center;
      gap: 9px;
      min-height: 28px;
      color: var(--accent);
      font-size: 14px;
      font-weight: 700;
      white-space: nowrap;
    }

    .dot {
      width: 9px;
      height: 9px;
      border-radius: 50%;
      background: currentColor;
      box-shadow: 0 0 0 5px var(--accent-soft);
    }

    .running .dot { animation: pulse 1.8s ease-out infinite; }
    .waiting { color: var(--waiting); }
    .failed { color: var(--danger); }

    .overview { padding: 46px 0 42px; }

    .overview-top {
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 24px;
      margin-bottom: 20px;
    }

    .overview h2 {
      margin: 0 0 10px;
      font-size: 15px;
      font-weight: 700;
    }

    .fraction {
      color: var(--muted);
      font-size: 14px;
    }

    .percent {
      font-variant-numeric: tabular-nums;
      font-size: 52px;
      line-height: 0.95;
      font-weight: 760;
    }

    .track {
      position: relative;
      width: 100%;
      height: 18px;
      overflow: hidden;
      border-radius: 4px;
      background: var(--track);
    }

    .fill {
      width: 0;
      height: 100%;
      border-radius: inherit;
      background: var(--accent);
      transition: width 700ms cubic-bezier(.2,.8,.2,1), background-color 200ms ease;
    }

    .failed .fill { background: var(--danger); }

    .metrics {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      margin-top: 26px;
      border-top: 1px solid var(--line);
      border-bottom: 1px solid var(--line);
    }

    .metric { padding: 18px 0; }
    .metric + .metric { padding-left: 24px; border-left: 1px solid var(--line); }
    .metric-label { display: block; color: var(--muted); font-size: 12px; }
    .metric-value {
      display: block;
      margin-top: 5px;
      font-size: 22px;
      font-weight: 720;
      font-variant-numeric: tabular-nums;
    }

    .phases { border-top: 1px solid var(--line); }

    .phase {
      display: grid;
      grid-template-columns: 165px minmax(200px, 1fr) 118px;
      align-items: center;
      gap: 24px;
      padding: 24px 0;
      border-bottom: 1px solid var(--line);
    }

    .phase-name { font-size: 15px; font-weight: 700; }
    .phase-meta { margin-top: 5px; color: var(--muted); font-size: 12px; }
    .phase .track { height: 9px; }
    .phase-number { text-align: right; font-size: 14px; font-variant-numeric: tabular-nums; }
    .phase-percent { display: block; margin-top: 5px; color: var(--muted); font-size: 12px; }

    footer {
      display: flex;
      justify-content: space-between;
      gap: 24px;
      padding-top: 22px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.5;
    }

    .error { color: var(--danger); }

    @keyframes enter {
      from { opacity: 0; transform: translateY(8px); }
      to { opacity: 1; transform: translateY(0); }
    }

    @keyframes pulse {
      0% { box-shadow: 0 0 0 0 rgba(19, 138, 91, .32); }
      70%, 100% { box-shadow: 0 0 0 8px rgba(19, 138, 91, 0); }
    }

    @media (max-width: 640px) {
      main { width: min(100% - 28px, 920px); padding-top: 28px; }
      header { display: block; }
      .live-status { margin-top: 18px; }
      .overview { padding: 36px 0 32px; }
      .percent { font-size: 42px; }
      .phase { grid-template-columns: 1fr auto; gap: 14px; }
      .phase-progress { grid-column: 1 / -1; grid-row: 2; }
      .phase-number { grid-column: 2; grid-row: 1; }
      .metrics { grid-template-columns: 1fr; }
      .metric { display: flex; align-items: center; justify-content: space-between; padding: 13px 0; }
      .metric + .metric { padding-left: 0; border-left: 0; border-top: 1px solid var(--line); }
      .metric-value { margin-top: 0; font-size: 18px; }
      footer { display: block; }
      footer span { display: block; margin-bottom: 5px; }
    }

    @media (prefers-reduced-motion: reduce) {
      *, *::before, *::after { animation: none !important; transition: none !important; }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <p class="eyebrow">TickFlow / Tushare Proxy</p>
        <h1>历史数据回填</h1>
        <p class="scope" id="scope">正在读取任务范围…</p>
      </div>
      <div class="live-status pending" id="live-status"><span class="dot"></span><span>连接中</span></div>
    </header>

    <section class="overview" aria-labelledby="minute-title">
      <div class="overview-top">
        <div>
          <h2 id="minute-title">分钟数据总进度</h2>
          <span class="fraction" id="minute-fraction">0 / 0</span>
        </div>
        <div class="percent" id="minute-percent">0.0%</div>
      </div>
      <div class="track" role="progressbar" aria-label="分钟数据总进度" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0">
        <div class="fill" id="minute-fill"></div>
      </div>
      <div class="metrics">
        <div class="metric"><span class="metric-label">正在处理</span><strong class="metric-value" id="running">0</strong></div>
        <div class="metric"><span class="metric-label">失败</span><strong class="metric-value" id="failed">0</strong></div>
        <div class="metric"><span class="metric-label">剩余</span><strong class="metric-value" id="remaining">0</strong></div>
      </div>
    </section>

    <section class="phases" aria-label="分阶段进度">
      <div class="phase" data-phase="stock_minute">
        <div><div class="phase-name">股票分钟</div><div class="phase-meta" data-role="meta"></div></div>
        <div class="phase-progress"><div class="track" role="progressbar" aria-label="股票分钟进度"><div class="fill"></div></div></div>
        <div class="phase-number"><span data-role="fraction">0 / 0</span><span class="phase-percent" data-role="percent">0.0%</span></div>
      </div>
      <div class="phase" data-phase="etf_minute">
        <div><div class="phase-name">ETF 分钟</div><div class="phase-meta" data-role="meta"></div></div>
        <div class="phase-progress"><div class="track" role="progressbar" aria-label="ETF 分钟进度"><div class="fill"></div></div></div>
        <div class="phase-number"><span data-role="fraction">0 / 0</span><span class="phase-percent" data-role="percent">0.0%</span></div>
      </div>
      <div class="phase" data-phase="adjustment">
        <div><div class="phase-name">复权因子</div><div class="phase-meta" data-role="meta"></div></div>
        <div class="phase-progress"><div class="track" role="progressbar" aria-label="复权因子进度"><div class="fill"></div></div></div>
        <div class="phase-number"><span data-role="fraction">0 / 0</span><span class="phase-percent" data-role="percent">0.0%</span></div>
      </div>
    </section>

    <footer>
      <span id="run-id">任务：—</span>
      <span id="updated">最近更新：—</span>
    </footer>
  </main>

  <script>
    const number = new Intl.NumberFormat('zh-CN');

    function phaseMeta(phase) {
      const parts = [];
      if (phase.running) parts.push(`${phase.running} 正在处理`);
      if (phase.failed) parts.push(`${phase.failed} 失败`);
      if (!parts.length) parts.push(phase.completed >= phase.total && phase.total ? '已完成' : '等待处理');
      return parts.join(' · ');
    }

    function renderPhase(name, phase) {
      const root = document.querySelector(`[data-phase="${name}"]`);
      root.querySelector('[data-role="fraction"]').textContent = `${number.format(phase.completed)} / ${number.format(phase.total)}`;
      root.querySelector('[data-role="percent"]').textContent = `${phase.percent.toFixed(1)}%`;
      root.querySelector('[data-role="meta"]').textContent = phaseMeta(phase);
      root.querySelector('.fill').style.width = `${phase.percent}%`;
      root.querySelector('[role="progressbar"]').setAttribute('aria-valuenow', phase.percent);
      root.classList.toggle('failed', phase.failed > 0);
    }

    function render(data) {
      const minute = data.minute;
      const remaining = Math.max(0, minute.total - minute.completed);
      document.getElementById('scope').textContent = `${data.history_start || '—'} 至 ${data.history_end || '—'} · ${data.run_id}`;
      document.getElementById('minute-fraction').textContent = `${number.format(minute.completed)} / ${number.format(minute.total)}`;
      document.getElementById('minute-percent').textContent = `${minute.percent.toFixed(1)}%`;
      document.getElementById('minute-fill').style.width = `${minute.percent}%`;
      document.querySelector('.overview [role="progressbar"]').setAttribute('aria-valuenow', minute.percent);
      document.getElementById('running').textContent = number.format(minute.running);
      document.getElementById('failed').textContent = number.format(minute.failed);
      document.getElementById('remaining').textContent = number.format(remaining);
      document.getElementById('run-id').textContent = `任务：${data.run_id}`;
      document.getElementById('updated').textContent = `最近更新：${data.updated_at ? new Date(data.updated_at).toLocaleString('zh-CN', { hour12: false }) : '—'}`;
      document.getElementById('updated').classList.remove('error');

      const status = document.getElementById('live-status');
      status.className = `live-status ${data.activity}`;
      status.querySelector('span:last-child').textContent = data.activity_label;
      document.querySelector('.overview').classList.toggle('failed', minute.failed > 0);

      renderPhase('stock_minute', data.phases.stock_minute);
      renderPhase('etf_minute', data.phases.etf_minute);
      renderPhase('adjustment', data.phases.adjustment);
    }

    async function refresh() {
      try {
        const response = await fetch('/api/progress', { cache: 'no-store' });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        render(await response.json());
      } catch (error) {
        const status = document.getElementById('live-status');
        status.className = 'live-status failed';
        status.querySelector('span:last-child').textContent = '读取失败';
        document.getElementById('updated').textContent = `读取失败：${error.message}`;
        document.getElementById('updated').classList.add('error');
      }
    }

    refresh();
    setInterval(refresh, 2000);
  </script>
</body>
</html>
"""


def make_handler(manifest_path: Path) -> type[BaseHTTPRequestHandler]:
    class ProgressHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/api/progress":
                try:
                    payload = json.dumps(build_snapshot(manifest_path), ensure_ascii=False).encode()
                except (OSError, ValueError, json.JSONDecodeError) as exc:
                    self._send_json({"error": str(exc)}, status=500)
                    return
                self._send(payload, "application/json; charset=utf-8")
                return
            if self.path in {"/", "/index.html"}:
                self._send(INDEX_HTML.encode(), "text/html; charset=utf-8")
                return
            self._send_json({"error": "not found"}, status=404)

        def _send_json(self, value: dict[str, Any], *, status: int) -> None:
            self._send(
                json.dumps(value, ensure_ascii=False).encode(),
                "application/json; charset=utf-8",
                status=status,
            )

        def _send(self, payload: bytes, content_type: str, *, status: int = 200) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            return

    return ProgressHandler


def main() -> None:
    parser = argparse.ArgumentParser(description="实时显示 Tushare 历史回填进度")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8877)
    args = parser.parse_args()
    manifest_path = args.manifest.expanduser().resolve()
    if not manifest_path.is_file():
        parser.error(f"manifest not found: {manifest_path}")

    server = ThreadingHTTPServer((args.host, args.port), make_handler(manifest_path))
    print(f"Backfill progress: http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
