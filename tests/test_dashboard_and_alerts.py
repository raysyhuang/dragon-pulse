from __future__ import annotations

import subprocess
import textwrap
from pathlib import Path

from src.core.alerts import AlertConfig, AlertManager


def test_send_telegram_avoids_duplicate_header(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "chat")

    manager = AlertManager(AlertConfig(enabled=True, channels=["telegram"]))
    sent_messages: list[str] = []

    def capture_message(token: str, chat_id: str, text: str) -> bool:
        sent_messages.append(text)
        return True

    monkeypatch.setattr(manager, "_send_telegram_message", capture_message)

    title = "🐉 Dragon Pulse — 2026-03-10"
    message = f"{title}\n\n🟢 Regime: <b>BULL</b> | Picks: <b>3</b>"

    assert manager._send_telegram(title, message, {"asof": "2026-03-10"}, "normal") is True
    assert sent_messages == [message]


def test_dashboard_overlap_chips_include_company_names():
    script = textwrap.dedent(
        """
        const fs = require('fs');
        const vm = require('vm');

        global.document = {
          querySelector: () => null,
          querySelectorAll: () => [],
          addEventListener: () => {},
          createElement: () => ({ classList: { add() {}, remove() {} } }),
          body: { appendChild() {} },
        };
        global.window = { open() {} };
        global.setTimeout = () => 0;

        const source = fs.readFileSync('static/dashboard.js', 'utf8');
        vm.runInThisContext(source);

        const rendered = renderPicks({
          date: '2026-03-10',
          summary: {},
          hybrid_top3: [
            {
              ticker: '600519.SH',
              name: '贵州茅台',
              rank: 1,
              hybrid_score: 82,
              sources: ['Weekly'],
            },
          ],
          primary_top5: [
            {
              ticker: '600519.SH',
              name: '贵州茅台',
              rank: 1,
              composite_score: 8.2,
            },
          ],
          overlaps: {
            all_three: ['600519.SH'],
          },
        });

        // Overlap chip must contain company name and ticker
        if (!rendered.includes('overlap-chip">贵州茅台')) {
          throw new Error('Missing company name in overlap chip: ' + rendered);
        }
        if (!rendered.includes('600519.SH')) {
          throw new Error('Missing ticker code: ' + rendered);
        }
        // Pick card must also show company name
        if (!rendered.includes('pick-name">贵州茅台')) {
          throw new Error('Missing company name in pick card: ' + rendered);
        }
        """
    )

    result = subprocess.run(
        ["node", "-e", script],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout


def test_dashboard_tab_uses_latest_run_label():
    html = Path("static/dashboard.html").read_text(encoding="utf-8")

    assert "Latest Run" in html
    assert "Today's Picks" not in html


def test_dashboard_dedupes_overlap_chips_and_supports_overlap_name_map():
    script = textwrap.dedent(
        """
        const fs = require('fs');
        const vm = require('vm');

        global.document = {
          querySelector: () => null,
          querySelectorAll: () => [],
          addEventListener: () => {},
          createElement: () => ({ classList: { add() {}, remove() {} } }),
          body: { appendChild() {} },
        };
        global.window = { open() {} };
        global.setTimeout = () => 0;

        const source = fs.readFileSync('static/dashboard.js', 'utf8');
        vm.runInThisContext(source);

        const rendered = renderPicks({
          date: '2026-03-10',
          summary: {},
          overlap_name_map: {
            '601398.SH': '工商银行',
          },
          overlaps: {
            primary_pro30: ['601398.SH'],
            primary_movers: ['601398.SH'],
          },
        });

        const chips = rendered.match(/overlap-chip">/g) || [];
        if (chips.length !== 1) {
          throw new Error('Expected one deduped overlap chip, saw ' + chips.length + ': ' + rendered);
        }
        if (!rendered.includes('工商银行')) {
          throw new Error('Missing overlap-only company name: ' + rendered);
        }
        """
    )

    result = subprocess.run(
        ["node", "-e", script],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout


def test_dashboard_performance_includes_compact_summary_and_slim_system_view():
    script = textwrap.dedent(
        """
        const fs = require('fs');
        const vm = require('vm');

        global.document = {
          querySelector: () => null,
          querySelectorAll: () => [],
          addEventListener: () => {},
          createElement: () => ({ classList: { add() {}, remove() {} } }),
          body: { appendChild() {} },
        };
        global.window = { open() {} };
        global.setTimeout = () => 0;

        const source = fs.readFileSync('static/dashboard.js', 'utf8');
        vm.runInThisContext(source);

        const performanceHtml = renderPerformance({
          runs: [{
            date: '2026-03-10',
            weekly_top5_count: 5,
            pro30_candidates_count: 12,
            movers_count: 3,
            has_overlaps: true,
          }],
        });
        const systemHtml = renderSystem(
          { status: 'healthy', version: '1.0.0', timestamp: '2026-03-10T09:35:00' },
          { runs: [{ date: '2026-03-10' }] },
        );

        if (!performanceHtml.includes('run-summary')) {
          throw new Error('Missing compact run summary: ' + performanceHtml);
        }
        if (!systemHtml.includes('Last Run')) {
          throw new Error('Missing last run card: ' + systemHtml);
        }
        if (systemHtml.includes('Interactive Docs') || systemHtml.includes('/docs')) {
          throw new Error('System view should not expose API docs: ' + systemHtml);
        }
        """
    )

    result = subprocess.run(
        ["node", "-e", script],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout
