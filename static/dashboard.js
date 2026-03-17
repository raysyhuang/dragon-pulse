/* Dragon Pulse v4 — Dashboard */

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

const cache = {};
let currentTab = 'picks';

// ─── Helpers ──────────────────────────────────────

async function fetchJSON(url) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`${res.status}`);
    return res.json();
}

function html(el, content) { el.innerHTML = content; }

function tickerDisplay(ticker, name) {
    if (name) return `${name} <span class="ticker-code">${ticker}</span>`;
    return ticker;
}

function regimeBadge(regime) {
    if (!regime) return '';
    const r = regime.toLowerCase();
    const emoji = { bull: '\u{1F7E2}', bear: '\u{1F534}', choppy: '\u{1F7E1}' }[r] || '\u26AA';
    const cls = `regime-${r}`;
    return `<span class="regime-badge ${cls}">${emoji} ${regime.toUpperCase()}</span>`;
}

function acceptanceBadge(mode) {
    if (!mode) return '';
    const m = mode.toLowerCase();
    const cls = {
        full: 'acc-full',
        selective: 'acc-selective',
        abstain: 'acc-abstain',
        breadth_suppressed: 'acc-suppressed',
    }[m] || 'acc-off';
    return `<span class="acceptance-badge ${cls}">${mode.toUpperCase()}</span>`;
}

function miniBar(value, max) {
    const pct = Math.max(0, Math.min(100, (value / max) * 100));
    return `<div class="pick-bar-mini"><div class="pick-bar-fill" style="width:${pct}%"></div></div>`;
}

function loading() {
    return '<div class="loading"><div class="spinner"></div><br>Loading...</div>';
}

function toast(msg) {
    let el = $('#toast');
    if (!el) {
        el = document.createElement('div');
        el.id = 'toast';
        el.className = 'toast';
        document.body.appendChild(el);
    }
    el.textContent = msg;
    el.classList.add('show');
    setTimeout(() => el.classList.remove('show'), 3000);
}

// ─── Router ───────────────────────────────────────

function initRouter() {
    $$('.tab').forEach(tab => {
        tab.addEventListener('click', (e) => {
            e.preventDefault();
            switchTab(tab.dataset.tab);
            $('#nav').classList.remove('open');
        });
    });
    $('#hamburger').addEventListener('click', () => $('#nav').classList.toggle('open'));
    switchTab('picks');
}

function switchTab(name) {
    currentTab = name;
    $$('.tab').forEach(t => t.classList.toggle('active', t.dataset.tab === name));
    $$('.view').forEach(v => v.classList.toggle('active', v.id === `view-${name}`));
    ({ picks: loadPicksView, performance: loadPerformanceView, system: loadSystemView })[name]?.();
}

// ─── Picks View ───────────────────────────────────

async function loadPicksView() {
    const el = $('#view-picks');
    if (cache.picks) { html(el, cache.picks); return; }
    html(el, loading());
    try {
        const data = await fetchJSON('/api/latest');
        cache.picks = renderPicks(data);
        html(el, cache.picks);
    } catch {
        html(el, '<div class="empty">No scan data yet.<br>Run <code>python main.py scan</code> to generate picks.</div>');
    }
}

function renderPicks(data) {
    const picks = data.picks || [];
    const rd = data.regime_detail || {};
    const regime = data.regime || '';
    const date = data.date || '';
    const signalsTotal = data.signals_total || 0;
    const eligible = rd.acceptance_eligible_count || 0;
    const dqScore = rd.day_quality_score || 0;
    const accMode = rd.acceptance_mode || '';
    const breadth = rd.market_breadth_pct_above_sma20;

    let out = '';

    // ── Header
    out += '<div class="page-header">';
    out += `<div class="page-date">${date || 'Latest Scan'}</div>`;
    out += '<div class="page-subtitle">';
    if (regime) out += regimeBadge(regime);
    if (breadth != null) out += `<span class="stat-pill">Breadth ${(breadth * 100).toFixed(0)}%</span>`;
    out += '</div></div>';

    // ── Acceptance stats
    out += '<div class="acceptance-stats">';
    out += `<span class="stat-pill">MR Signals <b>${signalsTotal}</b></span>`;
    out += `<span class="stat-pill">Eligible <b>${eligible}</b></span>`;
    out += `<span class="stat-pill">DQ <b>${dqScore.toFixed(0)}</b>/100</span>`;
    out += acceptanceBadge(accMode);
    out += `<span class="stat-pill">Picks <b>${picks.length}</b></span>`;
    out += '</div>';

    // ── Picks
    if (!picks.length) {
        if (accMode === 'breadth_suppressed') {
            out += '<div class="empty">\u{1F4C9} Breadth suppressed — no picks today.</div>';
        } else if (accMode === 'abstain') {
            out += '<div class="empty">\u23F8 Day quality too low — abstained.</div>';
        } else {
            out += '<div class="empty">No picks for this date.</div>';
        }
    } else {
        out += `<div class="section-title">Mean Reversion Picks</div>`;
        for (let i = 0; i < picks.length; i++) {
            const p = picks[i];
            const name = p.name_cn || p.name || '';
            const score = p.score || 0;
            const maxEntry = p.max_entry_price ? ` max=\u00A5${p.max_entry_price.toFixed(2)}` : '';
            out += `<div class="pick-card">
                <div class="pick-rank">${i + 1}</div>
                <div class="pick-body">
                    <div class="pick-name">${tickerDisplay(p.ticker, name)}</div>
                    <div class="pick-meta">
                        Entry: \u00A5${p.entry_price?.toFixed(2) || '—'}${maxEntry} |
                        Stop: \u00A5${p.stop_loss?.toFixed(2) || '—'} |
                        T1: \u00A5${p.target_1?.toFixed(2) || '—'} |
                        Hold: ${p.holding_period || 3}d
                    </div>
                    ${p.reason_summary ? `<div class="pick-reason">${p.reason_summary}</div>` : ''}
                </div>
                <div class="pick-right">
                    <div class="pick-score-value">${Number(score).toFixed(0)}<span class="pick-score-max">/100</span></div>
                    ${miniBar(score, 100)}
                </div>
            </div>`;
        }
    }

    return out;
}

// ─── Performance View ─────────────────────────────

async function loadPerformanceView() {
    const el = $('#view-performance');
    if (cache.perf) { html(el, cache.perf); return; }
    html(el, loading());
    try {
        const data = await fetchJSON('/runs?limit=30');
        cache.perf = renderPerformance(data);
        html(el, cache.perf);
    } catch {
        html(el, '<div class="empty">Failed to load run history.</div>');
    }
}

function renderPerformance(data) {
    const runs = data.runs || [];
    if (!runs.length) return '<div class="empty">No runs found.</div>';

    const total = runs.length;
    const avgPicks = (runs.reduce((s, r) => s + r.picks_count, 0) / total).toFixed(1);
    const avgDQ = (runs.reduce((s, r) => s + r.day_quality_score, 0) / total).toFixed(0);

    let out = `<div class="kpi-row">
        <div class="kpi"><div class="kpi-value">${total}</div><div class="kpi-label">Runs</div></div>
        <div class="kpi"><div class="kpi-value">${avgPicks}</div><div class="kpi-label">Avg Picks/Day</div></div>
        <div class="kpi"><div class="kpi-value">${avgDQ}</div><div class="kpi-label">Avg Day Quality</div></div>
    </div>`;

    out += '<div class="section-title">Run History</div>';
    for (const r of runs) {
        out += `<div class="run-item" onclick="window.open('/runs/${r.date}','_blank')">
            <div class="run-date">${r.date}</div>
            <div class="run-counts">
                ${regimeBadge(r.regime)}
                ${acceptanceBadge(r.acceptance_mode)}
                <span>Picks <b>${r.picks_count}</b></span>
                <span>Signals <b>${r.signals_total}</b></span>
                <span>DQ <b>${r.day_quality_score.toFixed(0)}</b></span>
            </div>
        </div>`;
    }

    return out;
}

// ─── System View ──────────────────────────────────

async function loadSystemView() {
    const el = $('#view-system');
    if (cache.sys) { html(el, cache.sys); return; }
    html(el, loading());
    try {
        const [health, runs] = await Promise.all([
            fetchJSON('/health'),
            fetchJSON('/runs?limit=1'),
        ]);
        cache.sys = renderSystem(health, runs);
        html(el, cache.sys);
    } catch {
        html(el, '<div class="empty">Failed to load system info.</div>');
    }
}

function renderSystem(health, runs) {
    const dates = (runs.runs || []).map(r => r.date);

    let out = '<div class="card"><div class="card-title">System</div>';
    out += `<div class="info-row"><span class="info-key">Status</span><span class="info-val">${health.status}</span></div>`;
    out += `<div class="info-row"><span class="info-key">Version</span><span class="info-val">${health.version}</span></div>`;
    out += `<div class="info-row"><span class="info-key">Engine</span><span class="info-val">Mean Reversion (MR-only)</span></div>`;
    out += `<div class="info-row"><span class="info-key">Stop</span><span class="info-val">0.95\u00D7 ATR</span></div>`;
    out += `<div class="info-row"><span class="info-key">Sniper</span><span class="info-val">Quarantined</span></div>`;
    out += `<div class="info-row"><span class="info-key">Acceptance</span><span class="info-val">Live Equivalent</span></div>`;
    out += '</div>';

    out += '<div class="card"><div class="card-title">Last Run</div>';
    if (dates.length) {
        const latest = dates[0];
        out += `<div class="info-row"><span class="info-key">Date</span><span class="info-val">${latest}</span></div>`;
        out += `<a class="api-link" href="/runs/${latest}" target="_blank"><span>Open raw JSON</span><span class="api-path">/runs/${latest}</span></a>`;
    } else {
        out += '<div class="info-row"><span class="info-key">Status</span><span class="info-val">No runs</span></div>';
    }
    out += '</div>';

    out += '<div class="card"><div class="card-title">Telegram</div>';
    out += '<p style="margin-bottom:10px;color:var(--text-muted);font-size:12px">Send a test alert to verify configuration.</p>';
    out += '<button class="btn" id="btn-test-alert" onclick="sendTestAlert()">Send Test Alert</button>';
    out += '</div>';

    return out;
}

async function sendTestAlert() {
    const btn = $('#btn-test-alert');
    if (btn) btn.disabled = true;
    try {
        const res = await fetch('/api/alert/test', { method: 'POST' });
        const data = await res.json();
        toast(data.success ? 'Alert sent!' : `Failed: ${data.error || 'unknown'}`);
    } catch { toast('Request failed'); }
    finally { if (btn) btn.disabled = false; }
}

// ─── Init ─────────────────────────────────────────

document.addEventListener('DOMContentLoaded', initRouter);
