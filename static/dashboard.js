/* Dragon Pulse — Vanilla JS Dashboard */

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

function buildNameMap(...groups) {
    const entries = groups.flat().filter(item => item?.ticker && item?.name);
    return Object.fromEntries(entries.map(item => [item.ticker, item.name]));
}

function uniqueTickers(...groups) {
    return [...new Set(groups.flat().filter(Boolean))];
}

function overlapChip(ticker, nameMap) {
    const name = nameMap[ticker];
    if (name) return `<span class="overlap-chip">${name} <span class="ticker-code">${ticker}</span></span>`;
    return `<span class="overlap-chip">${ticker}</span>`;
}

function regimeBadge(regime) {
    if (!regime) return '';
    const r = regime.toLowerCase();
    const emoji = { bull: '\u{1F7E2}', bear: '\u{1F534}', choppy: '\u{1F7E1}', caution: '\u{1F7E1}' }[r] || '\u26AA';
    const cls = `regime-${r.includes('caution') ? 'caution' : r}`;
    return `<span class="regime-badge ${cls}">${emoji} ${regime.toUpperCase()}</span>`;
}

function sourceTag(src) {
    const s = src.toLowerCase();
    let cls = 'weekly';
    if (s.includes('pro30') || s.includes('30d')) cls = 'pro30';
    else if (s.includes('mover')) cls = 'movers';
    return `<span class="source-tag ${cls}">${src}</span>`;
}

function confidenceTag(conf) {
    if (!conf) return '';
    const c = conf.toLowerCase();
    let cls = 'conf-medium';
    if (c === 'high') cls = 'conf-high';
    else if (c === 'speculative') cls = 'conf-speculative';
    return `<span class="confidence-tag ${cls}">${conf}</span>`;
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
        html(el, '<div class="empty">No scan data yet.<br>Run <code>python main.py all</code> to generate picks.</div>');
    }
}

function renderPicks(data) {
    const summary = data.summary || {};
    const overlaps = data.overlaps || {};
    const hybrid = data.hybrid_top3 || [];
    const weekly = data.weekly_top5 || data.primary_top5 || [];
    const nameMap = {
        ...(data.overlap_name_map || {}),
        ...buildNameMap(hybrid, weekly),
    };
    const regime = data.regime || summary.regime || '';
    const date = data.date || data.asof || '';
    const label = data.primary_label || 'Weekly';

    let out = '';

    // ── Header: date + regime + counts
    out += '<div class="page-header">';
    out += `<div class="page-date">${date || 'Latest Scan'}</div>`;
    out += '<div class="page-subtitle">';
    if (regime) out += regimeBadge(regime);
    out += `<div class="stat-pills">
        <span class="stat-pill">${label} <b>${summary.weekly_top5_count ?? weekly.length}</b></span>
        <span class="stat-pill">Pro30 <b>${summary.pro30_candidates_count ?? 0}</b></span>
        <span class="stat-pill">Movers <b>${summary.movers_count ?? 0}</b></span>
    </div>`;
    out += '</div></div>';

    // ── Overlaps callout (most important — show first)
    const allThree = uniqueTickers(overlaps.all_three || []);
    const overlapTickers = uniqueTickers(
        (overlaps.weekly_pro30 || overlaps.primary_pro30 || []).filter(t => !allThree.includes(t)),
        (overlaps.weekly_movers || overlaps.primary_movers || []).filter(t => !allThree.includes(t)),
        (overlaps.pro30_movers || []).filter(t => !allThree.includes(t)),
    );

    if (allThree.length) {
        out += `<div class="overlap-callout">
            <div class="overlap-header">\u2B50 All Three Systems Agree</div>
            <div class="overlap-items">${allThree.map(t => overlapChip(t, nameMap)).join('')}</div>
        </div>`;
    }
    if (overlapTickers.length) {
        out += `<div class="overlap-callout overlap-callout-green">
            <div class="overlap-header">\u{1F3AF} Overlaps</div>
            <div class="overlap-items">${overlapTickers.map(t => overlapChip(t, nameMap)).join('')}</div>
        </div>`;
    }

    // ── Hybrid Top 3
    if (hybrid.length) {
        out += `<div class="section-title">\u{1F3C6} Hybrid Top ${Math.min(3, hybrid.length)}</div>`;
        for (const p of hybrid.slice(0, 3)) {
            const score = p.hybrid_score ?? p.composite_score ?? 0;
            const sources = (p.sources || []).map(sourceTag).join(' ');
            const conf = p.confidence || '';
            out += `<div class="pick-card">
                <div class="pick-rank">${p.rank ?? ''}</div>
                <div class="pick-body">
                    <div class="pick-name">${tickerDisplay(p.ticker, p.name)}</div>
                    <div class="pick-meta">${sources} ${confidenceTag(conf)}</div>
                </div>
                <div class="pick-right">
                    <div class="pick-score-value">${Number(score).toFixed(0)}<span class="pick-score-max">/100</span></div>
                    ${miniBar(score, 100)}
                </div>
            </div>`;
        }
    }

    // ── Weekly/Primary Top 5
    if (weekly.length) {
        out += `<div class="section-title">${label} Top ${Math.min(5, weekly.length)}</div>`;
        for (const p of weekly.slice(0, 5)) {
            const score = p.composite_score ?? p.swing_score ?? 0;
            const conf = p.confidence || '';
            out += `<div class="pick-card">
                <div class="pick-rank">${p.rank ?? ''}</div>
                <div class="pick-body">
                    <div class="pick-name">${tickerDisplay(p.ticker, p.name)}</div>
                    <div class="pick-meta">${confidenceTag(conf)}</div>
                </div>
                <div class="pick-right">
                    <div class="pick-score-value">${Number(score).toFixed(1)}<span class="pick-score-max">/10</span></div>
                    ${miniBar(score, 10)}
                </div>
            </div>`;
        }
    }

    if (!hybrid.length && !weekly.length) {
        out += '<div class="empty">No picks for this date.</div>';
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
    const withOverlaps = runs.filter(r => r.has_overlaps).length;
    const overlapPct = total ? ((withOverlaps / total) * 100).toFixed(0) : 0;

    let out = `<div class="kpi-row">
        <div class="kpi"><div class="kpi-value">${total}</div><div class="kpi-label">Runs</div></div>
        <div class="kpi"><div class="kpi-value">${withOverlaps}</div><div class="kpi-label">With Overlaps</div></div>
        <div class="kpi"><div class="kpi-value">${overlapPct}%</div><div class="kpi-label">Overlap Rate</div></div>
    </div>`;

    out += '<div class="section-title">Run History</div>';
    for (const r of runs) {
        out += `<div class="run-item" onclick="window.open('/runs/${r.date}','_blank')">
            <div class="run-date">${r.date}</div>
            <div class="run-counts">
                <span>Weekly <b>${r.weekly_top5_count}</b></span>
                <span>Pro30 <b>${r.pro30_candidates_count}</b></span>
                <span>Movers <b>${r.movers_count}</b></span>
            </div>
            <div class="run-summary">${r.weekly_top5_count}W • ${r.pro30_candidates_count}P • ${r.movers_count}M</div>
            ${r.has_overlaps ? '<span class="run-overlap-badge">OVERLAP</span>' : ''}
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

    let out = '<div class="card"><div class="card-title">Health</div>';
    out += `<div class="info-row"><span class="info-key">Status</span><span class="info-val">${health.status}</span></div>`;
    out += `<div class="info-row"><span class="info-key">Timestamp</span><span class="info-val">${health.timestamp}</span></div>`;
    out += `<div class="info-row"><span class="info-key">Version</span><span class="info-val">${health.version}</span></div>`;
    out += '</div>';

    out += '<div class="card"><div class="card-title">Last Run</div>';
    if (dates.length) {
        const latest = dates[0];
        out += `<div class="info-row"><span class="info-key">Date</span><span class="info-val">${latest}</span></div>`;
        out += `<a class="api-link" href="/runs/${latest}" target="_blank"><span>Open raw JSON</span><span class="api-path">/runs/${latest}</span></a>`;
    } else {
        out += '<div class="info-row"><span class="info-key">Status</span><span class="info-val">No runs available</span></div>';
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
