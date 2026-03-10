import sqlite3
import json
from datetime import date

SCHEMA = """
CREATE TABLE IF NOT EXISTS picks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    name_cn TEXT NOT NULL,
    entry_date TEXT NOT NULL,
    entry_price REAL NOT NULL,
    target_price REAL NOT NULL,
    stop_price REAL NOT NULL,
    max_hold_days INTEGER NOT NULL,
    lens TEXT NOT NULL,
    confluence_type TEXT NOT NULL,
    composite_score REAL NOT NULL,
    regime TEXT NOT NULL,
    position_size_mult REAL NOT NULL DEFAULT 1.0,
    status TEXT NOT NULL DEFAULT 'open',
    exit_date TEXT,
    exit_price REAL,
    pnl_pct REAL,
    exit_reason TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
    UNIQUE(ticker, entry_date)
);

CREATE TABLE IF NOT EXISTS equity_curve (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL UNIQUE,
    daily_pnl_pct REAL NOT NULL DEFAULT 0.0,
    cumulative_pnl_pct REAL NOT NULL DEFAULT 0.0,
    open_positions INTEGER NOT NULL DEFAULT 0,
    picks_today INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS evolution_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    generation INTEGER NOT NULL,
    variant_id INTEGER NOT NULL,
    params_json TEXT NOT NULL,
    fitness REAL NOT NULL,
    win_rate REAL NOT NULL,
    profit_factor REAL NOT NULL,
    total_trades INTEGER NOT NULL,
    sharpe REAL,
    max_drawdown_pct REAL NOT NULL,
    train_start TEXT NOT NULL,
    train_end TEXT NOT NULL,
    test_start TEXT,
    test_end TEXT,
    test_win_rate REAL,
    test_fitness REAL,
    created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
);

CREATE INDEX IF NOT EXISTS idx_picks_status ON picks(status);
CREATE INDEX IF NOT EXISTS idx_picks_date ON picks(entry_date);
CREATE INDEX IF NOT EXISTS idx_evolution_gen ON evolution_results(generation);
"""

def init_db(db_path: str = "data/dragon_pulse.db") -> sqlite3.Connection:
    import os
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    conn.commit()
    return conn

def save_pick(conn: sqlite3.Connection, pick: dict, entry_date: str) -> None:
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO picks 
        (ticker, name_cn, entry_date, entry_price, target_price, stop_price, max_hold_days, 
         lens, confluence_type, composite_score, regime, position_size_mult, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')
    """, (
        pick['ticker'], pick['name_cn'], entry_date, pick['entry_price'], 
        pick['target_price'], pick['stop_price'], pick['max_hold_days'],
        pick.get('lens', 'multi'), pick['confluence_type'], pick['composite_score'],
        pick['regime'], pick['position_size_mult']
    ))
    conn.commit()

def get_open_picks(conn: sqlite3.Connection) -> list[dict]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM picks WHERE status = 'open'")
    return [dict(row) for row in cur.fetchall()]

def resolve_pick(conn: sqlite3.Connection, pick_id: int, exit_date: str, exit_price: float, exit_reason: str, pnl_pct: float) -> None:
    cur = conn.cursor()
    cur.execute("""
        UPDATE picks SET status = ?, exit_date = ?, exit_price = ?, exit_reason = ?, pnl_pct = ?
        WHERE id = ?
    """, ("closed", exit_date, exit_price, exit_reason, pnl_pct, pick_id))
    conn.commit()
