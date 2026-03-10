import pandas as pd
import sqlite3
import time
from typing import Dict, List
from datetime import date

def preload_historical_data(
    tickers: List[str],
    start_date: str,
    end_date: str,
    cache_db: str = "data/backtest_cache.db"
) -> Dict[str, pd.DataFrame]:
    """Download and cache OHLCV data using AkShare with SQLite backend."""
    import os
    os.makedirs(os.path.dirname(cache_db), exist_ok=True)
    
    # Initialize DB
    conn = sqlite3.connect(cache_db)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_bars (
            ticker TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            UNIQUE(ticker, date)
        )
    """)
    conn.commit()
    
    # Check what we already have
    cached_tickers = set(r[0] for r in conn.execute("SELECT DISTINCT ticker FROM daily_bars").fetchall())
    missing_tickers = [t for t in tickers if t not in cached_tickers]
    
    if missing_tickers:
        print(f"Downloading historical data for {len(missing_tickers)} missing tickers...")
        from core.cn_data import download_daily_range
        # Download in chunks to avoid overwhelming memory/API
        chunk_size = 100
        for i in range(0, len(missing_tickers), chunk_size):
            chunk = missing_tickers[i:i+chunk_size]
            data_map, _ = download_daily_range(chunk, start_date, end_date, threads=True)
            
            # Save chunk to DB
            cur = conn.cursor()
            for ticker, df in data_map.items():
                if df.empty: continue
                records = []
                for dt, row in df.iterrows():
                    dt_str = dt.strftime("%Y-%m-%d") if isinstance(dt, pd.Timestamp) else str(dt)[:10]
                    records.append((ticker, dt_str, row['Open'], row['High'], row['Low'], row['Close'], row['Volume']))
                cur.executemany("""
                    INSERT OR IGNORE INTO daily_bars (ticker, date, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, records)
            conn.commit()
            time.sleep(1) # brief pause
            
    # Load requested data from DB
    result = {}
    print("Loading backtest data from SQLite cache...")
    
    for ticker in tickers:
        df = pd.read_sql_query(
            "SELECT date as Date, open as Open, high as High, low as Low, close as Close, volume as Volume FROM daily_bars WHERE ticker = ? ORDER BY date",
            conn,
            params=(ticker,),
            parse_dates=['Date']
        )
        if not df.empty:
            df.set_index('Date', inplace=True)
            result[ticker] = df
            
    conn.close()
    return result

def preload_csi300_data(start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch CSI 300 index data."""
    try:
        import akshare as ak
        start_str = start_date.replace("-", "")
        end_str = end_date.replace("-", "")
        df = ak.stock_zh_a_hist(symbol="000300", start_date=start_str, end_date=end_str, period="daily")
        
        rename_map = {"日期": "Date", "开盘": "Open", "最高": "High", "最低": "Low", "收盘": "Close", "成交量": "Volume"}
        df = df.rename(columns=rename_map)
        df["Date"] = pd.to_datetime(df["Date"])
        df.set_index("Date", inplace=True)
        return df[["Open", "High", "Low", "Close", "Volume"]]
    except Exception as e:
        print(f"Failed to fetch CSI300: {e}")
        return pd.DataFrame()
