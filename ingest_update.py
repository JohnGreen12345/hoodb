# pipelines/ingest_update.py
"""
Smart updater:
- Finds latest price_daily date
- Fetches new bars (Yahoo or IBKR)
- Updates price_daily and daily_features
- Rebuilds weekly/monthly multi_tf_features by default
"""

import argparse
import datetime as dt
import pathlib

import duckdb
import pandas as pd
import yfinance as yf
from features import REGISTRY
from ib_insync import IB, util

from pipelines.build_multi_tf_features import build_multi_tf_features
from pipelines.symbols import (
    canonicalize_list,
    canonicalize_series,
    ibkr_contract,
    ibkr_what_to_show,
    rename_map,
    yahoo_symbol,
)

DB = pathlib.Path("data/stock_hub.duckdb").as_posix()

p = argparse.ArgumentParser()
p.add_argument("--source", choices=["yahoo", "ibkr"], default="yahoo", help="Data source")
p.add_argument("--years", type=float, default=3.0, help="History length if DB empty")
p.add_argument("--skip-multi-tf", action="store_true", help="skip rebuilding multi_tf_features")
args = p.parse_args()

con = duckdb.connect(DB)
RENAMES = rename_map(pathlib.Path("data/ticker_renames.csv"))


def _ensure_daily_features_columns() -> None:
    existing = set(con.sql("PRAGMA table_info('daily_features')").fetchdf()["name"])
    for col, coltype in {
        "open": "DOUBLE",
        "high": "DOUBLE",
        "low": "DOUBLE",
        "close": "DOUBLE",
        "volume": "BIGINT",
    }.items():
        if col not in existing:
            con.execute(f'ALTER TABLE daily_features ADD COLUMN "{col}" {coltype}')
            existing.add(col)


def fetch_yahoo(tickers: list[str], start: dt.date, end: dt.date) -> pd.DataFrame:
    yahoo_tickers = [yahoo_symbol(t) for t in tickers]
    yahoo_to_canonical = {yahoo_symbol(t): t for t in tickers}
    dfyf = yf.download(
        yahoo_tickers,
        start=start,
        end=end + dt.timedelta(1),
        group_by="ticker",
        auto_adjust=False,
        progress=False,
    )
    frames = []
    if isinstance(dfyf.columns, pd.MultiIndex):
        for y in yahoo_tickers:
            if y not in dfyf or dfyf[y].empty:
                continue
            tmp = dfyf[y].copy()
            tmp.columns = tmp.columns.str.lower()
            tmp = tmp.reset_index().rename(columns={"Date": "date"})
            tmp = tmp[["date", "open", "high", "low", "close", "volume"]]
            tmp["ticker"] = yahoo_to_canonical.get(y, y)
            frames.append(tmp)
    else:
        if not tickers or dfyf.empty:
            return pd.DataFrame()
        tmp = dfyf.copy()
        tmp.columns = tmp.columns.str.lower()
        tmp = tmp.reset_index().rename(columns={"Date": "date"})
        tmp = tmp[["date", "open", "high", "low", "close", "volume"]]
        tmp["ticker"] = tickers[0]
        frames.append(tmp)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def fetch_ibkr(tickers: list[str], start: dt.date, end: dt.date) -> pd.DataFrame:
    ib = IB()
    ib.connect("127.0.0.1", 7497, clientId=102)
    frames = []
    days = max(1, (end - start).days + 1)
    duration = f"{days} D" if days <= 365 else f"{(days + 364) // 365} Y"
    for t in tickers:
        contract = ibkr_contract(t)
        bars = []
        for what in ibkr_what_to_show(t):
            bars = ib.reqHistoricalData(
                contract,
                endDateTime=end,
                durationStr=duration,
                barSizeSetting="1 day",
                whatToShow=what,
                useRTH=True,
                formatDate=1,
            )
            if bars:
                break
        if not bars:
            continue
        df = util.df(bars)
        if df is None or df.empty:
            continue
        df = df.rename(columns={"date": "date"})
        df = df[["date", "open", "high", "low", "close", "volume"]]
        df["ticker"] = t
        frames.append(df)
    ib.disconnect()
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


last = con.sql("SELECT MAX(date) AS d FROM price_daily").fetchone()[0]
if last is None:
    start = dt.date.today() - dt.timedelta(days=int(args.years * 365))
else:
    start = last + dt.timedelta(days=1)
end = dt.date.today()
if start > end:
    start = end

tickers = con.sql(
    """
    SELECT ticker FROM universe
    WHERE active_flag = TRUE AND type IN ('stock','etf','benchmark')
"""
).fetchdf()["ticker"].tolist()
tickers = canonicalize_list(tickers, RENAMES)
_ensure_daily_features_columns()

raw_new = fetch_ibkr(tickers, start, end) if args.source == "ibkr" else fetch_yahoo(tickers, start, end)
if raw_new.empty:
    print("⚠ No new bars returned")
    raise SystemExit(0)

raw_new["ticker"] = canonicalize_series(raw_new["ticker"], RENAMES)

con.register("tmp", raw_new)
con.execute(
    """
INSERT OR REPLACE INTO price_daily
SELECT date::DATE, ticker, open, high, low, close, volume FROM tmp
"""
)
con.unregister("tmp")
print(f"[+] price_daily updated ({len(raw_new)} new rows)")

lookback_days = 400
if last:
    ticker_csv = ",".join([f"'{t}'" for t in tickers])
    ctx = con.sql(
        f"""
        SELECT date,ticker,open,high,low,close,volume
        FROM price_daily
        WHERE date >= DATE '{last}' - INTERVAL '{lookback_days} days'
          AND date <= DATE '{last}'
          AND ticker IN ({ticker_csv})
    """
    ).fetchdf()
    raw = pd.concat([ctx, raw_new], ignore_index=True) if not ctx.empty else raw_new
else:
    raw = raw_new

raw["date"] = pd.to_datetime(raw["date"])
raw = raw.sort_values(["ticker", "date"])
for fn in REGISTRY:
    raw = fn(raw, con)

if last:
    raw = raw[raw["date"].dt.date > last]

base_cols = {"date", "ticker", "open", "high", "low", "close", "volume"}
feature_cols = [c for c in raw.columns if c not in base_cols]

existing = set(con.sql("PRAGMA table_info('daily_features')").fetchdf()["name"])
for c in feature_cols:
    if c not in existing:
        con.execute(f'ALTER TABLE daily_features ADD COLUMN "{c}" DOUBLE')

payload_cols = ["date", "ticker", "open", "high", "low", "close", "volume"] + feature_cols
con.register("feat", raw[payload_cols])
cols_csv = ", ".join([f'"{c}"' for c in payload_cols])
con.execute(
    f"""
INSERT OR REPLACE INTO daily_features ({cols_csv})
SELECT {cols_csv} FROM feat
"""
)
con.unregister("feat")
print(f"[+] daily_features updated with {len(raw)} new rows, OHLCV + {len(feature_cols)} feature cols")

if not args.skip_multi_tf:
    build_multi_tf_features(DB)
    print("[+] multi_tf_features refreshed")
