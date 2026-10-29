"""
Yahoo -> BigQuery daily price updater.

Flow:
1. Read active tickers from BigQuery `universe`
2. Read latest loaded date from BigQuery `Daily_prices`
3. Fetch missing daily bars from Yahoo
4. Append new rows into BigQuery `Daily_prices`

This script is intentionally separate from the DuckDB pipeline.
"""

import argparse
import datetime as dt
import os

import pandas as pd
import yfinance as yf
from google.cloud import bigquery


DEFAULT_PROJECT_ID = "pricesz"
DEFAULT_DATASET = "Hood"
DEFAULT_UNIVERSE_TABLE = "universe"
DEFAULT_PRICE_TABLE = "Daily_prices"
YAHOO_SYMBOL_MAP = {
    "VIX": "^VIX",
}
# PUBLISH_SPEC.md sec4a point 2: "fewer than ~260 rows" is the per-ticker
# backfill trigger -- 260 trading days is one year, enough for a 200-day
# moving average plus margin.
MIN_HISTORY_ROWS = 260


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update BigQuery Daily_prices from Yahoo Finance")
    parser.add_argument("--project-id", default=os.getenv("GOOGLE_CLOUD_PROJECT", DEFAULT_PROJECT_ID))
    parser.add_argument("--dataset", default=os.getenv("BIGQUERY_DATASET", DEFAULT_DATASET))
    parser.add_argument("--universe-table", default=os.getenv("BIGQUERY_UNIVERSE_TABLE", DEFAULT_UNIVERSE_TABLE))
    parser.add_argument("--price-table", default=os.getenv("BIGQUERY_PRICE_TABLE", DEFAULT_PRICE_TABLE))
    parser.add_argument("--credentials", default=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    parser.add_argument(
        "--full-refresh-years",
        type=float,
        default=1.0,
        help="History length to pull if Daily_prices is empty, or for every ticker in --mode full_refresh",
    )
    parser.add_argument(
        "--mode",
        choices=["incremental", "full_refresh"],
        default="incremental",
        help="incremental (default, the nightly job): fetch only what's missing per ticker. "
        "full_refresh (the weekly job): re-fetch and replace every active ticker's full history -- "
        "corrects the discontinuity a split creates in an append-only incremental pipeline (a split "
        "after a ticker's rows are already stored never gets retroactively applied to them otherwise).",
    )
    return parser.parse_args()


def make_client(project_id: str, credentials_path: str | None) -> bigquery.Client:
    if credentials_path:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    return bigquery.Client(project=project_id)


def get_active_tickers(client: bigquery.Client, project_id: str, dataset: str, universe_table: str) -> list[str]:
    query = f"""
        SELECT ticker
        FROM `{project_id}.{dataset}.{universe_table}`
        WHERE COALESCE(active_flag, TRUE) = TRUE
          AND COALESCE(type, 'stock') IN ('stock', 'etf', 'benchmark')
          AND ticker IS NOT NULL
    """
    # create_bqstorage_client=False: these result sets are tiny (one row per
    # ticker), and the BigQuery Storage API read path needs an extra IAM
    # role (bigquery.readSessionUser) the ingest service account doesn't
    # have -- not worth granting just to skip the plain REST download here.
    df = client.query(query).to_dataframe(create_bqstorage_client=False)
    tickers = sorted(df["ticker"].dropna().astype(str).str.strip().unique().tolist())
    return [ticker for ticker in tickers if ticker]


def get_per_ticker_coverage(
    client: bigquery.Client, project_id: str, dataset: str, price_table: str
) -> dict[str, tuple[dt.date | None, int]]:
    """{ticker: (last_date, row_count)} -- the per-ticker backfill fix
    (PUBLISH_SPEC.md sec4a point 2) needs each ticker's own history length,
    not one global last-loaded-date for the whole table."""
    query = f"""
        SELECT ticker, MAX(date) AS last_date, COUNT(*) AS n
        FROM `{project_id}.{dataset}.{price_table}`
        GROUP BY ticker
    """
    df = client.query(query).result().to_dataframe(create_bqstorage_client=False)
    coverage: dict[str, tuple[dt.date | None, int]] = {}
    for _, row in df.iterrows():
        last_date = pd.Timestamp(row["last_date"]).date() if pd.notna(row["last_date"]) else None
        coverage[str(row["ticker"])] = (last_date, int(row["n"]))
    return coverage


def plan_fetch_start_dates(
    tickers: list[str],
    coverage: dict[str, tuple[dt.date | None, int]],
    today: dt.date,
    full_refresh_years: float,
    min_history_rows: int = MIN_HISTORY_ROWS,
) -> dict[str, dt.date]:
    """Per-ticker Yahoo fetch start date. A ticker with no rows yet, or
    fewer than min_history_rows, gets a full backfill from scratch instead
    of "yesterday's one bar" -- the bug PUBLISH_SPEC.md sec4a point 2
    describes: a newly added ticker's 200-day average stayed empty for
    ~10 months under the old "last global date + 1" rule."""
    full_backfill_start = today - dt.timedelta(days=int(full_refresh_years * 365))
    plan: dict[str, dt.date] = {}
    for ticker in tickers:
        last_date, row_count = coverage.get(ticker, (None, 0))
        if last_date is None or row_count < min_history_rows:
            plan[ticker] = full_backfill_start
        else:
            plan[ticker] = last_date + dt.timedelta(days=1)
    return plan


def to_yahoo_symbol(ticker: str) -> str:
    if ticker in YAHOO_SYMBOL_MAP:
        return YAHOO_SYMBOL_MAP[ticker]
    return ticker.replace(".", "-")


def fetch_yahoo_prices(tickers: list[str], start: dt.date, end: dt.date) -> pd.DataFrame:
    yahoo_tickers = [to_yahoo_symbol(ticker) for ticker in tickers]
    yahoo_to_canonical = {to_yahoo_symbol(ticker): ticker for ticker in tickers}
    dfyf = yf.download(
        yahoo_tickers,
        start=start,
        end=end + dt.timedelta(days=1),
        group_by="ticker",
        auto_adjust=False,
        progress=False,
    )

    price_cols = ["date", "open", "high", "low", "close", "adj_close", "volume"]
    out_cols = ["date", "ticker", "open", "high", "low", "close", "adj_close", "volume"]

    frames: list[pd.DataFrame] = []
    if isinstance(dfyf.columns, pd.MultiIndex):
        for yahoo_ticker in yahoo_tickers:
            if yahoo_ticker not in dfyf or dfyf[yahoo_ticker].empty:
                continue
            tmp = dfyf[yahoo_ticker].copy()
            tmp.columns = tmp.columns.str.lower().str.replace(" ", "_")
            tmp = tmp.reset_index().rename(columns={"Date": "date"})
            tmp = tmp[price_cols]
            tmp["ticker"] = yahoo_to_canonical[yahoo_ticker]
            frames.append(tmp)
    else:
        if not yahoo_tickers or dfyf.empty:
            return pd.DataFrame()
        tmp = dfyf.copy()
        tmp.columns = tmp.columns.str.lower().str.replace(" ", "_")
        tmp = tmp.reset_index().rename(columns={"Date": "date"})
        tmp = tmp[price_cols]
        tmp["ticker"] = tickers[0]
        frames.append(tmp)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"]).dt.date
    out = out[(out["date"] >= start) & (out["date"] <= end)].copy()
    if out.empty:
        return pd.DataFrame(columns=out_cols)
    # Multi-symbol yfinance responses share one date index. When a symbol has
    # no bar for a date that another symbol does, yfinance leaves an all-NaN
    # alignment row. These are not zero-volume market bars and must not reach
    # BigQuery as synthetic history.
    required_values = ["open", "high", "low", "close", "adj_close", "volume"]
    incomplete_count = int(out[required_values].isna().any(axis=1).sum())
    if incomplete_count:
        print(f"[i] Dropped {incomplete_count:,} incomplete Yahoo alignment row(s)")
    out = out.dropna(subset=required_values).copy()
    if out.empty:
        return pd.DataFrame(columns=out_cols)
    out["volume"] = out["volume"].astype("int64")
    out = out[out_cols]
    out = out.drop_duplicates(subset=["date", "ticker"])
    return out


def summarize_fetch_results(requested_tickers: list[str], fetched: pd.DataFrame, start: dt.date, end: dt.date) -> None:
    fetched_tickers = set()
    if not fetched.empty:
        fetched_tickers = set(fetched["ticker"].astype(str).unique().tolist())
    missing_tickers = sorted(set(requested_tickers) - fetched_tickers)
    print(f"[i] Yahoo returned rows for {len(fetched_tickers)}/{len(requested_tickers)} tickers in {start} to {end}")
    if missing_tickers:
        preview = ", ".join(missing_tickers[:10])
        suffix = " ..." if len(missing_tickers) > 10 else ""
        print(f"[!] Yahoo returned no rows in that range for {len(missing_tickers)} tickers: {preview}{suffix}")


def filter_new_rows_per_ticker(df: pd.DataFrame, last_dates: dict[str, dt.date | None]) -> pd.DataFrame:
    """Per-ticker version of filter_new_rows: a backfilling ticker's fetch
    starts well before some other ticker's last-loaded date (they're fetched
    together in one Yahoo call, grouped by shared start date), so each row
    must be checked against its OWN ticker's last-loaded date, not one
    shared cutoff, or an already-loaded row would be re-inserted as a
    duplicate."""
    if df.empty:
        return df
    last_date_per_row = df["ticker"].map(last_dates)
    keep = last_date_per_row.isna() | (df["date"] > last_date_per_row)
    return df[keep].copy()


def upload_prices(
    client: bigquery.Client,
    df: pd.DataFrame,
    project_id: str,
    dataset: str,
    price_table: str,
) -> None:
    if df.empty:
        print("[i] No new Yahoo rows to upload")
        return

    target = f"{project_id}.{dataset}.{price_table}"
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    job = client.load_table_from_dataframe(df.where(pd.notnull(df), None), target, job_config=job_config)
    job.result()
    print(f"[+] Uploaded {len(df)} rows to {target}")


def full_refresh(
    client: bigquery.Client,
    tickers: list[str],
    project_id: str,
    dataset: str,
    price_table: str,
    years: float,
) -> None:
    """The weekly job (PUBLISH_SPEC.md sec4a point 3's residual gap): the
    nightly incremental job only ever appends "new" rows, so it never
    retroactively corrects a ticker's already-stored rows once a split
    happens -- the stored `close`/`adj_close` would start a discontinuity
    exactly where PUBLISH_SPEC.md describes, just deferred rather than
    fixed. Re-fetching every active ticker's full history in one shot
    (same as the original backfill) and replacing its stored rows outright
    is self-consistent regardless of any split that happened meanwhile,
    because it's one fetch at one point in time -- same reasoning as why
    the initial backfill had no jump.
    """
    end_date = dt.date.today()
    start_date = end_date - dt.timedelta(days=int(years * 365))
    print(f"[i] Full refresh: fetching {len(tickers)} ticker(s) from {start_date} to {end_date}")
    fetched = fetch_yahoo_prices(tickers, start_date, end_date)
    summarize_fetch_results(tickers, fetched, start_date, end_date)

    if fetched.empty:
        print("[!] Full refresh got no data from Yahoo -- not touching Daily_prices")
        return

    refreshed_tickers = sorted(fetched["ticker"].astype(str).unique().tolist())
    table_ref = f"{project_id}.{dataset}.{price_table}"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("tickers", "STRING", refreshed_tickers)]
    )
    client.query(
        f"DELETE FROM `{table_ref}` WHERE ticker IN UNNEST(@tickers)", job_config=job_config
    ).result()
    print(f"[i] Cleared existing rows for {len(refreshed_tickers)} ticker(s) before reloading")

    upload_prices(client, fetched, project_id, dataset, price_table)


def main() -> None:
    args = parse_args()
    client = make_client(args.project_id, args.credentials)

    tickers = get_active_tickers(client, args.project_id, args.dataset, args.universe_table)
    if not tickers:
        print("[!] No active tickers found in BigQuery universe")
        return

    if args.mode == "full_refresh":
        full_refresh(client, tickers, args.project_id, args.dataset, args.price_table, args.full_refresh_years)
        return

    end_date = dt.date.today()
    coverage = get_per_ticker_coverage(client, args.project_id, args.dataset, args.price_table)
    fetch_starts = plan_fetch_start_dates(tickers, coverage, end_date, args.full_refresh_years)
    last_dates = {ticker: coverage.get(ticker, (None, 0))[0] for ticker in tickers}

    backfilling = sorted(t for t, s in fetch_starts.items() if coverage.get(t, (None, 0))[1] < MIN_HISTORY_ROWS)
    if backfilling:
        preview = ", ".join(backfilling[:10])
        suffix = " ..." if len(backfilling) > 10 else ""
        print(f"[i] {len(backfilling)} ticker(s) under {MIN_HISTORY_ROWS} rows, backfilling from scratch: {preview}{suffix}")

    # Group tickers by identical fetch start date so each distinct window is
    # only one Yahoo call, not one per ticker.
    groups: dict[dt.date, list[str]] = {}
    for ticker, start in fetch_starts.items():
        if start <= end_date:
            groups.setdefault(start, []).append(ticker)

    if not groups:
        print(f"[i] Daily_prices is already current through {end_date}")
        return

    fetched_frames = []
    for start, group_tickers in sorted(groups.items()):
        print(f"[i] Fetching Yahoo prices for {len(group_tickers)} ticker(s) from {start} to {end_date}")
        fetched = fetch_yahoo_prices(group_tickers, start, end_date)
        summarize_fetch_results(group_tickers, fetched, start, end_date)
        fetched_frames.append(fetched)

    all_fetched = pd.concat(fetched_frames, ignore_index=True) if fetched_frames else pd.DataFrame()
    new_rows = filter_new_rows_per_ticker(all_fetched, last_dates)

    if new_rows.empty:
        print(f"[i] No new rows returned through {end_date}")
        return

    upload_prices(client, new_rows, args.project_id, args.dataset, args.price_table)


if __name__ == "__main__":
    main()
