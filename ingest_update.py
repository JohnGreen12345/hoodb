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
        help="History length to pull if Daily_prices is empty",
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
    df = client.query(query).to_dataframe()
    tickers = sorted(df["ticker"].dropna().astype(str).str.strip().unique().tolist())
    return [ticker for ticker in tickers if ticker]


def get_last_loaded_date(client: bigquery.Client, project_id: str, dataset: str, price_table: str) -> dt.date | None:
    query = f"SELECT MAX(date) AS max_date FROM `{project_id}.{dataset}.{price_table}`"
    row = client.query(query).result().to_dataframe().iloc[0]
    max_date = row["max_date"]
    if pd.isna(max_date):
        return None
    return pd.Timestamp(max_date).date()


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

    frames: list[pd.DataFrame] = []
    if isinstance(dfyf.columns, pd.MultiIndex):
        for yahoo_ticker in yahoo_tickers:
            if yahoo_ticker not in dfyf or dfyf[yahoo_ticker].empty:
                continue
            tmp = dfyf[yahoo_ticker].copy()
            tmp.columns = tmp.columns.str.lower()
            tmp = tmp.reset_index().rename(columns={"Date": "date"})
            tmp = tmp[["date", "open", "high", "low", "close", "volume"]]
            tmp["ticker"] = yahoo_to_canonical[yahoo_ticker]
            frames.append(tmp)
    else:
        if not yahoo_tickers or dfyf.empty:
            return pd.DataFrame()
        tmp = dfyf.copy()
        tmp.columns = tmp.columns.str.lower()
        tmp = tmp.reset_index().rename(columns={"Date": "date"})
        tmp = tmp[["date", "open", "high", "low", "close", "volume"]]
        tmp["ticker"] = tickers[0]
        frames.append(tmp)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"]).dt.date
    out["volume"] = out["volume"].fillna(0).astype("int64")
    out = out[["date", "ticker", "open", "high", "low", "close", "volume"]]
    out = out.drop_duplicates(subset=["date", "ticker"])
    return out


def summarize_fetch_results(requested_tickers: list[str], fetched: pd.DataFrame) -> None:
    fetched_tickers = set()
    if not fetched.empty:
        fetched_tickers = set(fetched["ticker"].astype(str).unique().tolist())
    failed_tickers = sorted(set(requested_tickers) - fetched_tickers)
    print(f"[i] Yahoo fetched {len(fetched_tickers)}/{len(requested_tickers)} tickers")
    if failed_tickers:
        preview = ", ".join(failed_tickers[:10])
        suffix = " ..." if len(failed_tickers) > 10 else ""
        print(f"[!] Yahoo returned no data for {len(failed_tickers)} tickers: {preview}{suffix}")


def filter_new_rows(df: pd.DataFrame, last_loaded_date: dt.date | None) -> pd.DataFrame:
    if df.empty or last_loaded_date is None:
        return df
    return df[df["date"] > last_loaded_date].copy()


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


def main() -> None:
    args = parse_args()
    client = make_client(args.project_id, args.credentials)

    tickers = get_active_tickers(client, args.project_id, args.dataset, args.universe_table)
    if not tickers:
        print("[!] No active tickers found in BigQuery universe")
        return

    last_loaded_date = get_last_loaded_date(client, args.project_id, args.dataset, args.price_table)
    if last_loaded_date is None:
        start_date = dt.date.today() - dt.timedelta(days=int(args.full_refresh_years * 365))
    else:
        start_date = last_loaded_date + dt.timedelta(days=1)

    end_date = dt.date.today()
    if start_date > end_date:
        print(f"[i] Daily_prices is already current through {last_loaded_date}")
        return

    print(f"[i] Fetching Yahoo prices for {len(tickers)} tickers from {start_date} to {end_date}")
    fetched = fetch_yahoo_prices(tickers, start_date, end_date)
    summarize_fetch_results(tickers, fetched)
    new_rows = filter_new_rows(fetched, last_loaded_date)

    if new_rows.empty:
        print(f"[i] No new rows returned for {start_date} to {end_date}")
        return

    upload_prices(client, new_rows, args.project_id, args.dataset, args.price_table)


if __name__ == "__main__":
    main()
