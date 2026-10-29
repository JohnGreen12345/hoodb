r"""
Rebuild universe.csv to DATA_MODEL.md's D1 shape: Russell 1000 + Jo's
watchlist + context symbols, with source and lifecycle status columns.

Russell 1000 constituents come from russell1000_sector_symbols.csv (copied
from the read-only Research archive, snapshot dated 2026-07-08 -- see
DATA_MODEL.md sec1: "current classification, not point-in-time"; the same
applies to membership here. Re-copy that file to refresh it.

Precedence when a ticker appears in more than one source: context >
watchlist > r1000 (a deliberate Jo pick on the existing watchlist always
wins over the generic index bucket; the 18 context symbols win over
everything since they're fixed by name, not membership).

Usage:
    python build_universe.py
Rewrites universe.csv in place. Run classify_universe.py next to fill
sector/industry, then rebuild_universe_table.py to push to BigQuery.
"""

import datetime as dt
import os

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
UNIVERSE_CSV = os.path.join(HERE, "universe.csv")
R1000_CSV = os.path.join(HERE, "russell1000_sector_symbols.csv")

CONTEXT_SYMBOLS = {
    "SPY", "QQQ", "IWM", "DIA", "VIX",
    "XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY",
    "SMH", "IGV",
}


def main() -> None:
    today = dt.date.today().isoformat()

    existing = pd.read_csv(UNIVERSE_CSV, keep_default_na=False)
    existing["ticker"] = existing["ticker"].astype(str).str.strip()
    if "source" not in existing:
        existing["source"] = ""
    existing.loc[existing["ticker"].isin(CONTEXT_SYMBOLS), "source"] = "context"
    existing.loc[existing["source"].astype(str).str.strip() == "", "source"] = "watchlist"
    if "inactive_reason" not in existing:
        existing["inactive_reason"] = ""

    r1000 = pd.read_csv(R1000_CSV)
    r1000["ticker"] = r1000["ticker"].astype(str).str.strip()

    existing_tickers = set(existing["ticker"])
    new_r1000 = r1000[~r1000["ticker"].isin(existing_tickers)].drop_duplicates(subset=["ticker"]).copy()
    new_rows = pd.DataFrame({
        "ticker": new_r1000["ticker"],
        "name": new_r1000["company_name"],
        "type": "stock",
        "active_flag": True,
        "source": "r1000",
        "sector": "",
        "industry": "",
        "added_at": today,
        "classified_at": "",
        "inactive_reason": "",
    })

    out = pd.concat([existing, new_rows], ignore_index=True)
    out = out[[
        "ticker", "name", "type", "source", "sector", "industry",
        "active_flag", "added_at", "classified_at", "inactive_reason",
    ]]
    out = out.sort_values("ticker").reset_index(drop=True)

    out.to_csv(UNIVERSE_CSV, index=False)

    counts = out["source"].value_counts().to_dict()
    print(f"[+] universe.csv rewritten: {len(out)} tickers total -> {counts}")
    dupes = out["ticker"].duplicated().sum()
    print(f"[i] duplicate tickers: {dupes}")


if __name__ == "__main__":
    main()
