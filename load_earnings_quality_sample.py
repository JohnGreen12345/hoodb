"""Load the bounded F16--F18 earnings-quality sample from release reconciliations.

This deliberately covers only NUE, INGM, XMTR, HNGE and HTFL.  It does not
infer an adjusted EPS: a null value means the latest release did not publish a
comparable EPS reconciliation.  Cash/profit is calculated from the EODHD cash
flow statement and labelled separately from the issuer release facts.
"""

from __future__ import annotations

import datetime as dt
import json
import os
from decimal import Decimal
from typing import Any

from google.cloud import bigquery

from eodhd_client import EODHDClient


PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "pricesz")
DATASET = os.getenv("BIGQUERY_DATASET", "Hood")
TABLE = f"{PROJECT_ID}.{DATASET}.earnings_quality_quarterly"
SOURCE = "issuer earnings release / 8-K reconciliation; EODHD cash flow"

# Values are transcribed only from the cited issuer release / 8-K exhibit.
# Currency is USD millions.  A null adjusted EPS is intentional, not a zero.
RELEASES: tuple[dict[str, Any], ...] = (
    {"ticker": "NUE", "period_end": dt.date(2026, 10, 3), "report_date": dt.date(2026, 10, 26),
     "sales": 11249.0, "gaap_eps": 5.80, "adjusted_eps": None,
     "gaap_net_income": 1320.0, "adjusted_net_income": None, "adjustments": [],
     "note": "Q3 release did not publish adjusted net income/EPS; do not infer one from EBITDA or free cash flow.",
     "source_url": "https://investors.nucor.com/news/news-details/2026/Nucor-Reports-Results-for-the-Third-Quarter-of-2026/"},
    {"ticker": "INGM", "period_end": dt.date(2026, 9, 26), "report_date": dt.date(2026, 10, 29),
     "sales": 15220.461, "gaap_eps": 0.57, "adjusted_eps": 0.91,
     "gaap_net_income": 131.080, "adjusted_net_income": 210.591,
     "adjustments": ["amortization of intangibles", "restructuring costs", "net foreign-currency exchange loss", "integration, transition and operational-improvement costs", "cash-based compensation", "stock-based compensation", "other items", "tax impact of pre-tax adjustments", "other miscellaneous tax adjustments"],
     "note": "Issuer calls adjusted EPS non-GAAP diluted EPS.",
     "source_url": "https://ir.ingrammicro.com/press-releases/detail/977/ingram-micro-reports-record-q3-2026-results-with-solid-double-digit-growth-in-net-sales-and-net-income-and-strong-cash-flow-generation-raised-full-year-guidance-from-september-capital-markets-day"},
    {"ticker": "XMTR", "period_end": dt.date(2026, 6, 30), "report_date": dt.date(2026, 8, 4),
     "sales": 229.2, "gaap_eps": -0.10, "adjusted_eps": 0.16,
     "gaap_net_income": -5.314, "adjusted_net_income": 9.913,
     "adjustments": ["stock-based compensation", "payroll tax related to stock-based compensation", "amortization of deferred costs on convertible notes", "charitable contribution of common stock", "amortization of acquired intangible assets and patents", "dilutive share effect"],
     "note": "Issuer calls adjusted EPS non-GAAP diluted EPS; Q1 2026 changed its non-GAAP definition to exclude depreciation.",
     "source_url": "https://xometry.gcs-web.com/news-releases/news-release-details/xometry-reports-record-second-quarter-2026-results"},
    {"ticker": "HNGE", "period_end": dt.date(2026, 6, 30), "report_date": dt.date(2026, 8, 4),
     "sales": 212.817, "gaap_eps": 0.52, "adjusted_eps": 0.59,
     "gaap_net_income": 43.133, "adjusted_net_income": 49.147,
     "adjustments": ["stock-based compensation", "employer payroll tax related to stock-based compensation", "amortization of intangible assets", "acquisition-related expenses", "income-tax effect", "other dilutive items"],
     "note": "Issuer calls adjusted EPS non-GAAP diluted net income per share.",
     "source_url": "https://ir.hingehealth.com/news/news-details/2026/Hinge-Health-reports-record-second-quarter-2026-financial-results-signs-definitive-agreement-to-acquire-Cylinder-Health/default.aspx"},
    {"ticker": "HTFL", "period_end": dt.date(2026, 6, 30), "report_date": dt.date(2026, 8, 13),
     "sales": 64.082, "gaap_eps": -0.18, "adjusted_eps": -0.07,
     "gaap_net_income": -15.743, "adjusted_net_income": -5.767,
     "adjustments": ["stock-based compensation", "certain litigation expenses"],
     "note": "Issuer calls adjusted EPS non-GAAP net loss per share.",
     "source_url": "https://ir.heartflow.com/news-releases/news-release-details/heartflow-reports-second-quarter-2026-financial-results-and"},
)


def as_date(value: str | None) -> dt.date | None:
    return dt.date.fromisoformat(value[:10]) if value else None


def as_float(value: Any) -> float | None:
    return None if value in (None, "") else float(Decimal(str(value)))


def cash_flow_quarters(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the latest quarterly GAAP NI/operating-cash-flow observations."""
    quarterly = payload.get("Financials", {}).get("Cash_Flow", {}).get("quarterly", {})
    records = quarterly.values() if isinstance(quarterly, dict) else quarterly
    rows = []
    for record in records:
        period_end = as_date(record.get("date"))
        if period_end is None:
            continue
        rows.append({"period_end": period_end, "net_income": as_float(record.get("netIncome")),
                     "operating_cash_flow": as_float(record.get("totalCashFromOperatingActivities"))})
    return sorted(rows, key=lambda row: row["period_end"], reverse=True)


def quality_row(release: dict[str, Any], cash_flow: list[dict[str, Any]]) -> dict[str, Any]:
    usable = [row for row in cash_flow if row["net_income"] is not None and row["operating_cash_flow"] is not None][:4]
    cash_ttm = net_income_ttm = ratio = None
    if len(usable) == 4:
        cash_ttm = sum(row["operating_cash_flow"] for row in usable)
        net_income_ttm = sum(row["net_income"] for row in usable)
        ratio = round(cash_ttm / net_income_ttm, 6) if net_income_ttm else None
    adjusted_eps = release["adjusted_eps"]
    adjusted_income = release["adjusted_net_income"]
    adjustment_amount = None if adjusted_income is None else adjusted_income - release["gaap_net_income"]
    return {
        "ticker": release["ticker"], "period_end": release["period_end"], "report_date": release["report_date"],
        "sales": release["sales"], "gaap_eps": release["gaap_eps"], "adjusted_eps": adjusted_eps,
        "eps_gap": None if adjusted_eps is None else adjusted_eps - release["gaap_eps"],
        "gaap_net_income": release["gaap_net_income"], "adjusted_net_income": adjusted_income,
        "adjustment_amount": adjustment_amount,
        "adjustment_pct_revenue": None if adjustment_amount is None or not release["sales"] else round(100 * adjustment_amount / release["sales"], 6),
        "adjustments": release["adjustments"], "operating_cash_flow_ttm": cash_ttm,
        "net_income_ttm": net_income_ttm, "cash_to_profit_ttm": ratio,
        "cash_flow_quarters": len(usable), "note": release.get("note"), "source_url": release["source_url"], "source": SOURCE,
    }


def ensure_table(client: bigquery.Client) -> None:
    schema = [
        bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"), bigquery.SchemaField("period_end", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("report_date", "DATE", mode="REQUIRED"), bigquery.SchemaField("sales", "FLOAT"),
        bigquery.SchemaField("gaap_eps", "FLOAT"), bigquery.SchemaField("adjusted_eps", "FLOAT"), bigquery.SchemaField("eps_gap", "FLOAT"),
        bigquery.SchemaField("gaap_net_income", "FLOAT"), bigquery.SchemaField("adjusted_net_income", "FLOAT"),
        bigquery.SchemaField("adjustment_amount", "FLOAT"), bigquery.SchemaField("adjustment_pct_revenue", "FLOAT"),
        bigquery.SchemaField("adjustments", "STRING", mode="REPEATED"), bigquery.SchemaField("operating_cash_flow_ttm", "FLOAT"),
        bigquery.SchemaField("net_income_ttm", "FLOAT"), bigquery.SchemaField("cash_to_profit_ttm", "FLOAT"),
        bigquery.SchemaField("cash_flow_quarters", "INTEGER", mode="REQUIRED"), bigquery.SchemaField("note", "STRING"),
        bigquery.SchemaField("source_url", "STRING", mode="REQUIRED"), bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("retrieved_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    client.create_table(bigquery.Table(TABLE, schema=schema), exists_ok=True)


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    api = EODHDClient()
    rows = []
    for release in RELEASES:
        rows.append(quality_row(release, cash_flow_quarters(api.get_fundamentals(release["ticker"]))))
    now = dt.datetime.now(dt.timezone.utc)
    for row in rows:
        row["retrieved_at"] = now.isoformat()
        for field in ("period_end", "report_date"):
            row[field] = row[field].isoformat()
    ensure_table(client)
    tickers = [row["ticker"] for row in rows]
    config = bigquery.QueryJobConfig(query_parameters=[bigquery.ArrayQueryParameter("tickers", "STRING", tickers)])
    client.query(f"DELETE FROM `{TABLE}` WHERE ticker IN UNNEST(@tickers)", job_config=config).result()
    client.load_table_from_json(rows, TABLE, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")).result()
    print(f"Loaded {len(rows)} issuer-reconciled earnings-quality sample rows into {TABLE}.")


if __name__ == "__main__":
    main()
