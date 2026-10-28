r"""
Create Hood.weekly_bars and Hood.monthly_bars (DATA_MODEL.md D2): OHLCV
rolled up from Daily_prices. Views, not tables (sec0 rule 1: computable
from a table, never recomputed as its own fact).

`is_complete` is true once the period has fully elapsed relative to today
-- not by day-count, so a holiday-shortened week/month (fewer than 5 / ~21
trading days) is still correctly "complete" once its calendar period has
passed, rather than permanently reading as partial.

Usage:
    python build_bars_views.py
"""

import os

from google.cloud import bigquery

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "pricesz")
DATASET = os.getenv("BIGQUERY_DATASET", "Hood")

WEEKLY_SQL = f"""
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET}.weekly_bars` AS
WITH daily AS (
  SELECT
    ticker, date, open, high, low, close, adj_close, volume,
    DATE_ADD(DATE_TRUNC(date, WEEK(MONDAY)), INTERVAL 4 DAY) AS week
  FROM `{PROJECT_ID}.{DATASET}.Daily_prices`
),
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY ticker, week ORDER BY date ASC) AS rn_asc,
    ROW_NUMBER() OVER (PARTITION BY ticker, week ORDER BY date DESC) AS rn_desc
  FROM daily
)
SELECT
  ticker,
  week,
  MAX(IF(rn_asc = 1, open, NULL)) AS open,
  MAX(high) AS high,
  MIN(low) AS low,
  MAX(IF(rn_desc = 1, close, NULL)) AS close,
  MAX(IF(rn_desc = 1, adj_close, NULL)) AS adj_close,
  SUM(volume) AS volume,
  COUNT(*) AS n_days,
  week < CURRENT_DATE() AS is_complete
FROM ranked
GROUP BY ticker, week
"""

MONTHLY_SQL = f"""
CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET}.monthly_bars` AS
WITH daily AS (
  SELECT
    ticker, date, open, high, low, close, adj_close, volume,
    LAST_DAY(date, MONTH) AS month
  FROM `{PROJECT_ID}.{DATASET}.Daily_prices`
),
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY ticker, month ORDER BY date ASC) AS rn_asc,
    ROW_NUMBER() OVER (PARTITION BY ticker, month ORDER BY date DESC) AS rn_desc
  FROM daily
)
SELECT
  ticker,
  month,
  MAX(IF(rn_asc = 1, open, NULL)) AS open,
  MAX(high) AS high,
  MIN(low) AS low,
  MAX(IF(rn_desc = 1, close, NULL)) AS close,
  MAX(IF(rn_desc = 1, adj_close, NULL)) AS adj_close,
  SUM(volume) AS volume,
  COUNT(*) AS n_days,
  month < CURRENT_DATE() AS is_complete
FROM ranked
GROUP BY ticker, month
"""


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    client.query(WEEKLY_SQL).result()
    print(f"[+] {DATASET}.weekly_bars created")
    client.query(MONTHLY_SQL).result()
    print(f"[+] {DATASET}.monthly_bars created")


if __name__ == "__main__":
    main()
