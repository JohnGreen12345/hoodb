r"""
Build Hood.events (DATA_MODEL.md D2): v1 is `vol_event` only -- a
ticker/date where volume >= 2x its own trailing 50-day average (the 50
days strictly before that date; never includes the day itself, same
convention as every other trailing-average measure in this pipeline).

A table, not a view (DATA_MODEL.md sec0 rule 2's exception): detecting an
event is "computed nightly" and meant to accumulate event TYPES that
aren't recomputable from price data alone later (earnings, split), so it
is modeled as a captured fact from the start rather than a view that would
need restructuring once those land.

Rewritten in full each run (CREATE OR REPLACE) -- computed directly in
BigQuery SQL rather than pulled into pandas, since the window function does
the same thing a Python pass would without round-tripping ~1M rows.

Usage:
    python build_events.py
"""

import os

from google.cloud import bigquery

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "pricesz")
DATASET = os.getenv("BIGQUERY_DATASET", "Hood")

SQL = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.events` AS
WITH vol_avg AS (
  SELECT
    ticker, date, volume,
    AVG(volume) OVER (
      PARTITION BY ticker ORDER BY date
      ROWS BETWEEN 50 PRECEDING AND 1 PRECEDING
    ) AS avg_vol_50d
  FROM `{PROJECT_ID}.{DATASET}.Daily_prices`
)
SELECT
  ticker,
  date,
  'vol_event' AS type,
  TO_JSON_STRING(STRUCT(
    volume,
    avg_vol_50d,
    SAFE_DIVIDE(volume, avg_vol_50d) AS vol_ratio
  )) AS details,
  CURRENT_TIMESTAMP() AS detected_at
FROM vol_avg
WHERE avg_vol_50d IS NOT NULL AND volume >= 2 * avg_vol_50d
"""


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    client.query(SQL).result()
    n = list(client.query(f"SELECT COUNT(*) AS n FROM `{PROJECT_ID}.{DATASET}.events`").result())[0]["n"]
    print(f"[+] {DATASET}.events rebuilt: {n} vol_event rows")


if __name__ == "__main__":
    main()
