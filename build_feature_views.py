r"""
Create tradelab.weekly_features and tradelab.monthly_features
(DATA_MODEL.md D2), over Hood.weekly_bars / Hood.monthly_bars.

close_location matches trade-lab's own definition exactly
(src/trade_lab/features/daily.py: (close - low) / (high - low), clipped to
[0, 1]) -- one implementation per feature, this is SQL for display/context
only (sec0 rule 3), not a second implementation an envelope could gate on.

range_atr here is each bar's own (high - low) against its own trailing
10-period average range, not a true cross-timeframe ATR (there is no
weekly/monthly ATR defined anywhere else in this codebase to reuse) --
a self-normalizing ratio in the same spirit.

vol_ratio_10w / vol_ratio_10m use a trailing window that excludes the
current bar (10 PRECEDING AND 1 PRECEDING), matching build_events.py's
vol_event convention; sma_10w and high_52w include the current bar, the
standard moving-average/rolling-high convention.

Authorizes both as views on Hood (same pattern as tradelab.daily_market --
see trade-lab/docs/STATUS.md's authorized-view addendum), so a
tradelab-only reader can query them without any direct Hood grant.

Usage:
    python build_feature_views.py
"""

import os

from google.cloud import bigquery

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "pricesz")
HOOD_DATASET = os.getenv("BIGQUERY_DATASET", "Hood")
TRADELAB_DATASET = os.getenv("TRADELAB_DATASET", "tradelab")

WEEKLY_FEATURES_SQL = f"""
CREATE OR REPLACE VIEW `{PROJECT_ID}.{TRADELAB_DATASET}.weekly_features` AS
WITH bars AS (
  SELECT
    ticker, week, high, low, close, volume,
    AVG(volume) OVER (
      PARTITION BY ticker ORDER BY week ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS avg_vol_10w,
    AVG(high - low) OVER (
      PARTITION BY ticker ORDER BY week ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS avg_range_10w,
    AVG(close) OVER (
      PARTITION BY ticker ORDER BY week ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS sma_10w,
    MAX(high) OVER (
      PARTITION BY ticker ORDER BY week ROWS BETWEEN 51 PRECEDING AND CURRENT ROW
    ) AS high_52w
  FROM `{PROJECT_ID}.{HOOD_DATASET}.weekly_bars`
)
SELECT
  ticker,
  week,
  close,
  SAFE_DIVIDE(volume, avg_vol_10w) AS vol_ratio_10w,
  SAFE_DIVIDE(volume, avg_vol_10w) >= 2 AS vol_outlier,
  SAFE_DIVIDE(high - low, avg_range_10w) AS range_atr,
  LEAST(GREATEST(SAFE_DIVIDE(close - low, high - low), 0), 1) AS close_location,
  sma_10w,
  SAFE_DIVIDE(close - sma_10w, sma_10w) * 100 AS pct_vs_sma_10w,
  high_52w,
  SAFE_DIVIDE(close - high_52w, high_52w) * 100 AS pct_from_52w_high
FROM bars
"""

MONTHLY_FEATURES_SQL = f"""
CREATE OR REPLACE VIEW `{PROJECT_ID}.{TRADELAB_DATASET}.monthly_features` AS
WITH bars AS (
  SELECT
    ticker, month, high, low, close, volume,
    AVG(volume) OVER (
      PARTITION BY ticker ORDER BY month ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS avg_vol_10m,
    AVG(high - low) OVER (
      PARTITION BY ticker ORDER BY month ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS avg_range_10m
  FROM `{PROJECT_ID}.{HOOD_DATASET}.monthly_bars`
)
SELECT
  ticker,
  month,
  close,
  SAFE_DIVIDE(volume, avg_vol_10m) AS vol_ratio_10m,
  SAFE_DIVIDE(volume, avg_vol_10m) >= 2 AS vol_outlier,
  SAFE_DIVIDE(high - low, avg_range_10m) AS range_atr,
  LEAST(GREATEST(SAFE_DIVIDE(close - low, high - low), 0), 1) AS close_location
FROM bars
"""


def authorize_view_on_hood(client: bigquery.Client, view_table: str) -> None:
    hood_ds = client.get_dataset(f"{PROJECT_ID}.{HOOD_DATASET}")
    entries = list(hood_ds.access_entries)
    view_ref = {"projectId": PROJECT_ID, "datasetId": TRADELAB_DATASET, "tableId": view_table}
    already = any(getattr(e, "entity_type", None) == "view" and e.entity_id == view_ref for e in entries)
    if already:
        return
    entries.append(bigquery.AccessEntry(role=None, entity_type="view", entity_id=view_ref))
    hood_ds.access_entries = entries
    client.update_dataset(hood_ds, ["access_entries"])
    print(f"[+] authorized {TRADELAB_DATASET}.{view_table} as a view on {HOOD_DATASET}")


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    client.query(WEEKLY_FEATURES_SQL).result()
    print(f"[+] {TRADELAB_DATASET}.weekly_features created")
    client.query(MONTHLY_FEATURES_SQL).result()
    print(f"[+] {TRADELAB_DATASET}.monthly_features created")

    authorize_view_on_hood(client, "weekly_features")
    authorize_view_on_hood(client, "monthly_features")


if __name__ == "__main__":
    main()
