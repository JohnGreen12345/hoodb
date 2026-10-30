"""Create authorized, read-only Hood source views for tradelab.

The views are intentionally daily-grain only. Weekly/monthly bars are built
from v_daily_prices in application code, not duplicated here.
"""

import os

from google.cloud import bigquery

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "pricesz")
HOOD_DATASET = os.getenv("BIGQUERY_DATASET", "Hood")
TRADELAB_DATASET = os.getenv("TRADELAB_DATASET", "tradelab")
SOURCE_VIEWS = {
    "v_daily_prices": "Daily_prices",
    "v_universe": "universe",
    "v_fundamentals_quarterly": "fundamentals_quarterly",
    "v_earnings_calendar": "earnings_calendar",
}


def authorize_view_on_hood(client: bigquery.Client, view_name: str) -> bool:
    """Authorize one tradelab view to read the private Hood dataset."""
    hood_dataset = client.get_dataset(f"{PROJECT_ID}.{HOOD_DATASET}")
    entries = list(hood_dataset.access_entries)
    view_ref = {"projectId": PROJECT_ID, "datasetId": TRADELAB_DATASET, "tableId": view_name}
    if any(entry.entity_type == "view" and entry.entity_id == view_ref for entry in entries):
        return False
    entries.append(bigquery.AccessEntry(role=None, entity_type="view", entity_id=view_ref))
    hood_dataset.access_entries = entries
    client.update_dataset(hood_dataset, ["access_entries"])
    return True


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    for view_name, source_table in SOURCE_VIEWS.items():
        sql = (
            f"CREATE OR REPLACE VIEW `{PROJECT_ID}.{TRADELAB_DATASET}.{view_name}` AS "
            f"SELECT * FROM `{PROJECT_ID}.{HOOD_DATASET}.{source_table}`"
        )
        client.query(sql).result()
        changed = authorize_view_on_hood(client, view_name)
        print(f"{TRADELAB_DATASET}.{view_name}: created; authorization {'added' if changed else 'already present'}")


if __name__ == "__main__":
    main()
