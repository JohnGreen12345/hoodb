"""Focused regression test for the NYSE-session ingest guard."""

import datetime as dt
import unittest

import pandas as pd

from ingest_update import filter_to_nyse_sessions


class NyseCalendarGuardTests(unittest.TestCase):
    def test_drops_memorial_day_and_weekend_but_keeps_session(self) -> None:
        prices = pd.DataFrame(
            {
                "date": [dt.date(2026, 5, 22), dt.date(2026, 5, 25), dt.date(2026, 5, 30)],
                "ticker": ["NUE", "NUE", "NUE"],
            }
        )

        kept = filter_to_nyse_sessions(prices)

        self.assertEqual(kept["date"].tolist(), [dt.date(2026, 5, 22)])


if __name__ == "__main__":
    unittest.main()
