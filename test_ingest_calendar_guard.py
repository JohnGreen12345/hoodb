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

    def test_blocks_every_full_nyse_holiday_in_2026(self) -> None:
        holidays = [
            dt.date(2026, 1, 1), dt.date(2026, 1, 19), dt.date(2026, 2, 16),
            dt.date(2026, 4, 3), dt.date(2026, 5, 25), dt.date(2026, 6, 19),
            dt.date(2026, 7, 3), dt.date(2026, 9, 7), dt.date(2026, 11, 26),
            dt.date(2026, 12, 25),
        ]
        prices = pd.DataFrame({"date": holidays, "ticker": ["NUE"] * len(holidays)})

        kept = filter_to_nyse_sessions(prices)

        self.assertTrue(kept.empty)


if __name__ == "__main__":
    unittest.main()
