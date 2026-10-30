"""Regression tests for safe, date-scoped price repair validation."""

import datetime as dt
import unittest

import pandas as pd

from repair_prices import validate_replacement


class RepairValidationTests(unittest.TestCase):
    def test_accepts_complete_valid_requested_bars(self) -> None:
        rows = pd.DataFrame({
            "date": [dt.date(2026, 10, 28), dt.date(2026, 10, 29)],
            "ticker": ["NUE", "NUE"],
            "open": [270.0, 273.0], "high": [272.37, 275.89],
            "low": [263.44, 271.30], "close": [271.79, 275.29],
        })
        validate_replacement(rows, "NUE", set(rows["date"]))

    def test_rejects_impossible_ohlc(self) -> None:
        rows = pd.DataFrame({
            "date": [dt.date(2026, 10, 28)], "ticker": ["NUE"],
            "open": [270.0], "high": [269.9], "low": [263.44], "close": [266.7],
        })
        with self.assertRaisesRegex(ValueError, "invalid OHLC"):
            validate_replacement(rows, "NUE", {dt.date(2026, 10, 28)})


if __name__ == "__main__":
    unittest.main()
