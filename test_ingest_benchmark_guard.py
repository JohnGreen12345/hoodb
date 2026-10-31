"""Regression tests for latest-session benchmark completeness."""

import datetime as dt

import pandas as pd
import pytest

from ingest_update import assert_latest_session_benchmarks


def _bars(tickers: list[str], day: dt.date) -> pd.DataFrame:
    return pd.DataFrame({"ticker": tickers, "date": [day] * len(tickers)})


def test_raises_when_spy_is_missing_from_a_newer_universe_session():
    day = dt.date(2026, 10, 30)
    fetched = _bars(["AAPL", "VIX"], day)
    coverage = {"SPY": (dt.date(2026, 10, 29), 750), "VIX": (dt.date(2026, 10, 29), 750)}

    with pytest.raises(RuntimeError, match="SPY"):
        assert_latest_session_benchmarks(fetched, ["AAPL", "SPY", "VIX"], coverage)


def test_accepts_a_benchmark_already_stored_for_the_latest_session():
    day = dt.date(2026, 10, 30)
    fetched = _bars(["AAPL"], day)
    coverage = {"SPY": (day, 750), "VIX": (day, 750)}

    assert_latest_session_benchmarks(fetched, ["AAPL", "SPY", "VIX"], coverage)
