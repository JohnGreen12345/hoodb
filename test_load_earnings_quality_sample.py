import datetime as dt

from load_earnings_quality_sample import quality_row


def test_quality_row_keeps_reconciliation_and_cash_profit_on_separate_bases():
    release = {
        "ticker": "TEST",
        "period_end": dt.date(2026, 9, 30),
        "report_date": dt.date(2026, 10, 29),
        "sales": 100.0,
        "gaap_eps": 0.50,
        "adjusted_eps": 0.75,
        "gaap_net_income": 50.0,
        "adjusted_net_income": 75.0,
        "adjustments": ["stock-based compensation", "restructuring"],
        "source_url": "https://example.test/release",
    }
    cash_flow = [
        {"period_end": dt.date(2026, 9, 30), "net_income": 50.0, "operating_cash_flow": 60.0},
        {"period_end": dt.date(2026, 6, 30), "net_income": 40.0, "operating_cash_flow": 30.0},
        {"period_end": dt.date(2026, 3, 31), "net_income": 30.0, "operating_cash_flow": 45.0},
        {"period_end": dt.date(2025, 12, 31), "net_income": 20.0, "operating_cash_flow": 25.0},
    ]

    row = quality_row(release, cash_flow)

    assert row["eps_gap"] == 0.25
    assert row["adjustment_amount"] == 25.0
    assert row["adjustment_pct_revenue"] == 25.0
    assert row["adjustments"] == ["stock-based compensation", "restructuring"]
    assert row["operating_cash_flow_ttm"] == 160.0
    assert row["net_income_ttm"] == 140.0
    assert row["cash_to_profit_ttm"] == round(160 / 140, 6)


def test_quality_row_leaves_unreported_adjusted_eps_null():
    release = {
        "ticker": "TEST",
        "period_end": dt.date(2026, 9, 30),
        "report_date": dt.date(2026, 10, 29),
        "sales": 100.0,
        "gaap_eps": 0.50,
        "adjusted_eps": None,
        "gaap_net_income": 50.0,
        "adjusted_net_income": None,
        "adjustments": [],
        "source_url": "https://example.test/release",
    }

    row = quality_row(release, [])

    assert row["eps_gap"] is None
    assert row["adjustment_pct_revenue"] is None
    assert row["cash_to_profit_ttm"] is None
