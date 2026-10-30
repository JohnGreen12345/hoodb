import datetime as dt

from load_eodhd_m2_sample import calendar_rows, fundamentals_rows


def test_sample_transforms_keep_provider_eps_and_null_missing_sales_fields():
    fundamentals = fundamentals_rows("NUE", {
        "Financials": {"Income_Statement": {"quarterly": {
            "2026-09-30": {"date": "2026-09-30", "filing_date": "2026-10-26", "totalRevenue": "11249000000.00"}
        }}},
        "Earnings": {"History": {"2026-09-30": {"epsActual": 5.8}}},
    })
    calendar = calendar_rows("INGM", {"earnings": [{
        "report_date": "2026-10-29", "before_after_market": "AfterMarket", "estimate": 0.78, "actual": 0.72
    }]})

    assert fundamentals == [{"ticker": "NUE", "period_end": dt.date(2026, 9, 30), "filed_date": dt.date(2026, 10, 26), "sales": 11_249_000_000.0, "eps": 5.8, "source": "EODHD v1.1/fundamentals (provider-reported EPS)"}]
    assert calendar[0]["timing"] == "AMC"
    assert calendar[0]["eps_act"] == 0.72
    assert calendar[0]["sales_est"] is calendar[0]["sales_act"] is None
