# STATUS — hoodb

## D1 — universe rebuild (done)

- `universe.csv`: 164 -> 1,116 tickers. 952 Russell 1000 (from the archived
  `russell1000_sector_symbols.csv` snapshot, 2026-07-08) + 146 watchlist + 18
  context (SPY/QQQ/sector ETFs/VIX), tagged via a new `source` column.
  Precedence on overlap: context > watchlist > r1000.
- Classification: 1,051/1,061 stock rows have sector+industry from Yahoo
  (99.1%, vs. the >=95% bar). 10 unresolved are genuine 404s/delisted
  (MASI, AL, CWEN-A, NSA, ...).
- Pushed to `pricesz.Hood.universe` (`WRITE_TRUNCATE`, extended schema:
  ticker, name, type, source, sector, industry, active_flag, added_at,
  classified_at).
- 3-year backfill: 952 under-covered tickers backfilled 2023-10-29 ->
  2026-10-28, 715,904 rows uploaded to `pricesz.Hood.Daily_prices`.
- Verified: 0/1,116 tickers under 260 rows.

## D2 — events + bars/features (done)

- `Hood.events` (vol_event: volume >= 2x trailing 50-day avg volume):
  rebuilt against the full backfilled history, 63,482 rows.
- `Hood.weekly_bars` / `Hood.monthly_bars`: views over `Daily_prices`,
  first/last trading day per period, `is_complete` flag.
- `tradelab.weekly_features` / `tradelab.monthly_features`: views over the
  bars views (`close_location`, `range_atr`), authorized on `Hood`'s
  dataset ACL so `tradelab-reader` can query them.
- `build_events.py` wired into both `nightly_ingest.yml` and
  `weekly_full_refresh.yml`, so events stay current automatically.
- Spot-checked AAPL, AMD, NVDA: each has a real `vol_event` day and a real
  weekly vol-outlier; AAPL's 2026-06-26 outlier week hand-verified against
  raw `Daily_prices`.

Commits: `527b698` (D1), `f4e9b59` (D2).

## Pre-D1 cleanup (already satisfied, no action needed)

- Orphaned `bq-ingestion` service-account keys: only the one backing the
  `GCP_SA_KEY` GitHub secret remains.
- Sunday full-refresh job (`weekly_full_refresh.yml`, cron `0 12 * * 0`):
  confirmed present, re-fetches full history so splits self-correct.
- Read-only BigQuery account (`tradelab-reader`, PUBLISH_SPEC U4): confirmed
  present and scoped to `tradelab`.

## Next

D3/D4 (vectorized `daily_features`, `group_strength`, breadth views,
`v_watchlist_tonight`, `v_brief_inputs`) not started — on hold until the
strategy agent commits its trade-lab work.

## Independent verification (separate session, same day)

Jo asked a second session to finish push-universe / 3yr-backfill / D2 scripts / spot-checks and
report here. By the time it checked, all four were already done (above) — it verified rather than
redid the work:

- `Hood.universe`: 1,116 rows, schema matches (ticker, name, type, source, sector, industry,
  active_flag, added_at, classified_at).
- `Hood.Daily_prices`: 1,116 distinct tickers, **0 under 260 rows**.
- `Hood.events`: 63,482 rows, 1,116 distinct tickers.
- `Hood.weekly_bars` / `monthly_bars` / `tradelab.weekly_features` / `monthly_features`: all queryable,
  175,212 / 41,292 / 175,212 / 41,292 rows respectively.
- `tradelab-reader` key: `weekly_features`/`monthly_features` both readable; `Hood.weekly_bars` queried
  directly with the same key is still denied — confirms the authorized-view scoping holds for these
  two new views too, not just `daily_market`.
- Own spot-check (independent of the AAPL/AMD/NVDA check already logged above): each of AAPL, AMD,
  NVDA has a real `vol_event` (volume 2.2–2.4x its trailing 50-day average, on a distinct real date)
  and a real weekly `vol_outlier` (`vol_ratio_10w` 2.0–2.7x). AAPL's outlier week landed on
  2026-06-26 — the same date this file's D2 section already named.

D1/D2 fully done and independently confirmed. Stopping here per instructions (report in this repo,
not trade-lab's). D3/D4 still on hold.
