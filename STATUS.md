# M2 earnings-quality sample (F16--F18, 2026-10-30)

- Existing cadence verified, not duplicated: the calendar workflow runs on every trading-day processing run and fundamentals weekly.
- Loaded only NUE, INGM, XMTR, HNGE and HTFL into `Hood.earnings_quality_quarterly`; `tradelab.v_earnings_quality_quarterly` is an authorized read-only Hood view.
- F16/F17 are issuer release / 8-K reconciliation facts (`source_url` retained); F18 is an independently labelled four-quarter EODHD operating-cash-flow versus GAAP-net-income comparison. Null adjusted EPS means no comparable reconciliation was published (NUE Q3), never zero or inferred.
- Sample reconciliation gaps: INGM +$0.34/share (0.57 to 0.91), XMTR +$0.26 (-0.10 to 0.16), HNGE +$0.07 (0.52 to 0.59), HTFL +$0.11 (-0.18 to -0.07). No full quality sweep or quality refresh schedule was started.

# M2 full-universe sweep and refresh cadence (2026-10-30)

- Explaining `vol_event` **58,434 → 36,603**: 58,434 already included the 50-prior-session guard. Removing 23,315 all-null placeholder/lifecycle price rows and rebuilding yielded 36,632 (a 21,802-event drop); deleting the two NYSE-holiday dates and repairing two NUE bars yielded the final 36,603 (29 net fewer). This is a data-cleanup effect, not a changed event threshold.
- Swept **1,049 active common stocks** through the master loader. Active coverage: 1,047 tickers with fundamentals and 1,048 with earnings-calendar facts. `NIQ` returned no fundamentals; `NUGT` returned no facts; both responses were successful but empty and remain absent.
- Added `.github/workflows/eodhd_facts_refresh.yml`: earnings calendar refreshes every trading-day run (Tue-Sat UTC) and fundamentals refresh weekly (Sunday). No post-sweep manual refresh run was started.

# M2 fundamentals and earnings sample (2026-10-30)

- Loaded only NUE, INGM, XMTR, HNGE and HTFL: 40 `Hood.fundamentals_quarterly` rows and 47 `Hood.earnings_calendar` rows. Each fact carries EODHD source text and `retrieved_at`.
- Added `tradelab.v_fundamentals_quarterly` and `tradelab.v_earnings_calendar` as authorized Hood views. `tradelab-reader` can read both views and is denied both raw Hood fact tables.
- M2 validation: NUE Q3 matches its release ($11.249B sales, $5.80 EPS); XMTR/HNGE/HTFL future dates and BMO/AMC timing match their announcements. INGM remains intentionally flagged: EODHD has Q3 EPS $0.72 vs its release’s $0.57 GAAP / $0.91 non-GAAP and its fundamental facts only reach Q2.
- No full ticker sweep and no scheduled refresh were added. Stop for Jo’s approval.

# Price-data repairs (2026-10-30)

- Deleted the 161 Labor Day (2026-09-07) `Hood.Daily_prices` rows after the earlier Memorial Day cleanup. A fresh 2023-10-30 through 2026-10-30 NYSE audit finds **zero** non-session dates.
- The shared ingest calendar test now verifies every 2026 full NYSE holiday is rejected; the same `fetch_yahoo_prices()` path is used by nightly incremental and weekly full-refresh jobs.
- Replaced only NUE 2026-10-28 and 2026-10-29 after a date-scoped Yahoo re-fetch passed OHLC checks. The new closes are 271.790009 and 275.290009, respectively.
- Created `pricesz.Hood.price_repairs` and recorded both before/after bars, source, reason, repair ID, and timestamp. Repair ID: `a0cb22da-8e3d-4d3b-bff1-130e94759925`.
- Rebuilt `Hood.events`: **36,603** `vol_event` rows.

# EODHD All-in-One validation (2026-10-30)

All paid-endpoint responses below are cached in gitignored `.eodhd_cache/`; no token or raw cache file is committed. Historical 401/403 cache entries are automatically retried because a plan entitlement can change after a response was cached.

| Check | Expected | Got | Pass |
|---|---|---|---|
| NUE earnings calendar | Date/timing and actual vs estimate | 2026-10-26, AfterMarket, EPS 5.80 vs 5.71 estimate | Yes |
| INGM earnings calendar | 2026-10-29 AMC; adjusted EPS 0.91 vs 0.78; sales 15.2B vs 13.9B | 2026-10-29, AfterMarket, EPS **0.72** vs 0.78. Calendar has no sales fields. | **No** — EPS differs; sales unavailable here. |
| XMTR earnings calendar | 2026-11-03 BMO | 2026-11-03, BeforeMarket, EPS estimate 0.00; actual pending | Yes |
| HNGE earnings calendar | Date/timing and actual vs estimate | 2026-11-02, AfterMarket, EPS estimate 0.27; actual pending | Yes (upcoming) |
| HTFL earnings calendar | Date/timing and actual vs estimate | 2026-11-12, AfterMarket, EPS estimate -0.18; actual pending | Yes (upcoming) |
| NUE Q3 2026 fundamentals | EPS 5.80; sales 11.25B | EPS 5.80; revenue 11.249B | Yes |
| Eight-quarter fundamentals | Sales and EPS history for all five | Eight sales quarters returned for each. INGM stops at 2026-Q2; HNGE/HTFL have missing EPS history for older periods. | Partial |
| Delisted history | A known delisted ticker has historical bars | `TWTR.US`: six bars from 2022-10-20 through its last session, 2022-10-27 | Yes |
| NUE close differences | Match Hood or identify cause | 2026-10-28: Hood 266.700012 / 274,076 volume vs EODHD 271.79 / 2,044,056. 2026-10-29: Hood 273.859985 / 21,657 vs EODHD 275.29 / 1,485,697. | **No** — not a corporate-action adjustment (both `adj_close == close`); Hood’s Yahoo-sourced rows are incomplete/corrupt (including 2026-10-28 `open` 270 > `high` 269.90). |
| GitHub Actions secret call | One Action call using `EODHD_API_TOKEN` | Jo manually ran the secret-safe `.github/workflows/eodhd_secret_smoke.yml` workflow; smoke test passed. | Yes |

### Eight-quarter compact history (sales USD billions / actual EPS)

| Ticker | Latest eight fundamental periods, newest first |
|---|---|
| NUE | 2026-Q3 11.249 / 5.80; Q2 10.397 / 4.84; Q1 9.496 / 3.23; 2025-Q4 7.687 / 1.73; Q3 8.521 / 2.63; Q2 8.456 / 2.65; Q1 7.830 / 0.77; 2024-Q4 7.076 / 1.22 |
| INGM | 2026-Q2 14.531 / 0.82; Q1 13.963 / 0.75; 2025-Q4 14.878 / 0.96; Q3 12.604 / 0.72; Q2 12.794 / 0.61; Q1 12.281 / 0.61; 2024-Q4 13.345 / 0.92; Q3 11.763 / 0.72 |
| XMTR | 2026-Q2 0.229 / -0.05; Q1 0.205 / 0.12; 2025-Q4 0.192 / 0.16; Q3 0.181 / 0.11; Q2 0.163 / 0.09; Q1 0.151 / 0.02; 2024-Q4 0.149 / 0.06; Q3 0.142 / 0.02 |
| HNGE | 2026-Q2 0.213 / 0.66; Q1 0.182 / 0.45; 2025-Q4 0.171 / 0.49; Q3 0.154 / 0.44; Q2 0.139 / 0.70; Q1 0.124 / 0.2044; 2024-Q4 0.117 / —; Q3 0.101 / — |
| HTFL | 2026-Q2 0.064 / -0.16; Q1 0.053 / -0.16; 2025-Q4 0.049 / -0.1182; Q3 0.046 / -0.27; Q2 0.043 / -1.10; Q1 0.037 / —; 2024-Q4 0.035 / —; Q3 0.033 / — |

# EODHD Sandbox smoke test (2026-10-30)

### Result

Live smoke test completed with **10 assumed calls of the 15-call task cap**. The initial `/user` response reported `apiRequests: 0` and `dailyRateLimit: 20`. Every response is cached in gitignored `.eodhd_cache/`; credentials and raw cache data are not in Git.

The client now loads `EODHD_API_TOKEN` only from the external file named by `TRADE_ENV_FILE`, defaulting to `C:\Users\Mindrix\.secrets\trade.env`. It deliberately does **not** load a checkout `.env`.

### Requested checks

| Endpoint / check | Works on free tier? | Matches expected? | Notes |
|---|---|---|---|
| User / limits | Yes | Yes | Reported 20 daily calls. |
| EOD prices: NUE, last 30 days | Yes | Partly | 22 matching trading dates in both sources. 20 closes matched; Hood differed on 2026-10-28 (266.700012 vs EODHD 271.79) and 2026-10-29 (273.859985 vs 275.29). |
| Earnings calendar: INGM around 2026-10-29 | No | Not tested | Confirmed HTTP 403: endpoint not in plan; BMO/AMC and estimate/actual cannot be checked on Sandbox. |
| Earnings calendar: XMTR around 2026-11-03 | No | Not tested | Confirmed HTTP 403: endpoint not in plan; timing cannot be checked on Sandbox. |
| News: XMTR, prior 7 days | Yes | Fetched | 3 articles. Latest returned headlines: “ACM Research And 2 Other Great Automation Stocks To Own”; “Illinois Tool Works (ITW) Surpasses Q3 Earnings Estimates”; “RiskVantage AI Unveils Explainable, Sovereign AI for the $75 Billion Contract and Deal Legal Market at Dubai AI Festival”. |
| US exchange symbols with `delisted=1` | Yes | Fetched | 59,978 US entries returned. Response has no per-row delisted flag, so inclusion/count of delisted entries cannot be independently established from this payload. |
| NUE fundamentals: quarterly sales/EPS | Not run | Not tested | Skipped: it costs 10 calls, while only 5 remained under the task cap. It also requires a paid Fundamentals/All-in-One entitlement. |

### Paid-plan endpoints confirmed or required

- `calendar/earnings`: confirmed unavailable on Sandbox (HTTP 403).
- `v1.1/fundamentals`: not called to preserve the cap; requires a paid Fundamentals/All-in-One entitlement.

### Client and safety

- `eodhd_client.py` provides one method per requested endpoint, 429 exponential backoff, clear sanitized 401/403 plan errors, and token-free caching.
- `.eodhd_cache/` is gitignored. No token, secret-file path contents, or response payloads were committed.

# tradelab authorized Hood source views (2026-10-30)

- Created `pricesz.tradelab.v_daily_prices` over `pricesz.Hood.Daily_prices` and `pricesz.tradelab.v_universe` over `pricesz.Hood.universe`.
- Added only those two views to the Hood dataset authorized-view ACL. No weekly/monthly views were created.
- Verified with the `tradelab-reader` credential: both new views are readable (`818,868` price rows; `1,118` universe rows), while direct reads of both Hood source tables are denied.
- `create_tradelab_source_views.py` reproduces the two views and their authorized-view ACL entries.

---
# STATUS — hoodb

## Corporate-action cleanup and placeholder deletion (2026-10-29)

This section supersedes the live counts in the 2026-10-28 audit below (that
section is retained as the investigation record).

### Outcome

- Deleted **23,315** incomplete `Daily_prices` rows, scoped exactly to the
  authorized categories: all rows belonging to the seven wholly unavailable
  symbols, plus incomplete rows on or before the verified first-valid-bar
  cutoff for 47 symbols. No complete price row was targeted or deleted.
- Verification now returns **0 pre-first-bar incomplete rows** and **0 rows for
  a ticker with no complete history**.
- **705 incomplete rows remain outside that deletion scope:** 319 holiday
  alignment rows (159 on 2026-05-25 and 160 on 2026-09-07), 384 post-last-trade
  rows for eight inactive securities, and one 2026-10-29 Yahoo miss each for
  active `MSGS` and `QRVO`. A concurrent nightly run, started before this fix
  was pushed, briefly added 33 new latest-day placeholders; individual Yahoo
  requests repaired 23 active symbols. The ingestion guard below prevents
  another batch-alignment recurrence after this commit is deployed.
- Added `inactive_reason` to `Hood.universe` and the local schema. Final
  universe: **1,118 rows, 1,103 active, 15 inactive**; every inactive row has
  exactly one of `delisted` or `renamed`, and no active row has a reason.
- Added active successor symbols `DFTX` and `VMRK` (`source=corporate_action`).
  Each has **753 complete rows**, 2023-10-30 through 2026-10-29, with no
  incomplete values. Yahoo carries the predecessor history continuously under
  both current symbols.
- Rebuilt `Hood.events` after all price changes: **36,632** `vol_event` rows.

### Corporate-action findings

The seven formerly “unavailable” symbols are fully reconciled:

- `AL` — acquired and taken private by Sumisho Air Lease on 2026-04-08;
  `delisted`, no public successor. ([Sumitomo completion notice](https://www.sumitomocorp.com/en/europe/news/release/2026/group/20970))
- `CNTA` — acquired by Lilly on 2026-06-24; `delisted`, no ticker continuity.
  ([Lilly completion notice](https://investor.lilly.com/news-releases/news-release-details/lilly-completes-acquisition-centessa-pharmaceuticals-advance))
- `CWEN-A` — Class A converted one-for-one into the still-active Class C
  `CWEN`; `renamed`. `CWEN` was already in the universe with its own valid
  history, so no cross-class price copy was made. ([Clearway 10-Q](https://www.sec.gov/Archives/edgar/data/1567683/000162828026053421/cwen-20260630.htm))
- `MASI` — acquired for cash by Danaher on 2026-06-10; `delisted`, no ticker
  continuity. ([Masimo completion notice](https://www.masimo.com/media/danaher-completes-acquisition-of-masimo-corporation))
- `MNMD` — same issuer renamed Definium Therapeutics and changed ticker to
  `DFTX` on 2026-01-15; `renamed`. `DFTX` was added and its continuous history
  loaded. ([Definium 10-K](https://ir.definiumtx.com/sec-filings/all-sec-filings/content/0001193125-26-076742/dftx-20251231.htm))
- `NSA` — acquired by Public Storage on 2026-07-22; `delisted`. `PSA` remains
  a distinct security, so NSA history was not copied into PSA. ([Public Storage completion notice](https://investors.publicstorage.com/news-events/press-releases/news-details/2026/Public-Storage-Announces-Closing-of-National-Storage-Affiliates-Acquisition/default.aspx))
- `TERN` — acquired by MSD/Merck on 2026-05-05; `delisted`, no ticker
  continuity. ([MSD completion notice](https://www.msd.com/news/msd-completes-acquisition-of-terns-pharmaceuticals-inc/))

The requested sparse-history review found:

- `EA` — cash acquisition closed after trading on 2026-08-04; `delisted`, no
  successor history. ([Nasdaq corporate-action notice](https://www.nasdaqtrader.com/TraderNews.aspx?id=ECA2026-545))
- `AVB` + `EQR` — merger completed 2026-08-17; the legal EQR issuer became
  Vivmark Residential and began trading as `VMRK` on 2026-08-18. `AVB` is
  `delisted`; `EQR` is `renamed`; continuous EQR/VMRK history is now stored
  under active `VMRK`. AVB history was not spliced into VMRK because AVB was
  the acquired security. ([Vivmark closing release](https://investors.avalonbay.com/sec-filings/all-sec-filings/content/0001104659-26-097833/tm2623381d1_ex99-1.htm))
- `WBD` — acquired for cash by Skydance on 2026-10-06 and ceased trading;
  `delisted`. `SKYD` is a different surviving security, so WBD history was not
  copied. ([Skydance completion notice](https://www.skydance.com/news/press/paramount-completes-acquisition-of-warner-bros-discovery-creating-a-new-global-entertainment-leader-skydance))
- `WBS` — Santander completed its acquisition on 2026-08-20; `delisted`, no
  direct ticker-history continuation. ([Santander completion notice](https://www.santander.com/en/press-room/press-releases/2026/08/santander-expands-us-presence-with-completion-of-webster-acquisition))
- `LBRDA` / `LBRDK` — Liberty Broadband merged into Charter on 2026-08-20;
  both `delisted`. Holders received `CHTR`, but Charter is a distinct issuer
  with its own existing history, so no splice was made. ([Nasdaq corporate-action notice](https://www.nasdaqtrader.com/TraderNews.aspx?id=ECA2026-577))
- `BEDZ` was the only additional >=1-year sparse symbol that was not a new
  listing. The ETF liquidated, with 2026-08-21 its final trading day;
  `delisted`. ([SEC liquidation supplement](https://www.sec.gov/Archives/edgar/data/1408970/000182912626008328/advisorshares-hotel_497.htm))

All other >=1-year pre-first-bar cases were current-security IPOs or spinoffs,
not missing predecessor history: `BLSH`, `CAI`, `CBC`, `CRCL`, `FDXF`, `FIGR`,
`FRMI`, `GLIBA`, `GLIBK`, `HONA`, `KRMN`, `MDLN`, `MRP`, `NIQ`, `Q`, `RAL`,
`SAIL`, `SFD`, `SNDK`, `SOLS`, `SPCX`, `VGNT`, and `VSNT`.

### Code and checks

- `ingest_update.py` drops a yfinance row if any OHLC, adjusted-close, or
  volume field is missing, before integer casting or upload.
- `build_universe.py`, `rebuild_universe_table.py`, and `bootstrap_bq.py`
  preserve/load `inactive_reason`; `build_universe.py` also preserves existing
  lifecycle/source metadata on rebuild.
- Compile checks passed for all four modified scripts.
- Synthetic two-symbol alignment test passed: three complete rows retained,
  one incomplete shared-index row dropped.
- `git diff --check` passed.

## Initial D3 cloud-run data-quality audit (2026-10-28; historical)

### 1. `Hood.Daily_prices` incomplete OHLCV

**Result:** the 24,756 rows were not partial trading bars. In every case,
`open`, `high`, `low`, `close`, and `adj_close` were all NULL and `volume` was
0. `ingest_update.fetch_yahoo_prices()` had retained all-NaN alignment rows
from multi-symbol yfinance downloads, then converted the missing volume to 0.
There were no isolated missing fields and no evidence of trading halts.

I refetched all 210 affected symbols individually for the full date window.
Only complete OHLCV+adjusted-close responses were eligible for repair. The
first pass found 2,462 matching bars, but 1,709 of those preceded the current
security's first known bar (`BLSH`, `SNDK`, `SPCX`, `TEM`, `VIK`) and were
symbol-reuse/pre-listing collisions. Those 1,709 changes were reverted. No
placeholder rows were deleted.

Final outcome:

- **753 rows repaired:** `QRVO` 751 missing dates (the existing 2026-10-02
  bar was already complete), `MSGS` 2026-10-28, and `BEDZ` 2026-08-14.
- **24,003 rows remain incomplete across 208 tickers.** These were left
  unchanged because an individual Yahoo request did not return a defensible
  complete bar for that ticker/date.
- **2026-10-28:** 17 before, 2 repaired (`MSGS`, `QRVO`), 15 unresolved:
  `AL`, `AVB`, `BEDZ`, `CNTA`, `CWEN-A`, `EA`, `EQR`, `LBRDA`, `LBRDK`,
  `MASI`, `MNMD`, `NSA`, `TERN`, `WBD`, `WBS`.
- `Hood.events` was rebuilt after the final correction: **57,759**
  `vol_event` rows.

The 24,003 unresolved rows break down exactly as follows (categories can
overlap by ticker):

1. **18,037 pre-first-Yahoo-bar placeholders, 47 tickers.** This category
   includes genuine new listings/spinoffs and symbol-reuse cases. It also
   includes several established-looking symbols for which Yahoo returned only
   sparse late history; those are Yahoo coverage/symbol-lifecycle anomalies,
   not asserted to be IPOs. Format is `ticker: rows [first..last]`:

   `ALAB:97 [2023-10-30..2024-03-19]`; `AMTM:226
   [2023-10-30..2024-09-23]`; `AS:64 [2023-10-30..2024-01-31]`; `AVB:699
   [2023-10-30..2026-08-13]`; `BEDZ:699 [2023-10-30..2026-08-13]`;
   `BLSH:447 [2023-10-30..2025-08-12]`; `CAI:409
   [2023-10-30..2025-06-17]`; `CBC:518 [2023-10-30..2025-11-20]`;
   `CRCL:400 [2023-10-30..2025-06-04]`; `EA:691
   [2023-10-30..2026-08-03]`; `ECG:250 [2023-10-30..2024-10-25]`;
   `EQR:700 [2023-10-30..2026-08-14]`; `FDXF:644
   [2023-10-30..2026-05-26]`; `FIGR:467 [2023-10-30..2025-09-10]`;
   `FRMI:481 [2023-10-30..2025-09-30]`; `GEV:102
   [2023-10-30..2024-03-26]`; `GLIBA:424 [2023-10-30..2025-07-10]`;
   `GLIBK:424 [2023-10-30..2025-07-10]`; `HONA:657
   [2023-10-30..2026-06-12]`; `INGM:248 [2023-10-30..2024-10-23]`;
   `KRMN:323 [2023-10-30..2025-02-12]`; `LBRDK:702
   [2023-10-30..2026-08-18]`; `LINE:184 [2023-10-30..2024-07-24]`;
   `LOAR:122 [2023-10-30..2024-04-24]`; `MDLN:535
   [2023-10-30..2025-12-16]`; `MRP:317 [2023-10-30..2025-02-04]`;
   `NBIS:245 [2023-10-30..2024-10-18]`; `NIQ:432
   [2023-10-30..2025-07-22]`; `Q:499 [2023-10-30..2025-10-24]`;
   `RAL:413 [2023-10-30..2025-06-24]`; `RBRK:122
   [2023-10-30..2024-04-24]`; `RDDT:98 [2023-10-30..2024-03-20]`;
   `RR:14 [2023-10-30..2023-11-16]`; `SAIL:323
   [2023-10-30..2025-02-12]`; `SARO:232 [2023-10-30..2024-10-01]`;
   `SERV:89 [2023-10-30..2024-03-07]`; `SFD:311
   [2023-10-30..2025-01-27]`; `SNDK:323 [2023-10-30..2025-02-12]`;
   `SOLS:494 [2023-10-30..2025-10-17]`; `SOLV:101
   [2023-10-30..2024-03-25]`; `SPCX:656 [2023-10-30..2026-06-11]`;
   `TEM:157 [2023-10-30..2024-06-13]`; `VGNT:603
   [2023-10-30..2026-03-26]`; `VIK:126 [2023-10-30..2024-04-30]`;
   `VSNT:533 [2023-10-30..2025-12-12]`; `WBD:734
   [2023-10-30..2026-10-02]`; `WBS:702 [2023-10-30..2026-08-18]`.

2. **5,264 symbol-unavailable placeholders, 7 tickers:** `AL`, `CNTA`,
   `CWEN-A`, `MASI`, `MNMD`, `NSA`, `TERN`; 752 session-shaped rows each,
   spanning 2023-10-30 through 2026-10-28. Individual Yahoo requests returned
   no usable history (404/delisted/no-timezone/authorization-style failures).

3. **376 post-last-Yahoo-bar placeholders, 8 tickers:** `AVB:52
   [2026-08-17..2026-10-28]`; `BEDZ:51 [2026-08-17..2026-10-28]`;
   `EA:60 [2026-08-05..2026-10-28]`; `EQR:51
   [2026-08-18..2026-10-28]`; `LBRDA:47 [2026-08-24..2026-10-28]`;
   `LBRDK:49 [2026-08-20..2026-10-28]`; `WBD:17
   [2026-10-06..2026-10-28]`; `WBS:49 [2026-08-20..2026-10-28]`.
   These are Yahoo history cutoffs/symbol-lifecycle anomalies; the data does
   not support calling them halts.

4. **326 US-market-holiday alignment placeholders:** 163 tickers on each of
   2026-05-25 and 2026-09-07. The exact ticker set is:

   `AA`, `AAPL`, `ABNB`, `ACHR`, `AFRM`, `AIQ`, `ALB`, `AMAT`, `AMD`,
   `AMKR`, `AMZN`, `APLD`, `APPN`, `ARKK`, `ARKQ`, `ARM`, `ARQT`, `ASML`,
   `ATEC`, `BE`, `BEDZ`, `BKCH`, `BOTZ`, `BTDR`, `CHWY`, `CIBR`, `CIFR`,
   `CNTA`, `COHR`, `COIN`, `CRDO`, `CRM`, `CRML`, `CRSP`, `CRWD`, `DAL`,
   `DIA`, `DQ`, `DTCR`, `EVTL`, `FCX`, `FINX`, `FROG`, `FRPT`, `FSLR`,
   `FTAI`, `FXI`, `GDX`, `GE`, `HAL`, `HIMS`, `HOOD`, `HUT`, `ICLN`,
   `IDRV`, `IGV`, `IHF`, `IHI`, `ILMN`, `INOD`, `INSM`, `INTC`, `IONQ`,
   `ISRG`, `ITA`, `IWM`, `JETS`, `JOBY`, `KVYO`, `LAR`, `LASR`, `LIND`,
   `LIT`, `MAGS`, `MDY`, `META`, `MNMD`, `MP`, `MU`, `NAMS`, `NB`, `NBIS`,
   `NEM`, `NET`, `NUE`, `NUGT`, `NVDA`, `NXE`, `NXT`, `ODD`, `OIH`,
   `OKLO`, `ONON`, `OSCR`, `OUT`, `PEJ`, `PL`, `PLTR`, `QBTS`, `QQQ`,
   `QTUM`, `RBLX`, `RCL`, `RDDT`, `REMX`, `RERE`, `RIVN`, `RKLB`, `ROBO`,
   `RR`, `RSP`, `RYAAY`, `RYTM`, `SERV`, `SGML`, `SKYY`, `SLB`, `SLX`,
   `SMH`, `SMR`, `SNDK`, `SNOW`, `SOCL`, `SOFI`, `SONO`, `SOUN`, `SOXX`,
   `SPHR`, `SPY`, `STX`, `SYM`, `TAN`, `TERN`, `TEVA`, `TME`, `TSLA`,
   `TTMI`, `U`, `UAMY`, `UBER`, `UNH`, `URA`, `USAR`, `VIK`, `WGS`, `WMT`,
   `XBI`, `XLB`, `XLC`, `XLE`, `XLF`, `XLI`, `XLK`, `XLP`, `XLRE`, `XLU`,
   `XLV`, `XLY`, `XME`, `XPEV`, `XRT`, `XYZ`, `ZETA`.

There were **no unresolved interior Yahoo gaps** once holidays and
pre/post-history ranges were separated.

Prevention: `ingest_update.py` now drops any Yahoo row missing one or more of
OHLC, adjusted close, or volume before casting/upload. A synthetic two-symbol
alignment test passed: one incomplete row was dropped and all three complete
rows were retained. `python -m py_compile ingest_update.py` also passed.

### 2. Sector reconciliation: 10 vs 49

Both counts are correct but measured different scopes:

- `Hood.universe` contains **65 raw NULL-sector rows**: 10 rows typed `stock`,
  54 typed `etf`, and the `VIX` benchmark.
- The earlier **10 unresolved** report came from `classify_universe.py`, which
  deliberately classifies only rows whose `type == "stock"`; it skips ETFs
  and benchmarks.
- D3 supplies **16 context overrides** for `SPY`, `QQQ`, `IWM`, `DIA`, `VIX`
  and the 11 sector ETFs (`XLB`, `XLC`, `XLE`, `XLF`, `XLI`, `XLK`, `XLP`,
  `XLRE`, `XLU`, `XLV`, `XLY`). Therefore 65 - 16 = **49** rows remain
  without sector after D3 overrides.

The exact 49 are:

- **10 stock-typed rows:** `AL`, `CNTA`, `CWEN-A`, `FISV`, `MASI`, `MNMD`,
  `NIQ`, `NSA`, `NUGT`, `TERN`.
- **39 ETF-typed rows outside D3's overrides:** `AIQ`, `ARKK`, `ARKQ`,
  `BEDZ`, `BKCH`, `BOTZ`, `CIBR`, `DTCR`, `EVTL`, `FINX`, `FXI`, `GDX`,
  `ICLN`, `IDRV`, `IGV`, `IHF`, `IHI`, `ITA`, `JETS`, `LIT`, `MAGS`, `MDY`,
  `OIH`, `PEJ`, `QTUM`, `REMX`, `ROBO`, `RSP`, `SKYY`, `SLX`, `SMH`,
  `SNDK`, `SOCL`, `SOXX`, `TAN`, `URA`, `XBI`, `XME`, `XRT`.

Two universe type labels also deserve follow-up: `NUGT` is a leveraged ETF
but is typed `stock`, while `SNDK` is typed `etf` even though its universe name
is `ScanDisk`. No sector values or type labels were changed in this audit; the
49-count difference is entirely the stock-only classifier versus D3's broader
post-override feature population.

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
  rebuilt against the full backfilled history, 58,434 rows (see the full-window
  fix below — this count supersedes the 63,482 first reported).
- `Hood.weekly_bars` / `Hood.monthly_bars`: views over `Daily_prices`,
  first/last trading day per period, `is_complete` flag.
- `tradelab.weekly_features` / `tradelab.monthly_features`: views over the
  bars views (`close_location`, `range_vs_avg10`), authorized on `Hood`'s
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

## D1/D2 review fixes (post-review, same day)

Jo reviewed D1/D2 and flagged three issues in `build_events.py` / `build_feature_views.py`. All three
fixed, re-run against BigQuery, and verified:

1. **Partial windows were producing events/outliers judged on incomplete history.** `avg_vol_50d`
   (`build_events.py`), and the weekly `avg_vol_10w` / `avg_range_10w` / `sma_10w` / `high_52w`
   (`build_feature_views.py`, plus the monthly `avg_vol_10m` / `avg_range_10m` analogs), all used
   BigQuery window frames that silently average over however many prior rows exist — so a ticker's
   first 50 days (events) or first 10/52 weeks (features) were being flagged against 1-49 days or
   1-9/1-51 weeks of history instead of a full window. Fixed by adding a `COUNT(*) OVER` guard on each
   window and returning NULL (no event/outlier) until the window is actually full (50 days / 10
   periods / 52 weeks). Verified: 0 vol_event rows now fall within a ticker's first 50 trading days
   (was producing false positives there before); 0 weekly_features rows now carry a non-NULL
   `vol_ratio_10w`/`range_vs_avg10` in a ticker's first 10 weeks, or a non-NULL `high_52w` before week
   52. `Hood.events` vol_event count dropped from 63,482 to 58,434 as the early-history false
   positives were removed.
2. **`build_events.py` rebuilt the whole table, which would wipe non-`vol_event` rows (earnings,
   etc.) once added.** Changed from `CREATE OR REPLACE TABLE` to `DELETE FROM ... WHERE type =
   'vol_event'` + `INSERT ... SELECT` scoped to `vol_event` rows only, with `CREATE TABLE IF NOT
   EXISTS` guarding first-run. Verified live: inserted a fake `type = 'earnings'` row, ran
   `build_events.py`, confirmed the earnings row survived the rebuild while `vol_event` rows refreshed
   normally (58,434, unchanged) — then removed the test row.
3. **`range_atr` was not ATR** — it's each bar's own range vs. its own trailing-10-period average
   range, not a true ATR. Renamed to `range_vs_avg10` in both `tradelab.weekly_features` and
   `tradelab.monthly_features`.

All three live in `build_events.py` and `build_feature_views.py`, re-run against BigQuery, output
re-verified via direct queries (see counts above). Stopping here per instructions. D3 still waits on
the strategy agent's trade-lab commit.
