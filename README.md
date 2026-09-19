# SBK statistics data pipeline

Fetches past car-auction results from the trade statistics source and forwards
each page to the SBK auction portal's ingest endpoint, which stores them for the
portal's "Past auction prices" page.

The portal's own hosting server cannot reach the source (its IP is blocked), so
this scheduled job does the fetching from GitHub's runners and posts the rows on.

- `aaa_fetch.py` — compares counts and reads only what is missing. The portal
  says how many rows it holds per maker and sale day; the first page of any
  sale-date window at the source says how many the source has; only windows that
  are short are read (big ones are split into single days first). The newest five
  sale days come first, every 90 minutes; the history behind them is filled with
  what is left of the day's allowance. No credentials in the code; they come from
  repository **Secrets** at run time: `AAA_USER`, `AAA_PASS`, `AAA_INGEST_TOKEN`,
  `AAA_INGEST_URL`.
- `.github/workflows/fetch.yml` — runs it, at most one request every 1.5 seconds
  and a daily allowance (repository Variables `AAA_DAILY_BUDGET`, `AAA_RUN_LIMIT`),
  and commits `aaa_fetch_state.json` (the day's count, the windows known complete).
- `python aaa_fetch.py --plan` shows what a run would look at without asking the
  source anything.
- `probe_gap.py` / `probe.yml` — a by-hand measuring run: source counts beside the
  portal's for a few slices, about two dozen requests.
