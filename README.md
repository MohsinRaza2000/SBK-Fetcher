# SBK statistics data pipeline

Fetches past car-auction results from the trade statistics source and forwards
each page to the SBK auction portal's ingest endpoint, which stores them for the
portal's "Past auction prices" page.

The portal's own hosting server cannot reach the source (its IP is blocked), so
this scheduled job does the fetching from GitHub's runners and posts the rows on.

- `aaa_fetch.py` — logs in, pages each maker, POSTs to the portal. No credentials
  in the code; they come from repository **Secrets** at run time:
  `AAA_USER`, `AAA_PASS`, `AAA_INGEST_TOKEN`, `AAA_INGEST_URL`.
- `.github/workflows/fetch.yml` — runs it on a schedule, one request a second,
  and commits `aaa_fetch_state.json` so the next run carries on where it stopped.
- After the first full sweep the job only re-reads the newest pages of each maker
  (the daily top-up), so it stays light.
