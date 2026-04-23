# wetteralarm-hail-fetcher

External worker that pulls MeteoSchweiz Open-Data hail products (POH, MESHS)
from the STAC collection `ch.meteoschweiz.ogd-radar-hail` and pushes sparse
pixel data to the Schadensplausibilisierung app at `schaden.wetteralarm.ch`.

## Why this lives in a separate public repo

The Schadensplausibilisierung app is hosted on Infomaniak shared hosting,
where `exec()` / `shell_exec()` are disabled and no HDF5 PHP extension is
available. HDF5 therefore has to be parsed elsewhere — this worker does the
conversion and pushes the result as JSON via an authenticated HTTPS POST.

Keeping this repo public makes GitHub Actions free (unlimited minutes for
public repositories). The code does nothing sensitive — the ingest token is
stored as a GitHub Secret and is not part of the code.

## How it works

1. GitHub Actions schedules the worker every 5 minutes (`cron: "*/5 * * * *"`).
2. Outside the hail season (Oct–Mar) the script exits immediately.
3. It queries the STAC items endpoint, groups `.h5` files by 5-min slot
   (`BZC*.h5` → POH, `MZC*.h5` → MESHS), skipping the daily sums.
4. For each slot that has not been ingested yet (`GET ?check=…`), it
   downloads the files, reads them via `h5py`, filters pixels with POH
   ≥ threshold and converts native grid coordinates to WGS84.
5. The pixel list is POSTed to `/api/hail-ingest.php` with the
   `X-Ingest-Token` header. Duplicates are rejected server-side.

## Setup

### GitHub Secrets (Settings → Secrets and variables → Actions)

- `STAGE_INGEST_URL` — e.g. `https://schaden.wetteralarm.ch/stage/api/hail-ingest.php`
- `STAGE_INGEST_TOKEN` — must match `HAIL_INGEST_TOKEN` in the Infomaniak `.env.stage`
- `PROD_INGEST_URL` — e.g. `https://schaden.wetteralarm.ch/api/hail-ingest.php`
- `PROD_INGEST_TOKEN` — must match `HAIL_INGEST_TOKEN` in the Infomaniak `.env.production`

The scheduled run targets **stage only**. Production is triggered manually via
`workflow_dispatch` once we are happy with stage behaviour.

### Local test

```bash
pip install -r requirements.txt
INGEST_URL=https://schaden.wetteralarm.ch/stage/api/hail-ingest.php \
INGEST_TOKEN=hail_ingest_stage_… \
POH_THRESHOLD=10 \
LOOKBACK_MINUTES=30 \
python fetch_hail.py
```

## Backfill

Use the `backfill-hail` workflow to load older frames from the STAC 14-day
rolling window. Triggered manually from **Actions → backfill-hail → Run workflow**.

Inputs:

- **target** — `stage` or `production`
- **lookback_minutes** — how far back to look (1440 = 1 day, 10080 = 7 days,
  20160 = 14 days, the STAC max)
- **max_frames** — safety cap per run (default 500). Re-run to continue from
  where the previous run stopped (idempotent via the `?check=` endpoint).
- **poh_threshold** — same semantics as the regular worker

Re-running the same backfill is safe: already-ingested frames are skipped.

## Tuning

- `POH_THRESHOLD` — minimum POH value (%) kept per pixel. Default 10. Lower
  means more data and larger DB. Useful for near-miss analyses.
- `LOOKBACK_MINUTES` — how far back to look at the STAC feed. 30 min is enough
  to catch up after a missed run or a late publication.

## Ownership

Part of the Wetter-Alarm Schadensplausibilisierung stack. Source of truth for
hail data ingested into the Infomaniak MySQL.
