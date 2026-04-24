#!/usr/bin/env python3
"""
Hail Fetcher Worker.

Läuft alle 5 Minuten via GitHub Actions (April-September).
Pollt die STAC-Collection ch.meteoschweiz.ogd-radar-hail, lädt neue
HDF5-Assets herunter, extrahiert POH (BZC) und MESHS (MZC), filtert Pixel
mit POH >= POH_THRESHOLD und POSTet sie an den Ingest-Endpoint von
schaden.wetteralarm.ch.

Env-Variablen (via GitHub Secrets):
  INGEST_URL          - z.B. https://schaden.wetteralarm.ch/stage/api/hail-ingest.php
  INGEST_TOKEN        - muss matchen mit HAIL_INGEST_TOKEN in der .env auf Infomaniak
  POH_THRESHOLD       - optional, default 10 (nur Pixel mit POH >= diesem Wert werden gesendet)
  LOOKBACK_MINUTES    - optional, default 30 (wie weit zurück in STAC geschaut wird)

Exit-Code:
  0  alle Frames erfolgreich oder schon vorhanden
  1  mindestens einer fehlgeschlagen
  2  Konfiguration fehlt
"""
from __future__ import annotations

import json
import logging
import os
import re
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import h5py
import numpy as np
import requests
from pyproj import Transformer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("hail-fetcher")

STAC_COLLECTION = "ch.meteoschweiz.ogd-radar-hail"
STAC_ITEMS_URL = (
    f"https://data.geo.admin.ch/api/stac/v1/collections/{STAC_COLLECTION}/items"
)

# Schweizer Radar-Composite (ODIM-HDF5): LV95 unterer linker Pixelmittelpunkt
# Quelle: MeteoSchweiz Open-Data-Doku, "Hail radar products" (D3)
# Das Grid ist 710 x 640 (spaltenweise / zeilenweise) à 1 km, mit
# LL_EAST/LL_NORTH als unteren Eckpunkt. Die konkreten Werte werden
# aus den HDF5-Metadaten gelesen (where/LL_lon etc.).
# Transformer WGS84 <- LV95 wird einmal initialisiert.
TRANSFORM_LV95_TO_WGS84 = Transformer.from_crs("EPSG:2056", "EPSG:4326", always_xy=True)

FNAME_RE = re.compile(
    r"^(?P<code>bzc|mzc)(?P<yy>\d{2})(?P<jjj>\d{3})(?P<hhmm>\d{4})",
    re.IGNORECASE,
)


@dataclass
class FrameAssets:
    timestamp: datetime  # UTC, gerundet auf 5 Min
    poh_url: str | None = None
    meshs_url: str | None = None


def require_env(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        log.error("Missing required env var: %s", name)
        sys.exit(2)
    return v


def parse_fname_timestamp(fname: str) -> datetime | None:
    """BZCyyjjjHHMMkk.xxx.h5 -> UTC datetime (5-Min-Slot, ignoriert Tagessummen)."""
    m = FNAME_RE.match(fname)
    if not m:
        return None
    hhmm = m.group("hhmm")
    # Tagessummen: HHMM = 2400 (POH) oder 3000 (MESHS) — überspringen
    if hhmm in {"2400", "3000"}:
        return None
    try:
        year = 2000 + int(m.group("yy"))
        doy = int(m.group("jjj"))
        hour = int(hhmm[:2])
        minute = int(hhmm[2:])
        base = datetime(year, 1, 1, tzinfo=timezone.utc) + timedelta(days=doy - 1)
        return base.replace(hour=hour, minute=minute, second=0, microsecond=0)
    except (ValueError, IndexError):
        return None


def discover_frames(lookback_minutes: int) -> list[FrameAssets]:
    """
    Fragt STAC alle Items ab (Collection hat eine 14-Tage-Rolling-Window) und
    filtert die Assets per **Dateiname-Zeitstempel** auf das Lookback-Fenster.

    Die Item-Property `datetime` ist hier der letzte Update-Zeitstempel des
    Tages-Buckets, NICHT die Messzeit der 5-Min-Files — daher taugt sie nicht
    als STAC-Filter.
    """
    since = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
    slots: dict[datetime, FrameAssets] = {}

    url: str | None = STAC_ITEMS_URL
    params: dict[str, str | int] | None = {"limit": 100}
    total_items = 0
    total_assets = 0

    while url:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        for feat in data.get("features", []):
            total_items += 1
            for _asset_id, asset in (feat.get("assets") or {}).items():
                href = asset.get("href", "")
                if not href.lower().endswith(".h5"):
                    continue
                total_assets += 1
                fname = href.rsplit("/", 1)[-1]
                ts = parse_fname_timestamp(fname)
                if ts is None or ts < since:
                    continue

                slot = slots.setdefault(ts, FrameAssets(timestamp=ts))
                fname_lower = fname.lower()
                if fname_lower.startswith("bzc"):
                    slot.poh_url = href
                elif fname_lower.startswith("mzc"):
                    slot.meshs_url = href

        # STAC-Pagination: follow next link, wenn vorhanden
        next_link = next(
            (lnk for lnk in (data.get("links") or []) if lnk.get("rel") == "next"),
            None,
        )
        if next_link and next_link.get("href"):
            url = next_link["href"]
            params = None  # next-URL hat query-params bereits eingebaut
        else:
            url = None

    log.info(
        "Scanned %d items / %d h5-assets; %d slots in lookback window",
        total_items, total_assets, len(slots),
    )

    # Nur Frames, die mindestens POH haben (MESHS ist optional — kein Hagel ≥ 2 cm)
    return sorted(
        (s for s in slots.values() if s.poh_url),
        key=lambda s: s.timestamp,
    )


def frame_already_ingested(ingest_url: str, ts: datetime) -> bool:
    """GET ?check=YYYYMMDDHHmm — Existenz-Check, kein Token nötig."""
    try:
        r = requests.get(
            ingest_url,
            params={"check": ts.strftime("%Y%m%d%H%M")},
            timeout=15,
        )
        if r.status_code != 200:
            return False
        return bool(r.json().get("exists", False))
    except requests.RequestException as e:
        log.warning("check-request failed for %s: %s", ts, e)
        return False


def download(url: str, dest: Path) -> None:
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in r.iter_content(chunk_size=65536):
                fh.write(chunk)


def _value_mask(arr: np.ndarray, sentinel: float) -> np.ndarray:
    """
    Liefert eine bool-Maske für Pixel die `sentinel` matchen.
    NaN-safe: bei sentinel=NaN nutzt np.isnan() (weil NaN == NaN immer False ist).
    """
    if np.isnan(sentinel):
        return np.isnan(arr)
    return arr == sentinel


def read_odim_raster(h5_path: Path) -> tuple[np.ndarray, dict]:
    """
    Liest ODIM-HDF5: 2D-Array + geo-Metadaten.

    MeteoSchweiz POH/MESHS-Files (verifiziert 2026-04-24):
      - dtype: float64
      - nodata: NaN (nicht 255 wie ODIM-Default)
      - undetect: 0.0
      - quantity: 'POH' resp. 'MESHS'

    Returns (data_array, meta). Werte sind skaliert (gain/offset),
    nodata-Pixel als np.nan, undetect bleibt 0.
    """
    debug = os.environ.get("DEBUG_PARSE", "").strip().lower() in {"1", "true", "yes"}

    with h5py.File(h5_path, "r") as f:
        if debug:
            log.info("--- HDF5 root keys: %s", list(f.keys()))
            for k in f.keys():
                if hasattr(f[k], "keys"):
                    log.info("    /%s keys: %s", k, list(f[k].keys()))

        arr = f["/dataset1/data1/data"][...]
        what = f["/dataset1/data1/what"].attrs
        gain     = float(what.get("gain", 1.0))
        offset   = float(what.get("offset", 0.0))
        nodata   = float(what.get("nodata", float("nan")))
        undetect = float(what.get("undetect", 0.0))

        nodata_mask   = _value_mask(arr, nodata)
        undetect_mask = _value_mask(arr, undetect)
        valid_mask    = ~(nodata_mask | undetect_mask)

        if debug:
            log.info("--- /dataset1/data1/what attrs: %s", dict(what))
            log.info("    Raw arr: shape=%s dtype=%s", arr.shape, arr.dtype)
            log.info("    gain=%s offset=%s nodata=%s undetect=%s", gain, offset, nodata, undetect)
            log.info("    Pixel-Counts: total=%d nodata=%d undetect=%d valid=%d",
                     arr.size, int(nodata_mask.sum()), int(undetect_mask.sum()), int(valid_mask.sum()))

            valid = arr[valid_mask]
            if valid.size > 0:
                log.info("    Valid raw: min=%.2f max=%.2f mean=%.2f",
                         float(valid.min()), float(valid.max()), float(valid.mean()))
                for lo, hi in [(0, 10), (10, 30), (30, 50), (50, 80), (80, 101)]:
                    cnt = int(((valid >= lo) & (valid < hi)).sum())
                    if cnt > 0:
                        log.info("    raw bucket [%d, %d): %d pixels", lo, hi, cnt)
            else:
                log.info("    Komplett leerer Frame (alles nodata/undetect — typisch nachts)")

        # gain/offset anwenden, dann Sentinel-Werte normalisieren
        data = arr.astype(np.float32) * gain + offset
        data[nodata_mask]   = np.nan
        data[undetect_mask] = 0.0

        where = f["/where"].attrs
        meta = {
            "LL_lat": float(where["LL_lat"]),
            "LL_lon": float(where["LL_lon"]),
            "UR_lat": float(where["UR_lat"]),
            "UR_lon": float(where["UR_lon"]),
            "xsize": int(where["xsize"]),
            "ysize": int(where["ysize"]),
            "projdef": where.get("projdef", b"").decode("ascii", "replace")
            if isinstance(where.get("projdef"), (bytes, bytearray))
            else str(where.get("projdef", "")),
        }

    return data, meta


def build_pixel_list(
    poh: np.ndarray,
    poh_meta: dict,
    meshs: np.ndarray | None,
    meshs_meta: dict | None,
    threshold: int,
) -> list[dict]:
    """
    Gibt Liste von {lat,lng,poh,meshs} für alle Pixel mit POH >= threshold zurück.
    Nutzt ODIM-Projdef um aus Pixel-Indices echte WGS84-Koordinaten zu rechnen.
    """
    ysize, xsize = poh.shape
    if (poh_meta["xsize"], poh_meta["ysize"]) != (xsize, ysize):
        log.warning(
            "POH grid-size mismatch: meta=%sx%s, array=%sx%s",
            poh_meta["xsize"], poh_meta["ysize"], xsize, ysize,
        )

    # Projdef aus ODIM nutzen (LV03 oder LV95, je nach Datei)
    transformer = Transformer.from_crs(poh_meta["projdef"], "EPSG:4326", always_xy=True)

    # Pixel-Raster in Projdef-Koordinaten rekonstruieren: ODIM gibt Ecken in
    # WGS84, aber wir wollen das native Grid. Linearer Ansatz — LL/UR in WGS84
    # transformieren wir zurück in native CRS und bauen ein äquidistantes Grid.
    to_native = Transformer.from_crs("EPSG:4326", poh_meta["projdef"], always_xy=True)
    ll_e, ll_n = to_native.transform(poh_meta["LL_lon"], poh_meta["LL_lat"])
    ur_e, ur_n = to_native.transform(poh_meta["UR_lon"], poh_meta["UR_lat"])
    dx = (ur_e - ll_e) / xsize
    dy = (ur_n - ll_n) / ysize

    # MeteoSchweiz POH/MESHS Skalierung (verifiziert via DEBUG_PARSE 2026-04-24):
    #   POH:   Float 0.0-1.0    (ratio, NICHT %)        → ×100 für Prozent
    #   MESHS: Float in mm      (z.B. 25 = 2.5 cm Korn) → /10 für cm
    # Threshold ist in % (z.B. 10 = 10 %), darum hier Vergleich gegen poh*100.
    poh_pct = poh * 100.0

    # ODIM-Raster-Konvention: Zeile 0 ist oberste Reihe (UR-Richtung), Spalte 0 links
    rows, cols = np.where(poh_pct >= threshold)
    pixels: list[dict] = []
    for r_idx, c_idx in zip(rows.tolist(), cols.tolist()):
        p_val = float(poh_pct[r_idx, c_idx])
        if np.isnan(p_val):
            continue

        # Pixel-Mittelpunkt in native CRS
        east = ll_e + (c_idx + 0.5) * dx
        north = ur_n - (r_idx + 0.5) * dy  # Oben-Links-Origin -> Y invertiert
        lon, lat = transformer.transform(east, north)

        meshs_val = None
        if meshs is not None and meshs_meta is not None:
            if meshs.shape == poh.shape:
                mv_mm = meshs[r_idx, c_idx]
                if not np.isnan(mv_mm) and mv_mm >= 20.0:    # ≥ 2 cm = 20 mm
                    meshs_val = round(float(mv_mm) / 10.0, 1)  # mm → cm

        pixels.append(
            {
                "lat": round(lat, 5),
                "lng": round(lon, 5),
                "poh": int(round(p_val)),    # 0-100 %
                "meshs": meshs_val,          # cm (oder None bei < 2 cm)
            }
        )
    return pixels


def ingest(ingest_url: str, token: str, ts: datetime, pixels: list[dict]) -> bool:
    payload = {
        "frame_timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "pixels": pixels,
    }
    try:
        r = requests.post(
            ingest_url,
            json=payload,
            headers={"X-Ingest-Token": token},
            timeout=60,
        )
    except requests.RequestException as e:
        log.error("ingest POST failed: %s", e)
        return False

    if r.status_code >= 400:
        log.error("ingest HTTP %d: %s", r.status_code, r.text[:500])
        return False

    result = r.json() if r.content else {}
    log.info(
        "ingested %s -> status=%s received=%s inserted=%s",
        ts, result.get("status"), result.get("pixels_received"), result.get("pixels_inserted"),
    )
    return True


def is_in_season(now: datetime | None = None) -> bool:
    now = now or datetime.now(timezone.utc)
    return 4 <= now.month <= 9


def main() -> int:
    ingest_url = require_env("INGEST_URL")
    token = require_env("INGEST_TOKEN")
    threshold = int(os.environ.get("POH_THRESHOLD", "10"))
    lookback = int(os.environ.get("LOOKBACK_MINUTES", "30"))
    max_frames_env = os.environ.get("MAX_FRAMES", "").strip()
    max_frames = int(max_frames_env) if max_frames_env else None
    skip_season_check = os.environ.get("SKIP_SEASON_CHECK", "").strip().lower() in {"1", "true", "yes"}

    if not skip_season_check and not is_in_season():
        log.info("Outside hail season (April-September) — nothing to fetch.")
        return 0

    log.info(
        "Discovering frames (lookback=%d min, POH threshold=%d, max_frames=%s)",
        lookback, threshold, max_frames if max_frames else "unlimited",
    )
    frames = discover_frames(lookback)
    log.info("Found %d candidate frames in window", len(frames))

    any_error = False
    processed = 0
    with tempfile.TemporaryDirectory(prefix="hail_") as tmpdir:
        tmp = Path(tmpdir)
        for frame in frames:
            if frame_already_ingested(ingest_url, frame.timestamp):
                log.info("Skip %s (already ingested)", frame.timestamp)
                continue

            try:
                poh_file = tmp / f"poh_{frame.timestamp:%Y%m%d%H%M}.h5"
                download(frame.poh_url, poh_file)
                poh, poh_meta = read_odim_raster(poh_file)

                meshs, meshs_meta = None, None
                if frame.meshs_url:
                    meshs_file = tmp / f"meshs_{frame.timestamp:%Y%m%d%H%M}.h5"
                    download(frame.meshs_url, meshs_file)
                    meshs, meshs_meta = read_odim_raster(meshs_file)

                pixels = build_pixel_list(poh, poh_meta, meshs, meshs_meta, threshold)
                log.info("%s: %d pixels >= POH %d", frame.timestamp, len(pixels), threshold)

                if not ingest(ingest_url, token, frame.timestamp, pixels):
                    any_error = True

                # Cleanup, behalten nur wenig Plattenplatz pro Run
                poh_file.unlink(missing_ok=True)
                if frame.meshs_url:
                    (tmp / f"meshs_{frame.timestamp:%Y%m%d%H%M}.h5").unlink(missing_ok=True)

                processed += 1
                if max_frames is not None and processed >= max_frames:
                    log.info(
                        "Reached MAX_FRAMES=%d (of %d in window). Re-run to continue.",
                        max_frames, len(frames),
                    )
                    break

            except Exception as e:
                log.exception("Processing frame %s failed: %s", frame.timestamp, e)
                any_error = True

    log.info("Processed %d frames", processed)
    return 1 if any_error else 0


if __name__ == "__main__":
    sys.exit(main())
