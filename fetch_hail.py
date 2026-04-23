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
    r"^(?P<code>BZC|MZC)(?P<yy>\d{2})(?P<jjj>\d{3})(?P<hhmm>\d{4})"
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
    """Fragt STAC nach Items im Lookback-Fenster, gruppiert nach 5-Min-Slot."""
    since = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
    # STAC unterstützt datetime-Filter (ISO8601 Intervall)
    params = {
        "datetime": f"{since.isoformat().replace('+00:00', 'Z')}/..",
        "limit": 100,
    }
    r = requests.get(STAC_ITEMS_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    slots: dict[datetime, FrameAssets] = {}
    for feat in data.get("features", []):
        for asset_id, asset in (feat.get("assets") or {}).items():
            href = asset.get("href", "")
            if not href.lower().endswith(".h5"):
                continue
            fname = href.rsplit("/", 1)[-1]
            ts = parse_fname_timestamp(fname)
            if ts is None:
                continue
            slot = slots.setdefault(ts, FrameAssets(timestamp=ts))
            if fname.startswith("BZC"):
                slot.poh_url = href
            elif fname.startswith("MZC"):
                slot.meshs_url = href

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


def read_odim_raster(h5_path: Path) -> tuple[np.ndarray, dict]:
    """
    Liest ODIM-HDF5: data2d array + geo-Metadaten (LL/UR corner WGS84).

    Returns (data_array, meta) mit meta-keys: LL_lat, LL_lon, UR_lat, UR_lon,
    xsize, ysize. Werte sind bereits skaliert (gain/offset angewandt),
    nodata/undetect als np.nan.
    """
    with h5py.File(h5_path, "r") as f:
        # ODIM 2.3 Composite: /dataset1/data1/data + /dataset1/data1/what
        arr = f["/dataset1/data1/data"][...]
        what = f["/dataset1/data1/what"].attrs
        gain = float(what.get("gain", 1.0))
        offset = float(what.get("offset", 0.0))
        nodata = float(what.get("nodata", 255))
        undetect = float(what.get("undetect", 0))

        data = arr.astype(np.float32) * gain + offset
        data[arr == nodata] = np.nan
        data[arr == undetect] = 0.0

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

    # ODIM-Raster-Konvention: Zeile 0 ist oberste Reihe (UR-Richtung), Spalte 0 links
    rows, cols = np.where(poh >= threshold)
    pixels: list[dict] = []
    for r_idx, c_idx in zip(rows.tolist(), cols.tolist()):
        poh_val = float(poh[r_idx, c_idx])
        if np.isnan(poh_val):
            continue

        # Pixel-Mittelpunkt in native CRS
        east = ll_e + (c_idx + 0.5) * dx
        north = ur_n - (r_idx + 0.5) * dy  # Oben-Links-Origin -> Y invertiert
        lon, lat = transformer.transform(east, north)

        meshs_val = None
        if meshs is not None and meshs_meta is not None:
            # Annahme: gleiches Grid wie POH (beides CH-Composite). Wenn nicht,
            # ist der Index ungültig — dann None.
            if meshs.shape == poh.shape:
                mv = meshs[r_idx, c_idx]
                if not np.isnan(mv) and mv >= 2.0:
                    meshs_val = round(float(mv), 1)

        pixels.append(
            {
                "lat": round(lat, 5),
                "lng": round(lon, 5),
                "poh": int(round(poh_val)),
                "meshs": meshs_val,
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

    if not is_in_season():
        log.info("Outside hail season (April-September) — nothing to fetch.")
        return 0

    log.info("Discovering frames (lookback=%d min, POH threshold=%d)", lookback, threshold)
    frames = discover_frames(lookback)
    log.info("Found %d candidate frames", len(frames))

    any_error = False
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

            except Exception as e:
                log.exception("Processing frame %s failed: %s", frame.timestamp, e)
                any_error = True

    return 1 if any_error else 0


if __name__ == "__main__":
    sys.exit(main())
