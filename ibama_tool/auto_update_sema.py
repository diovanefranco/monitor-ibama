#!/usr/bin/env python3
"""
SEMA-MT Auto-Updater
Downloads GeoJSON from SEMA-MT GeoServer (WFS) with pagination,
then rebuilds the SQLite database.

Resilience: If SEMA server is down, keeps previous day's data intact.
Runs independently from IBAMA module (separate DB, separate files).
"""
import os, sys, subprocess, json, time, ssl
from datetime import datetime
from urllib.request import Request, urlopen
from urllib.error import URLError

# SEMA-MT GeoServer uses a self-signed certificate chain
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = _SCRIPT_DIR  # JSONs stored alongside scripts

# GeoServer WFS base URL with public authkey
WFS_BASE = "https://geo.sema.mt.gov.br/geoserver/Geoportal/ows"
AUTH_KEY = "541085de-9a2e-454e-bdba-eb3d57a2f492"

# Layers to download
# sort_by: required for layers that lack primary keys (SQL views)
LAYERS = {
    "sema_ai_siga.json": {
        "layer": "Geoportal:AUTOS_DE_INFRACAO_SIGA_PONTO",
        "sort_by": None,  # Has natural order
    },
    "sema_ai_legado.json": {
        "layer": "Geoportal:MVW_TIT_AUTUACAO",
        "sort_by": "OBJECTID",  # SQL view needs explicit sort for pagination
    },
    "sema_embargo_siga.json": {
        "layer": "Geoportal:AREA_EMBARGADA_SIGA_PONTO",
        "sort_by": None,
    },
    "sema_embargo_legado.json": {
        "layer": "Geoportal:AREAS_EMBARGADAS_SEMA",
        "sort_by": None,
    },
}

PAGE_SIZE = 5000


def check_update_needed(max_age_hours=24):
    """Check if any data file is missing or older than max_age_hours."""
    for fname in LAYERS:
        fpath = os.path.join(DATA_DIR, fname)
        if not os.path.exists(fpath):
            return True
        age_hours = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(fpath))).total_seconds() / 3600
        if age_hours > max_age_hours:
            return True
    return False


def _fetch_json(url, headers):
    """Fetch JSON from URL, trying urllib then curl. Returns parsed dict or None."""
    # Method 1: Python urllib with SSL workaround
    try:
        req = Request(url, headers=headers)
        response = urlopen(req, timeout=300, context=_SSL_CTX)
        raw = response.read()
        # Check if response is XML error instead of JSON
        if raw.startswith(b'<?xml') or raw.startswith(b'<Service'):
            print(f"    Server returned XML error")
            return None
        return json.loads(raw)
    except (URLError, OSError, json.JSONDecodeError) as e:
        pass

    # Method 2: curl -k (for environments where Python SSL fails)
    try:
        result = subprocess.run(
            ["curl", "-fsSL", "-k", "--max-time", "300", url],
            capture_output=True, text=True
        )
        if result.returncode == 0 and result.stdout and not result.stdout.startswith('<?xml'):
            return json.loads(result.stdout)
    except Exception:
        pass

    return None


def download_wfs_layer(layer_name, output_path, sort_by=None):
    """Download all features from a WFS layer with pagination.
    If sort_by is specified, adds sortBy param (needed for SQL view layers).
    Returns feature count. On failure, does NOT touch existing file."""
    all_features = []
    start_index = 0
    headers = {
        "User-Agent": "Mozilla/5.0 (Monitor-SEMA/1.0)",
        "Accept": "application/json",
    }

    print(f"  Downloading layer: {layer_name}")
    while True:
        url = (
            f"{WFS_BASE}?service=WFS&version=1.0.0"
            f"&authkey={AUTH_KEY}"
            f"&request=GetFeature"
            f"&typeName={layer_name}"
            f"&outputFormat=application/json"
            f"&maxFeatures={PAGE_SIZE}"
            f"&startIndex={start_index}"
        )
        if sort_by:
            url += f"&sortBy={sort_by}"

        data = None
        for attempt in range(3):
            data = _fetch_json(url, headers)
            if data:
                break
            print(f"    Attempt {attempt+1}/3 failed for page {start_index // PAGE_SIZE + 1}")
            if attempt < 2:
                time.sleep(5 * (attempt + 1))

        if data is None:
            print(f"  ERRO: Failed to download {layer_name} (server may be down)")
            print(f"  Keeping previous data if available.")
            return 0

        features = data.get("features", [])
        if not features:
            break

        all_features.extend(features)
        count = len(features)
        print(f"    Page {start_index // PAGE_SIZE + 1}: {count} features (total: {len(all_features)})")

        if count < PAGE_SIZE:
            break

        start_index += PAGE_SIZE
        time.sleep(1)

    if not all_features:
        print(f"  AVISO: 0 features returned. Keeping previous data.")
        return 0

    # Write to temp file, then atomic rename (never corrupts existing file)
    geojson = {
        "type": "FeatureCollection",
        "totalFeatures": len(all_features),
        "features": all_features,
    }

    tmp_path = output_path + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(geojson, f, ensure_ascii=False)

        if os.path.getsize(tmp_path) > 1000:  # sanity check (>1KB)
            os.rename(tmp_path, output_path)
            size_mb = os.path.getsize(output_path) / 1024 / 1024
            print(f"    OK: {len(all_features)} features ({size_mb:.1f}MB)")
            return len(all_features)
        else:
            print(f"  AVISO: File too small, keeping previous data.")
    except Exception as e:
        print(f"  ERRO writing file: {e}")

    if os.path.exists(tmp_path):
        os.remove(tmp_path)
    return 0


def rebuild_db():
    """Rebuild SQLite database from downloaded GeoJSON files."""
    print(f"\n[{datetime.now():%H:%M:%S}] Rebuilding SEMA database...")
    result = subprocess.run(
        [sys.executable, os.path.join(_SCRIPT_DIR, "build_db_sema.py")],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(f"ERRO: {result.stderr}")
    return result.returncode == 0


if __name__ == "__main__":
    print(f"=== SEMA-MT Auto-Update ===")
    print(f"Data dir: {DATA_DIR}")
    print()

    if not check_update_needed():
        print("Arquivos atualizados (< 24h). Nada a fazer.")
        sys.exit(0)

    print("Downloading SEMA-MT data from GeoServer WFS...\n")
    any_updated = False

    for fname, cfg in LAYERS.items():
        fpath = os.path.join(DATA_DIR, fname)

        # Skip if file is recent enough
        if os.path.exists(fpath):
            age = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(fpath))).total_seconds() / 3600
            if age <= 24:
                print(f"  {fname}: OK ({age:.0f}h old)")
                continue

        count = download_wfs_layer(cfg["layer"], fpath, sort_by=cfg.get("sort_by"))
        if count > 0:
            any_updated = True

    # Only rebuild if we have at least some data files
    has_any_data = any(os.path.exists(os.path.join(DATA_DIR, f)) for f in LAYERS)
    if has_any_data:
        rebuild_db()
    else:
        print("\nNenhum arquivo de dados encontrado. Impossivel construir DB.")
        sys.exit(1)
