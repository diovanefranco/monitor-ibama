#!/usr/bin/env python3
"""
SEMA-MT Auto-Updater
Downloads GeoJSON from SEMA-MT GeoServer (WFS) with pagination,
then rebuilds the SQLite database.

Resilience: If SEMA server is down, keeps previous day's data intact.
Runs independently from IBAMA module (separate DB, separate files).

Robust retry: warmup probe + 5 attempts per page with exponential backoff
+ up to 2 full global retries if all layers fail.
"""
import os, sys, subprocess, json, time, ssl, shutil
from datetime import datetime
from urllib.request import Request, urlopen
from urllib.error import URLError

# Ensure unbuffered output for build logs
os.environ.setdefault('PYTHONUNBUFFERED', '1')
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None

# SEMA-MT GeoServer uses a self-signed certificate chain
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(_SCRIPT_DIR, "sema_data")
os.makedirs(DATA_DIR, exist_ok=True)

# GitHub Releases mirror for pre-built sema.db (fallback when GeoServer is unreachable)
GH_RELEASE = "https://github.com/diovanefranco/monitor-ibama/releases/download/data-2026-03-06"
SEMA_DB_PATH = os.path.join(_SCRIPT_DIR, "sema.db")

# GeoServer WFS base URL with public authkey
WFS_BASE = "https://geo.sema.mt.gov.br/geoserver/Geoportal/ows"
AUTH_KEY = "541085de-9a2e-454e-bdba-eb3d57a2f492"

# ALL fiscalização layers (21 layers total)
LAYERS = {
    # ── AUTOS DE INFRAÇÃO ──
    "ai_siga_ponto.json":      {"layer": "Geoportal:AUTOS_DE_INFRACAO_SIGA_PONTO", "sort_by": None},
    "ai_siga_poligono.json":   {"layer": "Geoportal:AUTOS_DE_INFRACAO_SIGA_POLIGONO", "sort_by": None},
    "ai_legado.json":          {"layer": "Geoportal:MVW_TIT_AUTUACAO", "sort_by": "OBJECTID"},
    "ai_descentralizado.json": {"layer": "Geoportal:TDAD_FISCALIZACAO_AUTO_DE_INFRACAO", "sort_by": None},
    # ── AUTO DE INSPEÇÃO ──
    "inspecao_siga.json":             {"layer": "Geoportal:AUTOS_TERMOS_AUTO_INSPECAO", "sort_by": None},
    "inspecao_descentralizado.json":  {"layer": "Geoportal:TDAD_FISCALIZACAO_AUTO_DE_INSPECAO", "sort_by": None},
    # ── NOTIFICAÇÃO ──
    "notificacao_siga.json":             {"layer": "Geoportal:AUTOS_TERMOS_NOTIFICACAO", "sort_by": None},
    "notificacao_descentralizado.json":  {"layer": "Geoportal:TDAD_FISCALIZACAO_NOTIFICACAO", "sort_by": None},
    # ── TERMOS ──
    "termo_apreensao.json":   {"layer": "Geoportal:AUTOS_TERMOS_TERMO_APREENSAO", "sort_by": None},
    "termo_deposito.json":    {"layer": "Geoportal:AUTOS_TERMOS_TERMO_DEPOSITO", "sort_by": None},
    "termo_destruicao.json":  {"layer": "Geoportal:AUTOS_TERMOS_TERMO_DEST_INUT", "sort_by": None},
    "termo_soltura.json":     {"layer": "Geoportal:AUTOS_TERMOS_TERMO_SOLTURA", "sort_by": None},
    # ── EMBARGOS ──
    "embargo_siga_ponto.json":      {"layer": "Geoportal:AREA_EMBARGADA_SIGA_PONTO", "sort_by": None},
    "embargo_siga_poligono.json":   {"layer": "Geoportal:AREA_EMBARGADA_SIGA_POLIGONO", "sort_by": None},
    "embargo_legado.json":          {"layer": "Geoportal:AREAS_EMBARGADAS_SEMA", "sort_by": None},
    "embargo_descentralizado.json": {"layer": "Geoportal:TDAD_FISCALIZACAO_TERMO_DE_EMBARGO", "sort_by": None},
    # ── DESEMBARGOS ──
    "desembargo_siga_ponto.json":    {"layer": "Geoportal:AREAS_DESEMBARGADAS_SIGA_PONTO", "sort_by": None},
    "desembargo_siga_poligono.json": {"layer": "Geoportal:AREAS_DESEMBARGADAS_SIGA_POLIGONO", "sort_by": None},
    "desembargo_legado.json":        {"layer": "Geoportal:AREAS_DESEMBARGADAS_SEMA", "sort_by": None},
    # ── FISCALIZAÇÃO GERAL ──
    "fiscalizacao_descentralizado.json": {"layer": "Geoportal:TDAD_FISCALIZACAO", "sort_by": None},
    "las_descentralizado.json":          {"layer": "Geoportal:TDAD_FISCALIZACAO_LAS", "sort_by": None},
}

PAGE_SIZE = 5000
MAX_PAGE_RETRIES = 5       # retries per page request
MAX_GLOBAL_RETRIES = 2     # full retry of all layers if everything fails
WARMUP_RETRIES = 6         # warmup probes before giving up (6 x 15s = 90s max)


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


def warmup_geoserver():
    """Probe the GeoServer to ensure it's responding before heavy downloads.
    Returns True if server is alive, False if unreachable after all retries."""
    probe_url = (
        f"{WFS_BASE}?service=WFS&version=1.0.0"
        f"&authkey={AUTH_KEY}"
        f"&request=GetFeature"
        f"&typeName=Geoportal:AUTOS_DE_INFRACAO_SIGA_PONTO"
        f"&outputFormat=application/json"
        f"&maxFeatures=1"
    )
    headers = {
        "User-Agent": "Mozilla/5.0 (Monitor-SEMA/1.0)",
        "Accept": "application/json",
    }

    for attempt in range(1, WARMUP_RETRIES + 1):
        print(f"  Warmup probe {attempt}/{WARMUP_RETRIES}...", end=" ")
        try:
            req = Request(probe_url, headers=headers)
            resp = urlopen(req, timeout=30, context=_SSL_CTX)
            raw = resp.read()
            if raw and not raw.startswith(b'<?xml'):
                data = json.loads(raw)
                if data.get("features"):
                    print("OK - GeoServer is responding")
                    return True
                else:
                    print("empty response")
            else:
                print("XML error")
        except Exception as e:
            print(f"failed ({type(e).__name__})")

        # Also try curl as fallback
        try:
            result = subprocess.run(
                ["curl", "-fsSL", "-k", "--max-time", "30", probe_url],
                capture_output=True, text=True
            )
            if result.returncode == 0 and result.stdout and '"features"' in result.stdout:
                print(f"  Warmup OK via curl")
                return True
        except Exception:
            pass

        if attempt < WARMUP_RETRIES:
            wait = 15
            print(f"  Waiting {wait}s before next probe...")
            time.sleep(wait)

    print(f"  WARNING: GeoServer did not respond after {WARMUP_RETRIES} probes")
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
        for attempt in range(MAX_PAGE_RETRIES):
            data = _fetch_json(url, headers)
            if data:
                break
            wait = min(10 * (attempt + 1), 60)  # 10s, 20s, 30s, 40s, 50s
            print(f"    Attempt {attempt+1}/{MAX_PAGE_RETRIES} failed for page {start_index // PAGE_SIZE + 1}, waiting {wait}s...")
            if attempt < MAX_PAGE_RETRIES - 1:
                time.sleep(wait)

        if data is None:
            print(f"  ERRO: Failed to download {layer_name} after {MAX_PAGE_RETRIES} attempts")
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


def download_all_layers():
    """Download all SEMA layers. Returns number of successfully downloaded layers."""
    success_count = 0
    for fname, cfg in LAYERS.items():
        fpath = os.path.join(DATA_DIR, fname)

        # Skip if file is recent enough
        if os.path.exists(fpath):
            age = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(fpath))).total_seconds() / 3600
            if age <= 24:
                print(f"  {fname}: OK ({age:.0f}h old)")
                success_count += 1
                continue

        count = download_wfs_layer(cfg["layer"], fpath, sort_by=cfg.get("sort_by"))
        if count > 0:
            success_count += 1

    return success_count


def download_sema_db_fallback():
    """Download pre-built sema.db from GitHub Releases as fallback.
    Used when GeoServer is unreachable (e.g., from overseas servers like Render)."""
    url = f"{GH_RELEASE}/sema.db"
    print(f"\n  Fallback: downloading pre-built sema.db from GitHub Releases...")

    # Try curl first
    if shutil.which("curl"):
        tmp = SEMA_DB_PATH + ".tmp"
        result = subprocess.run(
            ["curl", "-fSL", "--retry", "3", "--retry-delay", "5",
             "--max-time", "300", "--connect-timeout", "30", "-o", tmp, url],
            capture_output=True, text=True
        )
        if result.returncode == 0 and os.path.exists(tmp) and os.path.getsize(tmp) > 100000:
            os.rename(tmp, SEMA_DB_PATH)
            size_mb = os.path.getsize(SEMA_DB_PATH) / 1024 / 1024
            print(f"  OK (GitHub/curl): sema.db {size_mb:.1f}MB")
            return True
        if os.path.exists(tmp):
            os.remove(tmp)

    # Try Python urllib
    try:
        from urllib.request import Request, urlopen as _urlopen
        headers = {"User-Agent": "Mozilla/5.0 (Monitor-SEMA/1.0)"}
        req = Request(url, headers=headers)
        response = _urlopen(req, timeout=300)
        tmp = SEMA_DB_PATH + ".tmp"
        with open(tmp, "wb") as f:
            while True:
                chunk = response.read(1024 * 256)
                if not chunk:
                    break
                f.write(chunk)
        if os.path.exists(tmp) and os.path.getsize(tmp) > 100000:
            os.rename(tmp, SEMA_DB_PATH)
            size_mb = os.path.getsize(SEMA_DB_PATH) / 1024 / 1024
            print(f"  OK (GitHub/python): sema.db {size_mb:.1f}MB")
            return True
        if os.path.exists(tmp):
            os.remove(tmp)
    except Exception as e:
        print(f"  GitHub urllib failed: {e}")

    print(f"  ERRO: fallback download failed")
    return False


if __name__ == "__main__":
    print(f"=== SEMA-MT Auto-Update ===")
    print(f"Data dir: {DATA_DIR}")
    print()

    # Quick check: if sema.db already exists and is recent, skip everything
    if os.path.exists(SEMA_DB_PATH):
        age = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(SEMA_DB_PATH))).total_seconds() / 3600
        if age <= 24:
            print(f"sema.db is recent ({age:.0f}h old). Nada a fazer.")
            sys.exit(0)

    if not check_update_needed():
        print("Arquivos atualizados (< 24h). Nada a fazer.")
        sys.exit(0)

    # Warmup: probe GeoServer before starting heavy downloads
    print("Checking GeoServer availability...")
    server_alive = warmup_geoserver()

    if not server_alive:
        # GeoServer unreachable - try GitHub fallback directly
        print("GeoServer unreachable. Trying GitHub Releases fallback...")
        if download_sema_db_fallback():
            print("\nSEMA DB downloaded from GitHub Releases successfully.")
            sys.exit(0)
        print("GitHub fallback also failed. Will attempt GeoServer downloads anyway...\n")

    # Global retry loop: if all downloads fail, wait and try again
    for global_attempt in range(1, MAX_GLOBAL_RETRIES + 1):
        print(f"\nDownloading SEMA-MT data (attempt {global_attempt}/{MAX_GLOBAL_RETRIES})...\n")

        success = download_all_layers()

        if success > 0:
            print(f"\n{success}/{len(LAYERS)} layers downloaded successfully.")
            break
        else:
            if global_attempt < MAX_GLOBAL_RETRIES:
                wait = 30 * global_attempt
                print(f"\nAll layers failed. Waiting {wait}s before global retry {global_attempt+1}...")
                time.sleep(wait)
            else:
                print(f"\nAll layers failed after {MAX_GLOBAL_RETRIES} global attempts.")

    # Only rebuild if we have at least some data files
    has_any_data = any(
        os.path.exists(os.path.join(DATA_DIR, f)) and os.path.getsize(os.path.join(DATA_DIR, f)) > 0
        for f in LAYERS
    )
    if has_any_data:
        rebuild_db()
    else:
        # Last resort: try GitHub fallback for pre-built DB
        print("\nNenhum arquivo de dados encontrado. Tentando fallback GitHub...")
        if download_sema_db_fallback():
            print("SEMA DB downloaded from GitHub Releases as last resort.")
            sys.exit(0)
        print("O sistema continuara funcionando apenas com dados do IBAMA.")
        sys.exit(0)  # Exit cleanly - don't break the build
