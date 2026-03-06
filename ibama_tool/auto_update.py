#!/usr/bin/env python3
"""
IBAMA Auto-Updater
Downloads fresh ZIPs from IBAMA open data portal and rebuilds the SQLite database.
Designed to run as a scheduled task or manually.
"""
import os, sys, subprocess
from datetime import datetime
from urllib.request import urlretrieve, Request, urlopen
from urllib.error import URLError

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.dirname(_SCRIPT_DIR)  # ZIPs live one level up

URLS = {
    "auto_infracao_json.zip": "https://dadosabertos.ibama.gov.br/dados/SIFISC/auto_infracao/auto_infracao/auto_infracao_json.zip",
    "termo_embargo_xml.zip": "https://dadosabertos.ibama.gov.br/dados/SIFISC/termo_embargo/termo_embargo/termo_embargo_xml.zip",
}


def check_update_needed(max_age_hours=24):
    """Check if files are missing or older than max_age_hours."""
    for fname in URLS:
        fpath = os.path.join(DATA_DIR, fname)
        if not os.path.exists(fpath):
            return True
        age_hours = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(fpath))).total_seconds() / 3600
        if age_hours > max_age_hours:
            return True
    return False


def download_file(url, dest):
    """Download a file with progress reporting."""
    print(f"  Downloading {os.path.basename(dest)}...")
    try:
        req = Request(url, headers={"User-Agent": "IBAMA-Monitor/1.0"})
        response = urlopen(req, timeout=300)
        total = int(response.headers.get("Content-Length", 0))

        tmp_path = dest + ".tmp"
        downloaded = 0
        with open(tmp_path, "wb") as f:
            while True:
                chunk = response.read(1024 * 256)  # 256KB chunks
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded * 100 // total
                    print(f"\r  {downloaded / 1024 / 1024:.1f}MB / {total / 1024 / 1024:.1f}MB ({pct}%)", end="", flush=True)

        print()
        os.rename(tmp_path, dest)
        print(f"  OK: {os.path.getsize(dest) / 1024 / 1024:.1f}MB")
        return True
    except (URLError, OSError) as e:
        print(f"  ERRO download: {e}")
        if os.path.exists(dest + ".tmp"):
            os.remove(dest + ".tmp")
        return False


def rebuild_db():
    """Rebuild SQLite database from downloaded files."""
    print(f"[{datetime.now():%H:%M:%S}] Rebuilding database...")
    result = subprocess.run(
        [sys.executable, os.path.join(_SCRIPT_DIR, "build_db.py")],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(f"ERRO: {result.stderr}")
    return result.returncode == 0


if __name__ == "__main__":
    print(f"=== IBAMA Auto-Update ===")
    print(f"Data dir: {DATA_DIR}")
    print()

    if not check_update_needed():
        print("Arquivos atualizados (< 24h). Nada a fazer.")
        sys.exit(0)

    all_ok = True
    for fname, url in URLS.items():
        fpath = os.path.join(DATA_DIR, fname)
        if os.path.exists(fpath):
            age = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(fpath))).total_seconds() / 3600
            if age <= 24:
                print(f"  {fname}: OK ({age:.0f}h)")
                continue

        if not download_file(url, fpath):
            all_ok = False

    if all(os.path.exists(os.path.join(DATA_DIR, f)) for f in URLS):
        rebuild_db()
    else:
        print("\nArquivos faltando. Impossivel reconstruir DB.")
        sys.exit(1)
