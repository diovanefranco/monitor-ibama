#!/usr/bin/env python3
"""
IBAMA Auto-Updater
Downloads fresh ZIPs from GitHub Releases (mirror) or IBAMA open data portal,
then rebuilds the SQLite database.
"""
import os, sys, subprocess, shutil
from datetime import datetime

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.dirname(_SCRIPT_DIR)  # ZIPs live one level up

# GitHub Releases mirror (works from cloud servers like Render)
GH_RELEASE = "https://github.com/diovanefranco/monitor-ibama/releases/download/data-2026-03-06"

# Primary source URLs (IBAMA - may block non-BR IPs via Cloudflare)
IBAMA_URLS = {
    "auto_infracao_json.zip": "https://dadosabertos.ibama.gov.br/dados/SIFISC/auto_infracao/auto_infracao/auto_infracao_json.zip",
    "termo_embargo_xml.zip": "https://dadosabertos.ibama.gov.br/dados/SIFISC/termo_embargo/termo_embargo/termo_embargo_xml.zip",
}

FILES = list(IBAMA_URLS.keys())


def check_update_needed(max_age_hours=24):
    """Check if files are missing or older than max_age_hours."""
    for fname in FILES:
        fpath = os.path.join(DATA_DIR, fname)
        if not os.path.exists(fpath):
            return True
        age_hours = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(fpath))).total_seconds() / 3600
        if age_hours > max_age_hours:
            return True
    return False


def download_with_curl(url, dest):
    """Download using curl."""
    tmp_path = dest + ".tmp"
    cmd = [
        "curl", "-fSL",
        "--retry", "3",
        "--retry-delay", "5",
        "--max-time", "600",
        "--connect-timeout", "30",
        "-o", tmp_path,
        url,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0 and os.path.exists(tmp_path):
        size = os.path.getsize(tmp_path)
        if size > 10000:  # sanity check
            os.rename(tmp_path, dest)
            return True
    if os.path.exists(tmp_path):
        os.remove(tmp_path)
    return False


def download_with_python(url, dest):
    """Download using Python urllib."""
    from urllib.request import Request, urlopen
    from urllib.error import URLError
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "*/*",
        }
        req = Request(url, headers=headers)
        response = urlopen(req, timeout=600)
        total = int(response.headers.get("Content-Length", 0))
        tmp_path = dest + ".tmp"
        downloaded = 0
        with open(tmp_path, "wb") as f:
            while True:
                chunk = response.read(1024 * 256)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded * 100 // total
                    print(f"\r    {downloaded / 1024 / 1024:.1f}MB / {total / 1024 / 1024:.1f}MB ({pct}%)", end="", flush=True)
        print()
        if os.path.exists(tmp_path) and os.path.getsize(tmp_path) > 10000:
            os.rename(tmp_path, dest)
            return True
    except (URLError, OSError) as e:
        print(f"    urllib error: {e}")
    if os.path.exists(dest + ".tmp"):
        os.remove(dest + ".tmp")
    return False


def download_file(fname, dest):
    """Download a file trying GitHub Releases first, then IBAMA directly."""
    print(f"  Downloading {fname}...")

    # Source 1: GitHub Releases mirror (works globally)
    gh_url = f"{GH_RELEASE}/{fname}"
    print(f"    Fonte: GitHub Releases...", flush=True)
    if shutil.which("curl"):
        if download_with_curl(gh_url, dest):
            print(f"    OK (GitHub/curl): {os.path.getsize(dest) / 1024 / 1024:.1f}MB")
            return True
    if download_with_python(gh_url, dest):
        print(f"    OK (GitHub/python): {os.path.getsize(dest) / 1024 / 1024:.1f}MB")
        return True
    print(f"    GitHub falhou, tentando IBAMA direto...")

    # Source 2: IBAMA (may fail from non-BR IPs due to Cloudflare)
    ibama_url = IBAMA_URLS[fname]
    print(f"    Fonte: IBAMA direta...", flush=True)
    if shutil.which("curl"):
        if download_with_curl(ibama_url, dest):
            print(f"    OK (IBAMA/curl): {os.path.getsize(dest) / 1024 / 1024:.1f}MB")
            return True
    if download_with_python(ibama_url, dest):
        print(f"    OK (IBAMA/python): {os.path.getsize(dest) / 1024 / 1024:.1f}MB")
        return True

    print(f"  ERRO: Todos os metodos falharam para {fname}")
    return False


def rebuild_db():
    """Rebuild SQLite database from downloaded files."""
    print(f"\n[{datetime.now():%H:%M:%S}] Rebuilding database...")
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

    for fname in FILES:
        fpath = os.path.join(DATA_DIR, fname)
        if os.path.exists(fpath):
            age = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(fpath))).total_seconds() / 3600
            if age <= 24:
                print(f"  {fname}: OK ({age:.0f}h)")
                continue
        download_file(fname, fpath)

    if all(os.path.exists(os.path.join(DATA_DIR, f)) for f in FILES):
        rebuild_db()
    else:
        print("\nArquivos faltando. Impossivel reconstruir DB.")
        print("O app vai iniciar sem dados IBAMA e tentar self-heal em background.")
        sys.exit(0)  # Don't break the build - app will self-heal at runtime
