#!/usr/bin/env python3
"""
IBAMA Auto-Updater
Downloads fresh ZIPs from IBAMA open data portal and rebuilds the SQLite database.
Designed to run as a scheduled task or manually.
"""
import os, sys, subprocess, shutil, time
from datetime import datetime

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


def download_with_curl(url, dest):
    """Download using curl (handles Cloudflare better than Python urllib)."""
    tmp_path = dest + ".tmp"
    cmd = [
        "curl", "-fSL",
        "--retry", "3",
        "--retry-delay", "5",
        "--max-time", "600",
        "--connect-timeout", "30",
        "-H", "User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "-H", "Accept: */*",
        "-H", "Accept-Language: pt-BR,pt;q=0.9",
        "-o", tmp_path,
        url,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0 and os.path.exists(tmp_path):
        size = os.path.getsize(tmp_path)
        if size > 1000:  # sanity check - file should be > 1KB
            os.rename(tmp_path, dest)
            return True
        else:
            print(f"  Arquivo muito pequeno ({size}B), provavelmente erro")
    if os.path.exists(tmp_path):
        os.remove(tmp_path)
    if result.stderr:
        print(f"  curl stderr: {result.stderr.strip()[-200:]}")
    return False


def download_with_wget(url, dest):
    """Download using wget as fallback."""
    tmp_path = dest + ".tmp"
    cmd = [
        "wget", "-q",
        "--tries=3",
        "--timeout=600",
        "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "-O", tmp_path,
        url,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0 and os.path.exists(tmp_path):
        size = os.path.getsize(tmp_path)
        if size > 1000:
            os.rename(tmp_path, dest)
            return True
    if os.path.exists(tmp_path):
        os.remove(tmp_path)
    return False


def download_with_python(url, dest):
    """Download using Python urllib as last resort."""
    from urllib.request import Request, urlopen
    from urllib.error import URLError

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "identity",
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
                    print(f"\r  {downloaded / 1024 / 1024:.1f}MB / {total / 1024 / 1024:.1f}MB ({pct}%)", end="", flush=True)
        print()
        os.rename(tmp_path, dest)
        return True
    except (URLError, OSError) as e:
        print(f"  python urllib: {e}")
        if os.path.exists(dest + ".tmp"):
            os.remove(dest + ".tmp")
        return False


def download_file(url, dest):
    """Download a file trying multiple methods."""
    print(f"  Downloading {os.path.basename(dest)}...")

    # Method 1: curl (best for Cloudflare-protected sites)
    if shutil.which("curl"):
        print("  Tentando curl...", flush=True)
        if download_with_curl(url, dest):
            print(f"  OK (curl): {os.path.getsize(dest) / 1024 / 1024:.1f}MB")
            return True
        print("  curl falhou")

    # Method 2: wget
    if shutil.which("wget"):
        print("  Tentando wget...", flush=True)
        if download_with_wget(url, dest):
            print(f"  OK (wget): {os.path.getsize(dest) / 1024 / 1024:.1f}MB")
            return True
        print("  wget falhou")

    # Method 3: Python urllib
    print("  Tentando python urllib...", flush=True)
    if download_with_python(url, dest):
        print(f"  OK (python): {os.path.getsize(dest) / 1024 / 1024:.1f}MB")
        return True

    print(f"  ERRO: Todos os metodos de download falharam para {os.path.basename(dest)}")
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
