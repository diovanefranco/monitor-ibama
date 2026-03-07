#!/usr/bin/env python3
"""
SEMA DB Verification Script - runs at end of build to ensure sema.db is valid.
If sema.db is missing, empty, or has wrong tables, downloads from GitHub Releases.
This is a safety net for the build process.
"""
import os, sys, sqlite3, subprocess

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SEMA_DB_PATH = os.path.join(_SCRIPT_DIR, "sema.db")
GH_URL = "https://github.com/diovanefranco/monitor-ibama/releases/download/data-2026-03-06/sema.db"

REQUIRED_TABLES = [
    "sema_autos_infracao",
    "sema_outros_termos",
    "sema_embargos",
    "sema_desembargos",
]


def verify_sema_db():
    """Check if sema.db exists and has the required tables with data."""
    print("=== SEMA DB Verification ===")

    if not os.path.exists(SEMA_DB_PATH):
        print(f"  sema.db NOT FOUND at {SEMA_DB_PATH}")
        return False

    size_mb = os.path.getsize(SEMA_DB_PATH) / 1024 / 1024
    if size_mb < 1:
        print(f"  sema.db too small ({size_mb:.2f}MB) - likely empty/corrupt")
        return False

    try:
        conn = sqlite3.connect(SEMA_DB_PATH)
        conn.execute("PRAGMA query_only=ON")

        # Check all required tables exist
        for table in REQUIRED_TABLES:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"  {table}: {count} rows")
                if table == "sema_autos_infracao" and count == 0:
                    conn.close()
                    print("  FAILED: main table is empty")
                    return False
            except Exception as e:
                conn.close()
                print(f"  FAILED: table {table} missing ({e})")
                return False

        conn.close()
        print(f"  sema.db OK ({size_mb:.1f}MB)")
        return True
    except Exception as e:
        print(f"  FAILED: {e}")
        return False


def download_from_github():
    """Download sema.db from GitHub Releases."""
    print(f"  Downloading sema.db from GitHub Releases...")

    # Remove bad file first
    if os.path.exists(SEMA_DB_PATH):
        os.remove(SEMA_DB_PATH)

    tmp = SEMA_DB_PATH + ".tmp"
    result = subprocess.run(
        ["curl", "-fSL", "--retry", "3", "--retry-delay", "5",
         "--max-time", "300", "--connect-timeout", "30", "-o", tmp, GH_URL],
        capture_output=True, text=True
    )

    if result.returncode == 0 and os.path.exists(tmp) and os.path.getsize(tmp) > 1_000_000:
        os.rename(tmp, SEMA_DB_PATH)
        size_mb = os.path.getsize(SEMA_DB_PATH) / 1024 / 1024
        print(f"  Downloaded OK: {size_mb:.1f}MB")
        return True

    if os.path.exists(tmp):
        os.remove(tmp)
    print(f"  Download FAILED (curl rc={result.returncode})")
    return False


if __name__ == "__main__":
    if verify_sema_db():
        print("=== SEMA DB: PASSED ===")
        sys.exit(0)

    print("\nSEMA DB verification failed. Attempting GitHub download...")
    if download_from_github():
        # Verify the downloaded file
        if verify_sema_db():
            print("=== SEMA DB: PASSED (after GitHub download) ===")
            sys.exit(0)
        else:
            print("=== SEMA DB: FAILED (downloaded file is also invalid) ===")
    else:
        print("=== SEMA DB: FAILED (could not download) ===")

    # Don't break the build - app will work without SEMA
    print("Continuing without SEMA data...")
    sys.exit(0)
