#!/usr/bin/env python3
"""
IBAMA Monitor - Web interface for searching IBAMA enforcement data.
"""
import os, sys, subprocess, threading, time, urllib.request
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, request, jsonify, render_template, session, redirect, url_for

import consulta
import consulta_sema

app = Flask(__name__)

# ── Stats cache ──────────────────────────────────────────────
# Pre-compute counts at startup so /api/stats responds instantly.
_stats_cache = {"ibama": None, "sema": None}
_stats_lock = threading.Lock()


def refresh_stats_cache():
    """Recalculate stats and store in memory (called at startup + after DB updates)."""
    try:
        ibama = consulta.stats()
    except Exception:
        ibama = None
    try:
        sema = consulta_sema.stats()
    except Exception:
        sema = None
    with _stats_lock:
        _stats_cache["ibama"] = ibama
        _stats_cache["sema"] = sema
    print("Stats cache refreshed")
app.secret_key = os.environ.get("SECRET_KEY", "ibama-monitor-secret-key-change-me")

# Login credentials – configure via Environment Variables no Render
APP_USER = os.environ.get("APP_USER", "")
APP_PASS = os.environ.get("APP_PASS", "")
APP_CODE = os.environ.get("APP_CODE", "")


def login_required(f):
    """Decorator to require login on routes."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            if request.path.startswith("/api/"):
                return jsonify({"error": "Nao autorizado"}), 401
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        user = request.form.get("username", "").strip()
        pwd = request.form.get("password", "").strip()
        code = request.form.get("code", "").strip()
        if user == APP_USER and pwd == APP_PASS and code == APP_CODE:
            session["logged_in"] = True
            session.permanent = True
            return redirect(url_for("index"))
        error = "Usuario ou senha incorretos"
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@login_required
def index():
    return render_template("index.html")


@app.route("/health")
def health():
    """Public health check endpoint for Render (no login required)."""
    return jsonify({"status": "ok"}), 200


@app.route("/api/stats")
@login_required
def api_stats():
    try:
        with _stats_lock:
            cached = _stats_cache.get("ibama")
        if cached:
            return jsonify(cached)
        # Fallback: compute live (first request before cache is ready)
        return jsonify(consulta.stats())
    except Exception as e:
        return jsonify({"error": f"IBAMA DB erro: {e}"}), 503


@app.route("/api/autos")
@login_required
def api_autos():
    try:
        params = {
            "nome": request.args.get("nome") or None,
            "cpf_cnpj": request.args.get("cpf_cnpj") or None,
            "uf": request.args.get("uf") or None,
            "municipio": request.args.get("municipio") or None,
            "num_auto": request.args.get("num_auto") or None,
            "num_processo": request.args.get("num_processo") or None,
            "tipo_infracao": request.args.get("tipo_infracao") or None,
            "ano_inicio": request.args.get("ano_inicio") or None,
            "ano_fim": request.args.get("ano_fim") or None,
            "limit": int(request.args.get("limit", 50)),
        }
        return jsonify(consulta.search_autos(**params))
    except Exception as e:
        return jsonify({"error": f"Erro na consulta IBAMA: {e}"}), 500


@app.route("/api/embargos")
@login_required
def api_embargos():
    try:
        params = {
            "nome": request.args.get("nome") or None,
            "cpf_cnpj": request.args.get("cpf_cnpj") or None,
            "uf": request.args.get("uf") or None,
            "municipio": request.args.get("municipio") or None,
            "num_tad": request.args.get("num_tad") or None,
            "num_processo": request.args.get("num_processo") or None,
            "num_auto": request.args.get("num_auto") or None,
            "ativos_only": request.args.get("ativos_only", "").lower() in ("true", "1", "sim"),
            "limit": int(request.args.get("limit", 50)),
        }
        return jsonify(consulta.search_embargos(**params))
    except Exception as e:
        return jsonify({"error": f"Erro na consulta IBAMA: {e}"}), 500


@app.route("/api/texto")
@login_required
def api_texto():
    try:
        q = request.args.get("q", "").strip()
        if not q:
            return jsonify({"error": "Parametro 'q' obrigatorio"})
        tabela = request.args.get("tabela", "autos")
        limit = int(request.args.get("limit", 50))
        return jsonify(consulta.search_texto(q, tabela=tabela, limit=limit))
    except Exception as e:
        return jsonify({"error": f"Erro na consulta IBAMA: {e}"}), 500


@app.route("/api/resumo")
@login_required
def api_resumo():
    nome = request.args.get("nome") or None
    cpf_cnpj = request.args.get("cpf_cnpj") or None
    if not nome and not cpf_cnpj:
        return jsonify({"error": "Informe 'nome' ou 'cpf_cnpj'"})
    try:
        return jsonify(consulta.resumo_autuado(nome=nome, cpf_cnpj=cpf_cnpj))
    except Exception as e:
        return jsonify({"error": f"Erro na consulta IBAMA: {e}"}), 500


# ============================================================
# SEMA-MT API Routes (independent from IBAMA)
# ============================================================

@app.route("/api/sema/stats")
@login_required
def api_sema_stats():
    try:
        with _stats_lock:
            cached = _stats_cache.get("sema")
        if cached:
            return jsonify(cached)
        return jsonify(consulta_sema.stats())
    except Exception as e:
        return jsonify({"error": f"SEMA DB nao disponivel: {e}"}), 503


@app.route("/api/sema/autos")
@login_required
def api_sema_autos():
    try:
        params = {
            "nome": request.args.get("nome") or None,
            "cpf_cnpj": request.args.get("cpf_cnpj") or None,
            "municipio": request.args.get("municipio") or None,
            "num_auto": request.args.get("num_auto") or None,
            "num_processo": request.args.get("num_processo") or None,
            "fonte": request.args.get("fonte") or None,
            "ano_inicio": request.args.get("ano_inicio") or None,
            "ano_fim": request.args.get("ano_fim") or None,
            "limit": int(request.args.get("limit", 50)),
        }
        return jsonify(consulta_sema.search_autos(**params))
    except Exception as e:
        return jsonify({"error": f"SEMA DB nao disponivel: {e}"}), 503


@app.route("/api/sema/embargos")
@login_required
def api_sema_embargos():
    try:
        params = {
            "nome": request.args.get("nome") or None,
            "cpf_cnpj": request.args.get("cpf_cnpj") or None,
            "municipio": request.args.get("municipio") or None,
            "num_embargo": request.args.get("num_embargo") or None,
            "num_processo": request.args.get("num_processo") or None,
            "num_auto": request.args.get("num_auto") or None,
            "fonte": request.args.get("fonte") or None,
            "limit": int(request.args.get("limit", 50)),
        }
        return jsonify(consulta_sema.search_embargos(**params))
    except Exception as e:
        return jsonify({"error": f"SEMA DB nao disponivel: {e}"}), 503


@app.route("/api/sema/termos")
@login_required
def api_sema_termos():
    try:
        params = {
            "nome": request.args.get("nome") or None,
            "cpf_cnpj": request.args.get("cpf_cnpj") or None,
            "municipio": request.args.get("municipio") or None,
            "num_doc": request.args.get("num_doc") or None,
            "num_processo": request.args.get("num_processo") or None,
            "fonte": request.args.get("fonte") or None,
            "ano_inicio": request.args.get("ano_inicio") or None,
            "ano_fim": request.args.get("ano_fim") or None,
            "limit": int(request.args.get("limit", 50)),
        }
        return jsonify(consulta_sema.search_termos(**params))
    except Exception as e:
        return jsonify({"error": f"SEMA DB nao disponivel: {e}"}), 503


@app.route("/api/sema/desembargos")
@login_required
def api_sema_desembargos():
    try:
        params = {
            "nome": request.args.get("nome") or None,
            "cpf_cnpj": request.args.get("cpf_cnpj") or None,
            "municipio": request.args.get("municipio") or None,
            "num_doc": request.args.get("num_doc") or None,
            "num_processo": request.args.get("num_processo") or None,
            "num_auto": request.args.get("num_auto") or None,
            "fonte": request.args.get("fonte") or None,
            "limit": int(request.args.get("limit", 50)),
        }
        return jsonify(consulta_sema.search_desembargos(**params))
    except Exception as e:
        return jsonify({"error": f"SEMA DB nao disponivel: {e}"}), 503


@app.route("/api/sema/resumo")
@login_required
def api_sema_resumo():
    nome = request.args.get("nome") or None
    cpf_cnpj = request.args.get("cpf_cnpj") or None
    if not nome and not cpf_cnpj:
        return jsonify({"error": "Informe 'nome' ou 'cpf_cnpj'"})
    try:
        return jsonify(consulta_sema.resumo_autuado(nome=nome, cpf_cnpj=cpf_cnpj))
    except Exception as e:
        return jsonify({"error": f"SEMA DB nao disponivel: {e}"}), 503


# ============================================================
# Startup warmup + scheduled updates
# ============================================================

_ibama_rebuilding = False


def _rebuild_ibama_background():
    """Run IBAMA auto-update in background thread to self-heal missing DB."""
    global _ibama_rebuilding
    _ibama_rebuilding = True
    print("IBAMA self-heal: starting background download...")
    try:
        script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "auto_update.py")
        result = subprocess.run(
            [sys.executable, "-u", script],
            capture_output=True, text=True, timeout=900  # 15 min max
        )
        print(result.stdout[-500:] if len(result.stdout) > 500 else result.stdout)
        if result.returncode != 0 and result.stderr:
            print(f"IBAMA self-heal stderr: {result.stderr[-300:]}")
        try:
            conn = consulta.get_conn()
            n = conn.execute("SELECT COUNT(*) FROM autos_infracao").fetchone()[0]
            conn.close()
            if n > 0:
                print(f"IBAMA self-heal SUCCESS: {n} autos loaded")
            else:
                print("IBAMA self-heal: DB still empty after rebuild")
        except Exception:
            print("IBAMA self-heal: DB still not available after rebuild")
    except subprocess.TimeoutExpired:
        print("IBAMA self-heal: timeout after 15 minutes")
    except Exception as e:
        print(f"IBAMA self-heal error: {e}")
    finally:
        _ibama_rebuilding = False
        refresh_stats_cache()


def _scheduled_update():
    """Background thread: runs daily updates at midnight BRT (03:00 UTC).
    If it fails, retries at 5 AM BRT (08:00 UTC).
    NOTE: Only updates IBAMA. SEMA is provided via build artifact (verify_sema_db.py).
    """
    BRT_OFFSET = -3  # BRT = UTC-3
    while True:
        try:
            now_utc = datetime.utcnow()
            now_brt = now_utc + timedelta(hours=BRT_OFFSET)

            # Next midnight BRT = 03:00 UTC
            next_midnight_brt = now_brt.replace(hour=0, minute=0, second=0, microsecond=0)
            if next_midnight_brt <= now_brt:
                next_midnight_brt += timedelta(days=1)
            next_run_utc = next_midnight_brt - timedelta(hours=BRT_OFFSET)
            wait_secs = (next_run_utc - now_utc).total_seconds()

            print(f"Scheduled update: next run at {next_midnight_brt:%Y-%m-%d %H:%M} BRT ({wait_secs/3600:.1f}h from now)")
            time.sleep(max(wait_secs, 60))

            print(f"\n=== Scheduled update starting at {datetime.utcnow():%H:%M:%S} UTC ===")
            script_dir = os.path.dirname(os.path.abspath(__file__))

            # Update IBAMA
            ibama_ok = False
            try:
                r = subprocess.run(
                    [sys.executable, "-u", os.path.join(script_dir, "auto_update.py")],
                    capture_output=True, text=True, timeout=900
                )
                print(r.stdout[-300:] if len(r.stdout) > 300 else r.stdout)
                ibama_ok = r.returncode == 0
            except Exception as e:
                print(f"Scheduled IBAMA update error: {e}")

            if not ibama_ok:
                # Retry at 5 AM BRT (08:00 UTC)
                now_utc = datetime.utcnow()
                now_brt = now_utc + timedelta(hours=BRT_OFFSET)
                retry_brt = now_brt.replace(hour=5, minute=0, second=0, microsecond=0)
                if retry_brt <= now_brt:
                    retry_brt += timedelta(days=1)
                retry_utc = retry_brt - timedelta(hours=BRT_OFFSET)
                wait_retry = (retry_utc - now_utc).total_seconds()

                if 0 < wait_retry < 86400:
                    print(f"Scheduled update: IBAMA failed. Retry at 5 AM BRT ({wait_retry/3600:.1f}h)")
                    time.sleep(wait_retry)

                    print(f"\n=== Retry update at {datetime.utcnow():%H:%M:%S} UTC ===")
                    try:
                        r = subprocess.run(
                            [sys.executable, "-u", os.path.join(script_dir, "auto_update.py")],
                            capture_output=True, text=True, timeout=900
                        )
                        print(r.stdout[-300:] if len(r.stdout) > 300 else r.stdout)
                    except Exception as e:
                        print(f"Retry IBAMA error: {e}")

            print(f"=== Scheduled update complete ===\n")
            # Refresh stats cache after DB update
            refresh_stats_cache()

        except Exception as e:
            print(f"Scheduler error: {e}")
            time.sleep(3600)  # Wait 1h on unexpected error


def warmup_db():
    """Lightweight startup check. DBs should already be present from build phase.
    NO heavy downloads at runtime to avoid OOM on 512MB Render instances.
    Only self-heals IBAMA (smaller download). SEMA must be in build artifact."""
    # IBAMA warmup
    ibama_ok = False
    try:
        conn = consulta.get_conn()
        # Use EXISTS instead of COUNT(*) to avoid full table scan
        has_data = conn.execute("SELECT 1 FROM autos_infracao LIMIT 1").fetchone()
        conn.close()
        if has_data:
            ibama_ok = True
            print(f"IBAMA DB warmup OK")
        else:
            print("IBAMA DB warmup: table exists but is empty")
    except Exception as e:
        print(f"IBAMA DB warmup skip: {e}")

    if not ibama_ok and not _ibama_rebuilding:
        print("IBAMA DB not available - launching background self-heal...")
        t = threading.Thread(target=_rebuild_ibama_background, daemon=True)
        t.start()

    # SEMA warmup - check file exists and has real data BEFORE opening connection
    # (sqlite3.connect creates an empty file if it doesn't exist!)
    # NO SELF-HEAL DOWNLOAD: sema.db must be provided by build phase (verify_sema_db.py)
    sema_db = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sema.db")
    if os.path.exists(sema_db) and os.path.getsize(sema_db) > 1_000_000:
        try:
            conn = consulta_sema.get_conn()
            has_data = conn.execute("SELECT 1 FROM sema_autos_infracao LIMIT 1").fetchone()
            conn.close()
            if has_data:
                print(f"SEMA DB warmup OK")
            else:
                print("SEMA DB warmup: table exists but is empty")
        except Exception as e:
            print(f"SEMA DB warmup FAILED: {e}")
    else:
        size = os.path.getsize(sema_db) if os.path.exists(sema_db) else 0
        print(f"SEMA DB warmup skip: file missing or too small ({size} bytes)")
        print("SEMA data unavailable - sema.db should be provided during build phase")


warmup_db()


def _deep_warmup():
    """Background: touch main indexes/tables so SQLite pages are in OS cache.
    This makes the FIRST user query fast instead of cold-starting from disk."""
    import time as _t
    _t.sleep(2)  # let gunicorn finish booting
    print("Deep warmup: pre-loading SQLite pages...")
    try:
        conn = consulta.get_conn()
        # Read a sample from each main table to warm OS page cache
        conn.execute("SELECT * FROM autos_infracao ORDER BY rowid DESC LIMIT 50").fetchall()
        conn.execute("SELECT * FROM termos_embargo ORDER BY rowid DESC LIMIT 50").fetchall()
        # Touch name-search paths (most common query pattern)
        conn.execute("SELECT COUNT(*) FROM autos_infracao WHERE NOME_INFRATOR LIKE 'ZZZ%'").fetchone()
        conn.execute("SELECT COUNT(*) FROM termos_embargo WHERE NOME_EMBARGADO LIKE 'ZZZ%'").fetchone()
        conn.close()
        print("Deep warmup: IBAMA OK")
    except Exception as e:
        print(f"Deep warmup IBAMA skip: {e}")
    try:
        conn = consulta_sema.get_conn()
        for tbl in ['sema_autos_infracao', 'sema_outros_termos', 'sema_embargos', 'sema_desembargos']:
            conn.execute(f"SELECT * FROM {tbl} ORDER BY rowid DESC LIMIT 50").fetchall()
        conn.close()
        print("Deep warmup: SEMA OK")
    except Exception as e:
        print(f"Deep warmup SEMA skip: {e}")
    print("Deep warmup: done")


# Pre-compute stats cache + deep warmup in background
def _startup_background():
    refresh_stats_cache()
    _deep_warmup()

threading.Thread(target=_startup_background, daemon=True).start()

# Start scheduled daily update thread (midnight BRT, retry 5 AM BRT)
_scheduler_thread = threading.Thread(target=_scheduled_update, daemon=True)
_scheduler_thread.start()


# ── Keep-alive self-ping ─────────────────────────────────────
# Pings own /health endpoint every 5 min to prevent Render from sleeping.
_SELF_URL = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")


def _keep_alive():
    """Self-ping loop: hits /health every 5 minutes to stay warm."""
    if not _SELF_URL:
        print("Keep-alive: RENDER_EXTERNAL_URL not set, skipping")
        return
    time.sleep(30)  # Wait for app to start
    url = f"{_SELF_URL}/health"
    print(f"Keep-alive: pinging {url} every 5 min")
    while True:
        try:
            urllib.request.urlopen(url, timeout=10)
        except Exception:
            pass
        time.sleep(300)  # 5 minutes


threading.Thread(target=_keep_alive, daemon=True).start()


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
