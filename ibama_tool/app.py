#!/usr/bin/env python3
"""
IBAMA Monitor - Web interface for searching IBAMA enforcement data.
"""
import os, sys, subprocess, threading, time
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, request, jsonify, render_template, session, redirect, url_for

import consulta
import consulta_sema

app = Flask(__name__)
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


_sema_rebuilding = False  # flag to prevent concurrent rebuilds
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


def _rebuild_sema_background():
    """Run SEMA auto-update in background thread to self-heal empty DB.
    Tries GeoServer first, falls back to pre-built sema.db from GitHub."""
    global _sema_rebuilding
    _sema_rebuilding = True
    print("SEMA self-heal: starting background download...")
    try:
        script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "auto_update_sema.py")
        result = subprocess.run(
            [sys.executable, "-u", script],
            capture_output=True, text=True, timeout=600  # 10 min max
        )
        print(result.stdout[-500:] if len(result.stdout) > 500 else result.stdout)
        if result.returncode != 0 and result.stderr:
            print(f"SEMA self-heal stderr: {result.stderr[-300:]}")

        # Verify it worked
        try:
            conn = consulta_sema.get_conn()
            n = conn.execute("SELECT COUNT(*) FROM sema_autos_infracao").fetchone()[0]
            conn.close()
            if n > 0:
                print(f"SEMA self-heal SUCCESS: {n} autos loaded")
            else:
                print("SEMA self-heal: DB still empty after rebuild")
        except Exception:
            print("SEMA self-heal: DB still not available after rebuild")
    except subprocess.TimeoutExpired:
        print("SEMA self-heal: timeout after 10 minutes")
    except Exception as e:
        print(f"SEMA self-heal error: {e}")
    finally:
        _sema_rebuilding = False


def _scheduled_update():
    """Background thread: runs daily updates at midnight BRT (03:00 UTC).
    If it fails, retries at 5 AM BRT (08:00 UTC)."""
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

            # Update SEMA
            sema_ok = False
            try:
                r = subprocess.run(
                    [sys.executable, "-u", os.path.join(script_dir, "auto_update_sema.py")],
                    capture_output=True, text=True, timeout=600
                )
                print(r.stdout[-300:] if len(r.stdout) > 300 else r.stdout)
                sema_ok = r.returncode == 0
            except Exception as e:
                print(f"Scheduled SEMA update error: {e}")

            if not ibama_ok or not sema_ok:
                # Retry at 5 AM BRT (08:00 UTC)
                now_utc = datetime.utcnow()
                now_brt = now_utc + timedelta(hours=BRT_OFFSET)
                retry_brt = now_brt.replace(hour=5, minute=0, second=0, microsecond=0)
                if retry_brt <= now_brt:
                    retry_brt += timedelta(days=1)
                retry_utc = retry_brt - timedelta(hours=BRT_OFFSET)
                wait_retry = (retry_utc - now_utc).total_seconds()

                if wait_retry > 0 and wait_retry < 86400:
                    failed = []
                    if not ibama_ok:
                        failed.append("IBAMA")
                    if not sema_ok:
                        failed.append("SEMA")
                    print(f"Scheduled update: {', '.join(failed)} failed. Retry at 5 AM BRT ({wait_retry/3600:.1f}h)")
                    time.sleep(wait_retry)

                    print(f"\n=== Retry update at {datetime.utcnow():%H:%M:%S} UTC ===")
                    if not ibama_ok:
                        try:
                            r = subprocess.run(
                                [sys.executable, "-u", os.path.join(script_dir, "auto_update.py")],
                                capture_output=True, text=True, timeout=900
                            )
                            print(r.stdout[-300:] if len(r.stdout) > 300 else r.stdout)
                        except Exception as e:
                            print(f"Retry IBAMA error: {e}")
                    if not sema_ok:
                        try:
                            r = subprocess.run(
                                [sys.executable, "-u", os.path.join(script_dir, "auto_update_sema.py")],
                                capture_output=True, text=True, timeout=600
                            )
                            print(r.stdout[-300:] if len(r.stdout) > 300 else r.stdout)
                        except Exception as e:
                            print(f"Retry SEMA error: {e}")

            print(f"=== Scheduled update complete ===\n")

        except Exception as e:
            print(f"Scheduler error: {e}")
            time.sleep(3600)  # Wait 1h on unexpected error


def warmup_db():
    """Pre-load DB pages into OS cache on startup for faster first queries.
    If any DB is missing or empty, starts background self-heal."""
    ibama_ok = False
    try:
        conn = consulta.get_conn()
        n = conn.execute("SELECT COUNT(*) FROM autos_infracao").fetchone()[0]
        conn.execute("SELECT COUNT(*) FROM termos_embargo").fetchone()
        conn.close()
        if n > 0:
            ibama_ok = True
            print(f"IBAMA DB warmup OK ({n} autos)")
        else:
            print("IBAMA DB warmup: table exists but is empty")
    except Exception as e:
        print(f"IBAMA DB warmup skip: {e}")

    if not ibama_ok and not _ibama_rebuilding:
        print("IBAMA DB not available - launching background self-heal...")
        t = threading.Thread(target=_rebuild_ibama_background, daemon=True)
        t.start()

    # SEMA warmup (independent - won't break if sema.db doesn't exist)
    sema_ok = False
    try:
        conn = consulta_sema.get_conn()
        n = conn.execute("SELECT COUNT(*) FROM sema_autos_infracao").fetchone()[0]
        conn.execute("SELECT COUNT(*) FROM sema_embargos").fetchone()
        conn.execute("SELECT COUNT(*) FROM sema_outros_termos").fetchone()
        conn.execute("SELECT COUNT(*) FROM sema_desembargos").fetchone()
        conn.close()
        if n > 0:
            sema_ok = True
            print(f"SEMA DB warmup OK ({n} autos)")
        else:
            print("SEMA DB warmup: table exists but is empty")
    except Exception as e:
        print(f"SEMA DB warmup skip: {e}")

    # Self-heal: if SEMA DB is missing/empty, rebuild in background
    if not sema_ok and not _sema_rebuilding:
        print("SEMA DB not available - launching background self-heal...")
        t = threading.Thread(target=_rebuild_sema_background, daemon=True)
        t.start()


warmup_db()

# Start scheduled daily update thread (midnight BRT, retry 5 AM BRT)
_scheduler_thread = threading.Thread(target=_scheduled_update, daemon=True)
_scheduler_thread.start()


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
