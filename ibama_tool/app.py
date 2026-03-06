#!/usr/bin/env python3
"""
IBAMA Monitor - Web interface for searching IBAMA enforcement data.
"""
import os, sys, subprocess, threading
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


@app.route("/api/stats")
@login_required
def api_stats():
    return jsonify(consulta.stats())


@app.route("/api/autos")
@login_required
def api_autos():
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


@app.route("/api/embargos")
@login_required
def api_embargos():
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


@app.route("/api/texto")
@login_required
def api_texto():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "Parametro 'q' obrigatorio"})
    tabela = request.args.get("tabela", "autos")
    limit = int(request.args.get("limit", 50))
    return jsonify(consulta.search_texto(q, tabela=tabela, limit=limit))


@app.route("/api/resumo")
@login_required
def api_resumo():
    nome = request.args.get("nome") or None
    cpf_cnpj = request.args.get("cpf_cnpj") or None
    if not nome and not cpf_cnpj:
        return jsonify({"error": "Informe 'nome' ou 'cpf_cnpj'"})
    return jsonify(consulta.resumo_autuado(nome=nome, cpf_cnpj=cpf_cnpj))


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


def _rebuild_sema_background():
    """Run SEMA auto-update in background thread to self-heal empty DB."""
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


def warmup_db():
    """Pre-load DB pages into OS cache on startup for faster first queries.
    If SEMA DB is missing or empty, starts background self-heal."""
    try:
        conn = consulta.get_conn()
        conn.execute("SELECT COUNT(*) FROM autos_infracao").fetchone()
        conn.execute("SELECT COUNT(*) FROM termos_embargo").fetchone()
        conn.close()
        print("IBAMA DB warmup OK")
    except Exception as e:
        print(f"IBAMA DB warmup skip: {e}")

    # SEMA warmup (independent - won't break if sema.db doesn't exist)
    sema_ok = False
    try:
        conn = consulta_sema.get_conn()
        n = conn.execute("SELECT COUNT(*) FROM sema_autos_infracao").fetchone()[0]
        conn.execute("SELECT COUNT(*) FROM sema_areas_embargadas").fetchone()
        conn.close()
        if n > 0:
            sema_ok = True
            print(f"SEMA DB warmup OK ({n} autos)")
        else:
            print("SEMA DB warmup: table exists but is empty")
    except Exception as e:
        print(f"SEMA DB warmup skip: {e}")

    # Self-heal: if SEMA DB is missing/empty, rebuild in background
    if not sema_ok:
        print("SEMA DB not available - launching background self-heal...")
        t = threading.Thread(target=_rebuild_sema_background, daemon=True)
        t.start()


warmup_db()


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
