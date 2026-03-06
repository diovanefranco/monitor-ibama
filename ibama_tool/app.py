#!/usr/bin/env python3
"""
IBAMA Monitor - Web interface for searching IBAMA enforcement data.
"""
import os
from functools import wraps
from flask import Flask, request, jsonify, render_template, session, redirect, url_for

import consulta

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


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
