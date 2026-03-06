#!/usr/bin/env python3
"""
IBAMA Monitor - Web interface for searching IBAMA enforcement data.
"""
from flask import Flask, request, jsonify, render_template
import consulta

app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/stats")
def api_stats():
    return jsonify(consulta.stats())


@app.route("/api/autos")
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
def api_texto():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"error": "Parametro 'q' obrigatorio"})
    tabela = request.args.get("tabela", "autos")
    limit = int(request.args.get("limit", 50))
    return jsonify(consulta.search_texto(q, tabela=tabela, limit=limit))


@app.route("/api/resumo")
def api_resumo():
    nome = request.args.get("nome") or None
    cpf_cnpj = request.args.get("cpf_cnpj") or None
    if not nome and not cpf_cnpj:
        return jsonify({"error": "Informe 'nome' ou 'cpf_cnpj'"})
    return jsonify(consulta.resumo_autuado(nome=nome, cpf_cnpj=cpf_cnpj))


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
