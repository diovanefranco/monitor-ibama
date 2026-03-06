#!/usr/bin/env python3
"""
IBAMA Query Tool - Search autos de infracao and termos de embargo.
Usage: python3 consulta.py <command> [args]
"""
import sqlite3, sys, json, os, unicodedata

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(_SCRIPT_DIR, "ibama.db")


def strip_accents(s):
    """Remove accents/diacritics from string (ã→a, é→e, etc.)."""
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def parse_valor_br(v):
    """Parse Brazilian currency string to float. Returns 0.0 on failure."""
    if not v or v.strip() == '':
        return 0.0
    try:
        return float(v.replace('.', '').replace(',', '.'))
    except ValueError:
        return 0.0


def fmt_valor(v):
    """Format float as Brazilian currency."""
    if v == 0:
        return 'N/A'
    return f"R$ {v:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')


def search_autos(nome=None, cpf_cnpj=None, uf=None, municipio=None,
                 num_auto=None, num_processo=None, tipo_infracao=None,
                 ano_inicio=None, ano_fim=None, limit=50):
    """Search autos de infracao with multiple filters."""
    conn = get_conn()
    conditions = []
    params = []

    if nome:
        conditions.append('UPPER(NOME_INFRATOR) LIKE ?')
        params.append(f'%{strip_accents(nome).upper()}%')
    if cpf_cnpj:
        clean = cpf_cnpj.replace('.', '').replace('-', '').replace('/', '')
        conditions.append('REPLACE(REPLACE(REPLACE(CPF_CNPJ_INFRATOR, ".", ""), "-", ""), "/", "") LIKE ?')
        params.append(f'%{clean}%')
    if uf:
        conditions.append('UPPER(UF) = ?')
        params.append(uf.upper())
    if municipio:
        conditions.append('UPPER(MUNICIPIO) LIKE ?')
        params.append(f'%{strip_accents(municipio).upper()}%')
    if num_auto:
        conditions.append('NUM_AUTO_INFRACAO = ?')
        params.append(num_auto)
    if num_processo:
        clean = num_processo.replace('.', '').replace('/', '').replace('-', '')
        conditions.append('REPLACE(REPLACE(REPLACE(NUM_PROCESSO, ".", ""), "/", ""), "-", "") LIKE ?')
        params.append(f'%{clean}%')
    if tipo_infracao:
        conditions.append('UPPER(TIPO_INFRACAO) LIKE ?')
        params.append(f'%{tipo_infracao.upper()}%')
    if ano_inicio:
        conditions.append("DAT_HORA_AUTO_INFRACAO >= ?")
        params.append(f"{ano_inicio}-01-01")
    if ano_fim:
        conditions.append("DAT_HORA_AUTO_INFRACAO <= ?")
        params.append(f"{ano_fim}-12-31")

    where = " AND ".join(conditions) if conditions else "1=1"
    sql = f"""
        SELECT * FROM autos_infracao
        WHERE {where}
        ORDER BY DAT_HORA_AUTO_INFRACAO DESC
        LIMIT ?
    """
    params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    results = [dict(r) for r in rows]

    count_sql = f"SELECT COUNT(*) FROM autos_infracao WHERE {where}"
    total = conn.execute(count_sql, params[:-1]).fetchone()[0]

    conn.close()
    return {"total": total, "showing": len(results), "results": results}


def search_embargos(nome=None, cpf_cnpj=None, uf=None, municipio=None,
                    num_tad=None, num_processo=None, num_auto=None,
                    ativos_only=False, limit=50):
    """Search termos de embargo with multiple filters."""
    conn = get_conn()
    conditions = []
    params = []

    if nome:
        conditions.append('UPPER(NOME_EMBARGADO) LIKE ?')
        params.append(f'%{strip_accents(nome).upper()}%')
    if cpf_cnpj:
        clean = cpf_cnpj.replace('.', '').replace('-', '').replace('/', '')
        conditions.append('REPLACE(REPLACE(REPLACE(CPF_CNPJ_EMBARGADO, ".", ""), "-", ""), "/", "") LIKE ?')
        params.append(f'%{clean}%')
    if uf:
        conditions.append('UPPER(UF) = ?')
        params.append(uf.upper())
    if municipio:
        conditions.append('UPPER(MUNICIPIO) LIKE ?')
        params.append(f'%{strip_accents(municipio).upper()}%')
    if num_tad:
        conditions.append('NUM_TAD = ?')
        params.append(num_tad)
    if num_processo:
        clean = num_processo.replace('.', '').replace('/', '').replace('-', '')
        conditions.append('REPLACE(REPLACE(REPLACE(NUM_PROCESSO, ".", ""), "/", ""), "-", "") LIKE ?')
        params.append(f'%{clean}%')
    if num_auto:
        conditions.append('NUM_AUTO_INFRACAO = ?')
        params.append(num_auto)
    if ativos_only:
        conditions.append("(SIT_DESEMBARGO IS NULL OR SIT_DESEMBARGO = '')")
        conditions.append("SIT_CANCELADO = 'N'")

    where = " AND ".join(conditions) if conditions else "1=1"
    sql = f"""
        SELECT * FROM termos_embargo
        WHERE {where}
        ORDER BY DAT_EMBARGO DESC
        LIMIT ?
    """
    params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    results = [dict(r) for r in rows]

    count_sql = f"SELECT COUNT(*) FROM termos_embargo WHERE {where}"
    total = conn.execute(count_sql, params[:-1]).fetchone()[0]

    conn.close()
    return {"total": total, "showing": len(results), "results": results}


def search_texto(termo, tabela="autos", limit=50):
    """Full-text search on descriptions using FTS5.
    tabela: 'autos' or 'embargos'
    """
    conn = get_conn()

    if tabela == "autos":
        fts_table = "fts_autos"
        main_table = "autos_infracao"
        id_col = "SEQ_AUTO_INFRACAO"
    else:
        fts_table = "fts_embargos"
        main_table = "termos_embargo"
        id_col = "SEQ_TAD"

    sql = f"""
        SELECT m.* FROM {main_table} m
        JOIN {fts_table} f ON m.rowid = f.rowid
        WHERE {fts_table} MATCH ?
        ORDER BY rank
        LIMIT ?
    """

    try:
        rows = conn.execute(sql, [termo, limit]).fetchall()
        results = [dict(r) for r in rows]
    except sqlite3.OperationalError as e:
        conn.close()
        return {"error": str(e), "hint": "Use aspas para frases exatas, OR para alternativas"}

    count_sql = f"SELECT COUNT(*) FROM {fts_table} WHERE {fts_table} MATCH ?"
    try:
        total = conn.execute(count_sql, [termo]).fetchone()[0]
    except sqlite3.OperationalError:
        total = len(results)

    conn.close()
    return {"total": total, "showing": len(results), "results": results}


def resumo_autuado(nome=None, cpf_cnpj=None):
    """Full profile of an autuado: autos + embargos + totals."""
    conn = get_conn()

    conditions_ai = []
    conditions_te = []
    params = []

    if nome:
        conditions_ai.append('UPPER(NOME_INFRATOR) LIKE ?')
        conditions_te.append('UPPER(NOME_EMBARGADO) LIKE ?')
        params.append(f'%{strip_accents(nome).upper()}%')
    if cpf_cnpj:
        clean = cpf_cnpj.replace('.', '').replace('-', '').replace('/', '')
        conditions_ai.append('REPLACE(REPLACE(REPLACE(CPF_CNPJ_INFRATOR, ".", ""), "-", ""), "/", "") LIKE ?')
        conditions_te.append('REPLACE(REPLACE(REPLACE(CPF_CNPJ_EMBARGADO, ".", ""), "-", ""), "/", "") LIKE ?')
        params.append(f'%{clean}%')

    where_ai = " AND ".join(conditions_ai) if conditions_ai else "1=1"
    where_te = " AND ".join(conditions_te) if conditions_te else "1=1"

    # Autos summary
    ai_sql = f"""
        SELECT COUNT(*) as total_autos,
               SUM(CASE WHEN SIT_CANCELADO = 'N' THEN 1 ELSE 0 END) as autos_ativos,
               SUM(CASE WHEN SIT_CANCELADO = 'S' THEN 1 ELSE 0 END) as autos_cancelados,
               GROUP_CONCAT(DISTINCT UF) as ufs,
               GROUP_CONCAT(DISTINCT TIPO_INFRACAO) as tipos,
               MIN(DAT_HORA_AUTO_INFRACAO) as primeiro_auto,
               MAX(DAT_HORA_AUTO_INFRACAO) as ultimo_auto
        FROM autos_infracao WHERE {where_ai}
    """

    # Value summary
    val_sql = f"""
        SELECT VAL_AUTO_INFRACAO, TIPO_MULTA, DES_STATUS_FORMULARIO
        FROM autos_infracao WHERE {where_ai} AND SIT_CANCELADO = 'N'
    """

    # Embargos summary
    te_sql = f"""
        SELECT COUNT(*) as total_embargos,
               SUM(CASE WHEN (SIT_DESEMBARGO IS NULL OR SIT_DESEMBARGO = '') AND SIT_CANCELADO = 'N' THEN 1 ELSE 0 END) as embargos_ativos,
               SUM(CASE WHEN SIT_CANCELADO = 'S' THEN 1 ELSE 0 END) as embargos_cancelados,
               GROUP_CONCAT(DISTINCT MUNICIPIO) as municipios
        FROM termos_embargo WHERE {where_te}
    """

    ai_summary = dict(conn.execute(ai_sql, params).fetchone())
    te_summary = dict(conn.execute(te_sql, params).fetchone())

    # Calculate total values with proper error tracking
    val_rows = conn.execute(val_sql, params).fetchall()
    total_valor = 0.0
    valores_invalidos = 0
    status_counts = {}
    for r in val_rows:
        val = parse_valor_br(r[0])
        if r[0] and r[0].strip() and val == 0.0:
            valores_invalidos += 1
        total_valor += val
        status = r[2] or 'Desconhecido'
        status_counts[status] = status_counts.get(status, 0) + 1

    ai_summary['valor_total'] = fmt_valor(total_valor)
    ai_summary['valor_total_float'] = total_valor
    ai_summary['status_formulario'] = status_counts
    if valores_invalidos:
        ai_summary['valores_nao_parseados'] = valores_invalidos

    # Recent autos (last 10)
    recent_ai = conn.execute(f"""
        SELECT NUM_AUTO_INFRACAO, DAT_HORA_AUTO_INFRACAO, VAL_AUTO_INFRACAO,
               TIPO_INFRACAO, UF, MUNICIPIO, DES_STATUS_FORMULARIO,
               DES_AUTO_INFRACAO, DS_ENQUADRAMENTO_NAO_ADMINISTRATIVO
        FROM autos_infracao WHERE {where_ai} AND SIT_CANCELADO = 'N'
        ORDER BY DAT_HORA_AUTO_INFRACAO DESC LIMIT 10
    """, params).fetchall()

    # Active embargos
    active_te = conn.execute(f"""
        SELECT NUM_TAD, DAT_EMBARGO, QTD_AREA_EMBARGADA, UF, MUNICIPIO,
               DES_TAD, DES_LOCALIZACAO, SIT_DESEMBARGO
        FROM termos_embargo WHERE {where_te}
            AND (SIT_DESEMBARGO IS NULL OR SIT_DESEMBARGO = '')
            AND SIT_CANCELADO = 'N'
        ORDER BY DAT_EMBARGO DESC LIMIT 10
    """, params).fetchall()

    conn.close()

    return {
        "autos_infracao": ai_summary,
        "termos_embargo": te_summary,
        "ultimos_autos": [dict(r) for r in recent_ai],
        "embargos_ativos": [dict(r) for r in active_te],
    }


def stats():
    """Database statistics."""
    conn = get_conn()
    result = {}

    for table in ['autos_infracao', 'termos_embargo']:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        result[table] = count

    # Top UFs for autos
    top_uf = conn.execute("""
        SELECT UF, COUNT(*) as n FROM autos_infracao
        WHERE SIT_CANCELADO = 'N'
        GROUP BY UF ORDER BY n DESC LIMIT 10
    """).fetchall()
    result['top_uf_autos'] = [(r[0], r[1]) for r in top_uf]

    # Top tipos infracao
    top_tipo = conn.execute("""
        SELECT TIPO_INFRACAO, COUNT(*) as n FROM autos_infracao
        WHERE SIT_CANCELADO = 'N'
        GROUP BY TIPO_INFRACAO ORDER BY n DESC LIMIT 10
    """).fetchall()
    result['top_tipo_infracao'] = [(r[0], r[1]) for r in top_tipo]

    # Year range
    year_range = conn.execute("""
        SELECT MIN(DAT_HORA_AUTO_INFRACAO), MAX(DAT_HORA_AUTO_INFRACAO)
        FROM autos_infracao
    """).fetchone()
    result['period'] = (year_range[0], year_range[1])

    # Embargos ativos
    ativos = conn.execute("""
        SELECT COUNT(*) FROM termos_embargo
        WHERE (SIT_DESEMBARGO IS NULL OR SIT_DESEMBARGO = '')
        AND SIT_CANCELADO = 'N'
    """).fetchone()[0]
    result['embargos_ativos'] = ativos

    db_size = os.path.getsize(DB_PATH)
    result['db_size_mb'] = round(db_size / 1024 / 1024, 1)

    conn.close()
    return result


def sql_query(query, params=None):
    """Execute arbitrary SQL query (SELECT only)."""
    if not query.strip().upper().startswith('SELECT'):
        return {"error": "Only SELECT queries allowed"}
    conn = get_conn()
    try:
        rows = conn.execute(query, params or []).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 consulta.py <command> [args]")
        print("Commands: stats, search_ai, search_te, search_texto, resumo, sql")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "stats":
        print(json.dumps(stats(), indent=2, ensure_ascii=False))

    elif cmd == "search_ai":
        kwargs = {}
        args = sys.argv[2:]
        i = 0
        while i < len(args):
            key = args[i].lstrip('-')
            val = args[i+1] if i+1 < len(args) else None
            if key == 'limit':
                kwargs[key] = int(val)
            else:
                kwargs[key] = val
            i += 2
        result = search_autos(**kwargs)
        print(json.dumps(result, indent=2, ensure_ascii=False))

    elif cmd == "search_te":
        kwargs = {}
        args = sys.argv[2:]
        i = 0
        while i < len(args):
            key = args[i].lstrip('-')
            val = args[i+1] if i+1 < len(args) else None
            if key in ('limit',):
                kwargs[key] = int(val)
            elif key == 'ativos_only':
                kwargs[key] = val.lower() in ('true', '1', 'sim')
            else:
                kwargs[key] = val
            i += 2
        result = search_embargos(**kwargs)
        print(json.dumps(result, indent=2, ensure_ascii=False))

    elif cmd == "search_texto":
        # search_texto "desmatamento ilegal" --tabela autos --limit 20
        termo = sys.argv[2]
        tabela = "autos"
        limit = 50
        args = sys.argv[3:]
        i = 0
        while i < len(args):
            key = args[i].lstrip('-')
            val = args[i+1] if i+1 < len(args) else None
            if key == 'tabela':
                tabela = val
            elif key == 'limit':
                limit = int(val)
            i += 2
        result = search_texto(termo, tabela=tabela, limit=limit)
        print(json.dumps(result, indent=2, ensure_ascii=False))

    elif cmd == "resumo":
        kwargs = {}
        args = sys.argv[2:]
        i = 0
        while i < len(args):
            key = args[i].lstrip('-')
            val = args[i+1]
            kwargs[key] = val
            i += 2
        result = resumo_autuado(**kwargs)
        print(json.dumps(result, indent=2, ensure_ascii=False))

    elif cmd == "sql":
        query = sys.argv[2]
        result = sql_query(query)
        print(json.dumps(result, indent=2, ensure_ascii=False))
