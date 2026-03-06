#!/usr/bin/env python3
"""
IBAMA Query Tool - Search autos de infracao and termos de embargo.
Usage: python3 consulta.py <command> [args]
"""
import sqlite3, sys, json, os, unicodedata, re

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
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA cache_size=-64000")  # 64MB read cache
    conn.execute("PRAGMA mmap_size=268435456")  # 256MB memory-mapped I/O
    conn.execute("PRAGMA temp_store=MEMORY")  # temp tables in RAM
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


def _fts_match_expr(terms):
    """Build FTS5 MATCH expression: each word must match (AND logic)."""
    words = terms.strip().split()
    # Quote each word and join with AND
    return ' '.join(f'"{w}"' for w in words if w)


def search_autos(nome=None, cpf_cnpj=None, uf=None, municipio=None,
                 num_auto=None, num_processo=None, tipo_infracao=None,
                 ano_inicio=None, ano_fim=None, limit=50):
    """Search autos de infracao with multiple filters (FTS5 JOIN for speed)."""
    conn = get_conn()
    joins = []
    conditions = []
    params = []

    # FTS5 JOIN for name search (15x faster than IN subquery on disk)
    if nome:
        norm = strip_accents(nome).upper()
        match_expr = _fts_match_expr(norm)
        if match_expr:
            joins.append('JOIN fts_ai_nome fn ON a.rowid = fn.rowid')
            conditions.append('fn.fts_ai_nome MATCH ?')
            params.append(match_expr)

    # CPF: exact match on digits (uses index directly)
    if cpf_cnpj:
        clean = re.sub(r'[^0-9]', '', cpf_cnpj)
        if len(clean) >= 11:
            conditions.append('a.CPF_CNPJ_NORM = ?')
        else:
            conditions.append('a.CPF_CNPJ_NORM LIKE ?')
            clean = f'%{clean}%'
        params.append(clean)

    if uf:
        conditions.append('a.UF = ?')
        params.append(uf.upper())

    # FTS5 JOIN for municipio search
    if municipio:
        norm = strip_accents(municipio).upper()
        match_expr = _fts_match_expr(norm)
        if match_expr:
            joins.append('JOIN fts_ai_mun fm ON a.rowid = fm.rowid')
            conditions.append('fm.fts_ai_mun MATCH ?')
            params.append(match_expr)

    if num_auto:
        conditions.append('a.NUM_AUTO_INFRACAO = ?')
        params.append(num_auto)
    if num_processo:
        clean = re.sub(r'[^0-9]', '', num_processo)
        conditions.append('a.NUM_PROCESSO_NORM LIKE ?')
        params.append(f'%{clean}%')
    if tipo_infracao:
        conditions.append('a.TIPO_INFRACAO LIKE ?')
        params.append(f'%{tipo_infracao.upper()}%')
    if ano_inicio:
        conditions.append("a.DAT_HORA_AUTO_INFRACAO >= ?")
        params.append(f"{ano_inicio}-01-01")
    if ano_fim:
        conditions.append("a.DAT_HORA_AUTO_INFRACAO <= ?")
        params.append(f"{ano_fim}-12-31")

    has_fts = bool(joins)
    join_clause = "\n        ".join(joins) if joins else ""
    where = " AND ".join(conditions) if conditions else "1=1"

    if has_fts:
        # FTS active: skip ORDER BY in SQL (causes slow random disk reads)
        # Fetch generous limit, sort in Python (instant for in-memory data)
        max_fetch = max(limit + 1, 5000)
        sql = f"""
            SELECT a.* FROM autos_infracao a
            {join_clause}
            WHERE {where}
            LIMIT ?
        """
        params.append(max_fetch)
        rows = conn.execute(sql, params).fetchall()
        results = [dict(r) for r in rows]
        # Sort in Python (fast: <1ms for thousands of rows)
        results.sort(key=lambda r: r.get('DAT_HORA_AUTO_INFRACAO', ''), reverse=True)
        has_more = len(results) > limit
        results = results[:limit]
    else:
        # No FTS: use SQL ORDER BY (fast with B-tree indexes)
        fetch_limit = limit + 1
        sql = f"""
            SELECT a.* FROM autos_infracao a
            WHERE {where}
            ORDER BY a.DAT_HORA_AUTO_INFRACAO DESC
            LIMIT ?
        """
        params.append(fetch_limit)
        rows = conn.execute(sql, params).fetchall()
        has_more = len(rows) > limit
        results = [dict(r) for r in rows[:limit]]

    # Remove normalized columns from results (internal use only)
    for r in results:
        r.pop('NOME_INFRATOR_NORM', None)
        r.pop('MUNICIPIO_NORM', None)
        r.pop('CPF_CNPJ_NORM', None)
        r.pop('NUM_PROCESSO_NORM', None)

    conn.close()
    showing = len(results)
    total = f"{showing}+" if has_more else str(showing)
    return {"total": total, "showing": showing, "results": results}


def search_embargos(nome=None, cpf_cnpj=None, uf=None, municipio=None,
                    num_tad=None, num_processo=None, num_auto=None,
                    ativos_only=False, limit=50):
    """Search termos de embargo with multiple filters (FTS5 JOIN for speed)."""
    conn = get_conn()
    joins = []
    conditions = []
    params = []

    # FTS5 JOIN for name search
    if nome:
        norm = strip_accents(nome).upper()
        match_expr = _fts_match_expr(norm)
        if match_expr:
            joins.append('JOIN fts_te_nome fn ON t.rowid = fn.rowid')
            conditions.append('fn.fts_te_nome MATCH ?')
            params.append(match_expr)

    # CPF: exact match on digits
    if cpf_cnpj:
        clean = re.sub(r'[^0-9]', '', cpf_cnpj)
        if len(clean) >= 11:
            conditions.append('t.CPF_CNPJ_NORM = ?')
        else:
            conditions.append('t.CPF_CNPJ_NORM LIKE ?')
            clean = f'%{clean}%'
        params.append(clean)

    if uf:
        conditions.append('t.UF = ?')
        params.append(uf.upper())

    # FTS5 JOIN for municipio search
    if municipio:
        norm = strip_accents(municipio).upper()
        match_expr = _fts_match_expr(norm)
        if match_expr:
            joins.append('JOIN fts_te_mun fm ON t.rowid = fm.rowid')
            conditions.append('fm.fts_te_mun MATCH ?')
            params.append(match_expr)

    if num_tad:
        conditions.append('t.NUM_TAD = ?')
        params.append(num_tad)
    if num_processo:
        clean = re.sub(r'[^0-9]', '', num_processo)
        conditions.append('t.NUM_PROCESSO_NORM LIKE ?')
        params.append(f'%{clean}%')
    if num_auto:
        conditions.append('t.NUM_AUTO_INFRACAO = ?')
        params.append(num_auto)
    if ativos_only:
        conditions.append("(t.SIT_DESEMBARGO IS NULL OR t.SIT_DESEMBARGO = '')")
        conditions.append("t.SIT_CANCELADO = 'N'")

    has_fts = bool(joins)
    join_clause = "\n        ".join(joins) if joins else ""
    where = " AND ".join(conditions) if conditions else "1=1"

    if has_fts:
        # FTS active: skip ORDER BY in SQL, sort in Python
        max_fetch = max(limit + 1, 5000)
        sql = f"""
            SELECT t.* FROM termos_embargo t
            {join_clause}
            WHERE {where}
            LIMIT ?
        """
        params.append(max_fetch)
        rows = conn.execute(sql, params).fetchall()
        results = [dict(r) for r in rows]
        results.sort(key=lambda r: r.get('DAT_EMBARGO', ''), reverse=True)
        has_more = len(results) > limit
        results = results[:limit]
    else:
        fetch_limit = limit + 1
        sql = f"""
            SELECT t.* FROM termos_embargo t
            WHERE {where}
            ORDER BY t.DAT_EMBARGO DESC
            LIMIT ?
        """
        params.append(fetch_limit)
        rows = conn.execute(sql, params).fetchall()
        has_more = len(rows) > limit
        results = [dict(r) for r in rows[:limit]]

    for r in results:
        r.pop('NOME_EMBARGADO_NORM', None)
        r.pop('MUNICIPIO_NORM', None)
        r.pop('CPF_CNPJ_NORM', None)
        r.pop('NUM_PROCESSO_NORM', None)

    conn.close()
    showing = len(results)
    total = f"{showing}+" if has_more else str(showing)
    return {"total": total, "showing": showing, "results": results}


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
    """Full profile of an autuado: autos + embargos + totals.
    Single-query approach: fetch all matching rows once, process in Python.
    Avoids 5 separate FTS JOINs which are slow on disk-bound servers.
    """
    conn = get_conn()

    # Build FTS joins for autos_infracao
    ai_joins = []
    ai_conditions = []
    ai_params = []

    if nome:
        norm = strip_accents(nome).upper()
        match_expr = _fts_match_expr(norm)
        if match_expr:
            ai_joins.append('JOIN fts_ai_nome fn ON a.rowid = fn.rowid')
            ai_conditions.append('fn.fts_ai_nome MATCH ?')
            ai_params.append(match_expr)
    if cpf_cnpj:
        clean = re.sub(r'[^0-9]', '', cpf_cnpj)
        if len(clean) >= 11:
            ai_conditions.append('a.CPF_CNPJ_NORM = ?')
        else:
            ai_conditions.append('a.CPF_CNPJ_NORM LIKE ?')
            clean = f'%{clean}%'
        ai_params.append(clean)

    ai_join = " ".join(ai_joins)
    ai_where = " AND ".join(ai_conditions) if ai_conditions else "1=1"

    # Build FTS joins for termos_embargo
    te_joins = []
    te_conditions = []
    te_params = []

    if nome:
        norm = strip_accents(nome).upper()
        match_expr = _fts_match_expr(norm)
        if match_expr:
            te_joins.append('JOIN fts_te_nome fn ON t.rowid = fn.rowid')
            te_conditions.append('fn.fts_te_nome MATCH ?')
            te_params.append(match_expr)
    if cpf_cnpj:
        clean = re.sub(r'[^0-9]', '', cpf_cnpj)
        if len(clean) >= 11:
            te_conditions.append('t.CPF_CNPJ_NORM = ?')
        else:
            te_conditions.append('t.CPF_CNPJ_NORM LIKE ?')
            clean = f'%{clean}%'
        te_params.append(clean)

    te_join = " ".join(te_joins)
    te_where = " AND ".join(te_conditions) if te_conditions else "1=1"

    # === SINGLE QUERY: fetch all matching autos at once ===
    ai_rows = conn.execute(f"""
        SELECT a.DAT_HORA_AUTO_INFRACAO, a.SIT_CANCELADO, a.UF, a.TIPO_INFRACAO,
               a.VAL_AUTO_INFRACAO, a.TIPO_MULTA, a.DES_STATUS_FORMULARIO,
               a.NUM_AUTO_INFRACAO, a.MUNICIPIO,
               a.DES_AUTO_INFRACAO, a.DS_ENQUADRAMENTO_NAO_ADMINISTRATIVO
        FROM autos_infracao a {ai_join} WHERE {ai_where}
    """, ai_params).fetchall()

    # Process autos in Python (instant for <10K rows)
    total_autos = len(ai_rows)
    autos_ativos = 0
    autos_cancelados = 0
    ufs = set()
    tipos = set()
    datas = []
    total_valor = 0.0
    valores_invalidos = 0
    status_counts = {}
    active_autos = []

    for r in ai_rows:
        dat, cancelado, uf, tipo, valor, tipo_multa, status = r[0], r[1], r[2], r[3], r[4], r[5], r[6]
        if cancelado == 'N':
            autos_ativos += 1
            val = parse_valor_br(valor)
            if valor and valor.strip() and val == 0.0:
                valores_invalidos += 1
            total_valor += val
            st = status or 'Desconhecido'
            status_counts[st] = status_counts.get(st, 0) + 1
            active_autos.append(dict(zip(
                ['DAT_HORA_AUTO_INFRACAO', 'SIT_CANCELADO', 'UF', 'TIPO_INFRACAO',
                 'VAL_AUTO_INFRACAO', 'TIPO_MULTA', 'DES_STATUS_FORMULARIO',
                 'NUM_AUTO_INFRACAO', 'MUNICIPIO',
                 'DES_AUTO_INFRACAO', 'DS_ENQUADRAMENTO_NAO_ADMINISTRATIVO'],
                r
            )))
        elif cancelado == 'S':
            autos_cancelados += 1
        if uf:
            ufs.add(uf)
        if tipo:
            tipos.add(tipo)
        if dat:
            datas.append(dat)

    ai_summary = {
        'total_autos': total_autos,
        'autos_ativos': autos_ativos,
        'autos_cancelados': autos_cancelados,
        'ufs': ','.join(sorted(ufs)) if ufs else None,
        'tipos': ','.join(sorted(tipos)) if tipos else None,
        'primeiro_auto': min(datas) if datas else None,
        'ultimo_auto': max(datas) if datas else None,
        'valor_total': fmt_valor(total_valor),
        'valor_total_float': total_valor,
        'status_formulario': status_counts,
    }
    if valores_invalidos:
        ai_summary['valores_nao_parseados'] = valores_invalidos

    # Recent active autos (last 10)
    active_autos.sort(key=lambda r: r.get('DAT_HORA_AUTO_INFRACAO', ''), reverse=True)
    recent_ai = active_autos[:10]

    # === SINGLE QUERY: fetch all matching embargos at once ===
    te_rows = conn.execute(f"""
        SELECT t.SIT_DESEMBARGO, t.SIT_CANCELADO, t.MUNICIPIO,
               t.NUM_TAD, t.DAT_EMBARGO, t.QTD_AREA_EMBARGADA, t.UF,
               t.DES_TAD, t.DES_LOCALIZACAO
        FROM termos_embargo t {te_join} WHERE {te_where}
    """, te_params).fetchall()

    # Process embargos in Python
    total_embargos = len(te_rows)
    embargos_ativos = 0
    embargos_cancelados = 0
    municipios = set()
    active_embargos = []

    for r in te_rows:
        desembargo, cancelado, mun = r[0], r[1], r[2]
        if cancelado == 'S':
            embargos_cancelados += 1
        elif not desembargo or desembargo == '':
            embargos_ativos += 1
            active_embargos.append(dict(zip(
                ['SIT_DESEMBARGO', 'SIT_CANCELADO', 'MUNICIPIO',
                 'NUM_TAD', 'DAT_EMBARGO', 'QTD_AREA_EMBARGADA', 'UF',
                 'DES_TAD', 'DES_LOCALIZACAO'],
                r
            )))
        if mun:
            municipios.add(mun)

    te_summary = {
        'total_embargos': total_embargos,
        'embargos_ativos': embargos_ativos,
        'embargos_cancelados': embargos_cancelados,
        'municipios': ','.join(sorted(municipios)) if municipios else None,
    }

    # Sort active embargos by date
    active_embargos.sort(key=lambda r: r.get('DAT_EMBARGO', ''), reverse=True)

    conn.close()

    return {
        "autos_infracao": ai_summary,
        "termos_embargo": te_summary,
        "ultimos_autos": recent_ai,
        "embargos_ativos": active_embargos[:10],
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
