#!/usr/bin/env python3
"""
SEMA-MT Query Tool - Search autos de infracao and areas embargadas from SEMA-MT.
Follows same patterns as consulta.py (IBAMA).
"""
import sqlite3, sys, json, os, unicodedata, re

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(_SCRIPT_DIR, "sema.db")


def strip_accents(s):
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA cache_size=-64000")
    conn.execute("PRAGMA mmap_size=268435456")
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn


def fmt_valor(v):
    if v == 0:
        return 'N/A'
    return f"R$ {v:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')


def _fts_match_expr(terms):
    words = terms.strip().split()
    return ' '.join(f'"{w}"' for w in words if w)


def search_autos(nome=None, cpf_cnpj=None, municipio=None,
                 num_auto=None, num_processo=None, fonte=None,
                 ano_inicio=None, ano_fim=None, limit=50):
    """Search SEMA-MT autos de infracao."""
    conn = get_conn()
    joins = []
    conditions = []
    params = []

    if nome:
        norm = strip_accents(nome).upper()
        match_expr = _fts_match_expr(norm)
        if match_expr:
            joins.append('JOIN fts_sema_ai_nome fn ON a.rowid = fn.rowid')
            conditions.append('fn.fts_sema_ai_nome MATCH ?')
            params.append(match_expr)

    if cpf_cnpj:
        clean = re.sub(r'[^0-9]', '', cpf_cnpj)
        if len(clean) >= 11:
            conditions.append('a.CPF_CNPJ_NORM = ?')
        else:
            conditions.append('a.CPF_CNPJ_NORM LIKE ?')
            clean = f'%{clean}%'
        params.append(clean)

    if municipio:
        norm = strip_accents(municipio).upper()
        match_expr = _fts_match_expr(norm)
        if match_expr:
            joins.append('JOIN fts_sema_ai_mun fm ON a.rowid = fm.rowid')
            conditions.append('fm.fts_sema_ai_mun MATCH ?')
            params.append(match_expr)

    if num_auto:
        conditions.append('a.NUMERO_AUTO_INFRACAO = ?')
        params.append(num_auto)
    if num_processo:
        clean = re.sub(r'[^0-9]', '', num_processo)
        conditions.append('a.NUM_PROCESSO_NORM LIKE ?')
        params.append(f'%{clean}%')
    if fonte:
        conditions.append('a.FONTE = ?')
        params.append(fonte.upper())
    if ano_inicio:
        conditions.append("a.DATA_AUTO >= ?")
        params.append(f"{ano_inicio}-01-01")
    if ano_fim:
        conditions.append("a.DATA_AUTO <= ?")
        params.append(f"{ano_fim}-12-31")

    has_fts = bool(joins)
    join_clause = "\n        ".join(joins) if joins else ""
    where = " AND ".join(conditions) if conditions else "1=1"

    if has_fts:
        max_fetch = max(limit + 1, 5000)
        sql = f"""
            SELECT a.* FROM sema_autos_infracao a
            {join_clause}
            WHERE {where}
            LIMIT ?
        """
        params.append(max_fetch)
        rows = conn.execute(sql, params).fetchall()
        results = [dict(r) for r in rows]
        results.sort(key=lambda r: r.get('DATA_AUTO', ''), reverse=True)
        has_more = len(results) > limit
        results = results[:limit]
    else:
        fetch_limit = limit + 1
        sql = f"""
            SELECT a.* FROM sema_autos_infracao a
            WHERE {where}
            ORDER BY a.DATA_AUTO DESC
            LIMIT ?
        """
        params.append(fetch_limit)
        rows = conn.execute(sql, params).fetchall()
        has_more = len(rows) > limit
        results = [dict(r) for r in rows[:limit]]

    for r in results:
        r.pop('NOME_NORM', None)
        r.pop('MUNICIPIO_NORM', None)
        r.pop('CPF_CNPJ_NORM', None)
        r.pop('NUM_PROCESSO_NORM', None)

    conn.close()
    showing = len(results)
    total = f"{showing}+" if has_more else str(showing)
    return {"total": total, "showing": showing, "results": results}


def search_embargos(nome=None, cpf_cnpj=None, municipio=None,
                    num_embargo=None, num_processo=None, num_auto=None,
                    fonte=None, limit=50):
    """Search SEMA-MT areas embargadas."""
    conn = get_conn()
    joins = []
    conditions = []
    params = []

    if nome:
        norm = strip_accents(nome).upper()
        match_expr = _fts_match_expr(norm)
        if match_expr:
            joins.append('JOIN fts_sema_te_nome fn ON t.rowid = fn.rowid')
            conditions.append('fn.fts_sema_te_nome MATCH ?')
            params.append(match_expr)

    if cpf_cnpj:
        clean = re.sub(r'[^0-9]', '', cpf_cnpj)
        if len(clean) >= 11:
            conditions.append('t.CPF_CNPJ_NORM = ?')
        else:
            conditions.append('t.CPF_CNPJ_NORM LIKE ?')
            clean = f'%{clean}%'
        params.append(clean)

    if municipio:
        norm = strip_accents(municipio).upper()
        match_expr = _fts_match_expr(norm)
        if match_expr:
            joins.append('JOIN fts_sema_te_mun fm ON t.rowid = fm.rowid')
            conditions.append('fm.fts_sema_te_mun MATCH ?')
            params.append(match_expr)

    if num_embargo:
        conditions.append('t.NUMERO_TERMO_EMBARGO = ?')
        params.append(num_embargo)
    if num_processo:
        clean = re.sub(r'[^0-9]', '', num_processo)
        conditions.append('t.NUM_PROCESSO_NORM LIKE ?')
        params.append(f'%{clean}%')
    if num_auto:
        conditions.append('t.NUMERO_AUTO_INFRACAO = ?')
        params.append(num_auto)
    if fonte:
        conditions.append('t.FONTE = ?')
        params.append(fonte.upper())

    has_fts = bool(joins)
    join_clause = "\n        ".join(joins) if joins else ""
    where = " AND ".join(conditions) if conditions else "1=1"

    if has_fts:
        max_fetch = max(limit + 1, 5000)
        sql = f"""
            SELECT t.* FROM sema_areas_embargadas t
            {join_clause}
            WHERE {where}
            LIMIT ?
        """
        params.append(max_fetch)
        rows = conn.execute(sql, params).fetchall()
        results = [dict(r) for r in rows]
        results.sort(key=lambda r: r.get('DATA_EMBARGO', ''), reverse=True)
        has_more = len(results) > limit
        results = results[:limit]
    else:
        fetch_limit = limit + 1
        sql = f"""
            SELECT t.* FROM sema_areas_embargadas t
            WHERE {where}
            ORDER BY t.DATA_EMBARGO DESC
            LIMIT ?
        """
        params.append(fetch_limit)
        rows = conn.execute(sql, params).fetchall()
        has_more = len(rows) > limit
        results = [dict(r) for r in rows[:limit]]

    for r in results:
        r.pop('NOME_NORM', None)
        r.pop('MUNICIPIO_NORM', None)
        r.pop('CPF_CNPJ_NORM', None)
        r.pop('NUM_PROCESSO_NORM', None)

    conn.close()
    showing = len(results)
    total = f"{showing}+" if has_more else str(showing)
    return {"total": total, "showing": showing, "results": results}


def resumo_autuado(nome=None, cpf_cnpj=None):
    """Full profile: SEMA-MT autos + embargos + totals."""
    conn = get_conn()

    # Build FTS joins for autos
    ai_joins = []
    ai_conditions = []
    ai_params = []

    if nome:
        norm = strip_accents(nome).upper()
        match_expr = _fts_match_expr(norm)
        if match_expr:
            ai_joins.append('JOIN fts_sema_ai_nome fn ON a.rowid = fn.rowid')
            ai_conditions.append('fn.fts_sema_ai_nome MATCH ?')
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

    # Fetch all matching autos
    ai_rows = conn.execute(f"""
        SELECT a.DATA_AUTO, a.SITUACAO, a.MUNICIPIO, a.TIPO_AUTO,
               a.VALOR_TOTAL_MULTA, a.NUMERO_AUTO_INFRACAO,
               a.DESCRICAO_OCORRENCIA, a.FONTE, a.ATIVIDADE
        FROM sema_autos_infracao a {ai_join} WHERE {ai_where}
    """, ai_params).fetchall()

    total_autos = len(ai_rows)
    municipios = set()
    datas = []
    total_valor = 0.0
    active_autos = []

    for r in ai_rows:
        dat, sit, mun, tipo, valor, num, desc, fonte, ativ = r
        if mun:
            municipios.add(mun)
        if dat:
            datas.append(dat)
        try:
            v = float(valor) if valor else 0.0
        except (ValueError, TypeError):
            v = 0.0
        total_valor += v
        active_autos.append({
            'DATA_AUTO': dat, 'SITUACAO': sit, 'MUNICIPIO': mun,
            'TIPO_AUTO': tipo, 'VALOR_TOTAL_MULTA': valor,
            'NUMERO_AUTO_INFRACAO': num, 'DESCRICAO_OCORRENCIA': desc,
            'FONTE': fonte, 'ATIVIDADE': ativ,
        })

    ai_summary = {
        'total_autos': total_autos,
        'municipios': ','.join(sorted(municipios)) if municipios else None,
        'primeiro_auto': min(datas) if datas else None,
        'ultimo_auto': max(datas) if datas else None,
        'valor_total': fmt_valor(total_valor),
        'valor_total_float': total_valor,
    }

    active_autos.sort(key=lambda r: r.get('DATA_AUTO', ''), reverse=True)
    recent_ai = active_autos[:10]

    # Build FTS joins for embargos
    te_joins = []
    te_conditions = []
    te_params = []

    if nome:
        norm = strip_accents(nome).upper()
        match_expr = _fts_match_expr(norm)
        if match_expr:
            te_joins.append('JOIN fts_sema_te_nome fn ON t.rowid = fn.rowid')
            te_conditions.append('fn.fts_sema_te_nome MATCH ?')
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

    te_rows = conn.execute(f"""
        SELECT t.NUMERO_TERMO_EMBARGO, t.DATA_EMBARGO, t.MUNICIPIO,
               t.PROPRIEDADE, t.AREA_HA, t.DESCRICAO_DANO,
               t.SITUACAO, t.FONTE, t.VALOR_TOTAL_MULTA
        FROM sema_areas_embargadas t {te_join} WHERE {te_where}
    """, te_params).fetchall()

    total_embargos = len(te_rows)
    active_embargos = []

    for r in te_rows:
        num_te, dat, mun, prop, area, dano, sit, fonte, valor = r
        active_embargos.append({
            'NUMERO_TERMO_EMBARGO': num_te, 'DATA_EMBARGO': dat,
            'MUNICIPIO': mun, 'PROPRIEDADE': prop,
            'AREA_HA': area, 'DESCRICAO_DANO': dano,
            'SITUACAO': sit, 'FONTE': fonte, 'VALOR_TOTAL_MULTA': valor,
        })

    te_summary = {
        'total_embargos': total_embargos,
    }

    active_embargos.sort(key=lambda r: r.get('DATA_EMBARGO', ''), reverse=True)

    conn.close()

    return {
        "autos_infracao": ai_summary,
        "areas_embargadas": te_summary,
        "ultimos_autos": recent_ai,
        "embargos_recentes": active_embargos[:10],
    }


def stats():
    """Database statistics."""
    conn = get_conn()
    result = {}

    for table in ['sema_autos_infracao', 'sema_areas_embargadas']:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        result[table] = count

    # By fonte
    for fonte in ['SIGA', 'LEGADO']:
        ai = conn.execute(
            "SELECT COUNT(*) FROM sema_autos_infracao WHERE FONTE = ?", [fonte]
        ).fetchone()[0]
        te = conn.execute(
            "SELECT COUNT(*) FROM sema_areas_embargadas WHERE FONTE = ?", [fonte]
        ).fetchone()[0]
        result[f'autos_{fonte.lower()}'] = ai
        result[f'embargos_{fonte.lower()}'] = te

    # Top municipios
    top_mun = conn.execute("""
        SELECT MUNICIPIO, COUNT(*) as n FROM sema_autos_infracao
        WHERE MUNICIPIO != ''
        GROUP BY MUNICIPIO ORDER BY n DESC LIMIT 10
    """).fetchall()
    result['top_municipios'] = [(r[0], r[1]) for r in top_mun]

    # Year range
    year_range = conn.execute("""
        SELECT MIN(DATA_AUTO), MAX(DATA_AUTO)
        FROM sema_autos_infracao WHERE DATA_AUTO != ''
    """).fetchone()
    result['period'] = (year_range[0], year_range[1])

    db_size = os.path.getsize(DB_PATH)
    result['db_size_mb'] = round(db_size / 1024 / 1024, 1)

    conn.close()
    return result


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 consulta_sema.py <command> [args]")
        print("Commands: stats, search_ai, search_te, resumo")
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
            if key == 'limit':
                kwargs[key] = int(val)
            else:
                kwargs[key] = val
            i += 2
        result = search_embargos(**kwargs)
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
