#!/usr/bin/env python3
"""
SEMA-MT Query Tool v2 - Search all 4 SEMA tables.
Tables: sema_autos_infracao, sema_outros_termos, sema_embargos, sema_desembargos
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
    conn.execute("PRAGMA cache_size=-8000")  # 8MB read cache (reduced for 512MB Render)
    conn.execute("PRAGMA mmap_size=67108864")  # 64MB memory-mapped I/O
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn


def fmt_valor(v):
    if v == 0:
        return 'N/A'
    return f"R$ {v:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')


def _fts_match_expr(terms):
    words = terms.strip().split()
    return ' '.join(f'"{w}"' for w in words if w)


def _strip_internal(results):
    """Remove internal normalized columns from results."""
    for r in results:
        r.pop('NOME_NORM', None)
        r.pop('MUNICIPIO_NORM', None)
        r.pop('CPF_CNPJ_NORM', None)
        r.pop('NUM_PROCESSO_NORM', None)


# ============================================================
# GENERIC SEARCH (shared by all 4 tables)
# ============================================================

def _generic_search(table_name, fts_prefix, nome=None, cpf_cnpj=None,
                    municipio=None, num_doc=None, num_processo=None,
                    num_auto=None, fonte=None,
                    ano_inicio=None, ano_fim=None, limit=50):
    """Generic search function for any SEMA table."""
    conn = get_conn()
    joins = []
    conditions = []
    params = []

    if nome:
        norm = strip_accents(nome).upper()
        match_expr = _fts_match_expr(norm)
        if match_expr:
            fts_table = f'fts_{fts_prefix}_nome'
            joins.append(f'JOIN {fts_table} fn ON a.rowid = fn.rowid')
            conditions.append(f'fn.{fts_table} MATCH ?')
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
            fts_table = f'fts_{fts_prefix}_mun'
            joins.append(f'JOIN {fts_table} fm ON a.rowid = fm.rowid')
            conditions.append(f'fm.{fts_table} MATCH ?')
            params.append(match_expr)

    if num_doc:
        conditions.append('a.NUMERO_DOCUMENTO = ?')
        params.append(num_doc)
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
        conditions.append("a.DATA_DOCUMENTO >= ?")
        params.append(f"{ano_inicio}-01-01")
    if ano_fim:
        conditions.append("a.DATA_DOCUMENTO <= ?")
        params.append(f"{ano_fim}-12-31")

    has_fts = bool(joins)
    join_clause = "\n        ".join(joins) if joins else ""
    where = " AND ".join(conditions) if conditions else "1=1"

    if has_fts:
        max_fetch = max(limit + 1, 5000)
        sql = f"""
            SELECT a.* FROM {table_name} a
            {join_clause}
            WHERE {where}
            LIMIT ?
        """
        params.append(max_fetch)
        rows = conn.execute(sql, params).fetchall()
        results = [dict(r) for r in rows]
        results.sort(key=lambda r: r.get('DATA_DOCUMENTO', ''), reverse=True)
        has_more = len(results) > limit
        results = results[:limit]
    else:
        fetch_limit = limit + 1
        sql = f"""
            SELECT a.* FROM {table_name} a
            WHERE {where}
            ORDER BY a.DATA_DOCUMENTO DESC
            LIMIT ?
        """
        params.append(fetch_limit)
        rows = conn.execute(sql, params).fetchall()
        has_more = len(rows) > limit
        results = [dict(r) for r in rows[:limit]]

    _strip_internal(results)
    conn.close()
    showing = len(results)
    total = f"{showing}+" if has_more else str(showing)
    return {"total": total, "showing": showing, "results": results}


# ============================================================
# PUBLIC SEARCH FUNCTIONS
# ============================================================

def search_autos(nome=None, cpf_cnpj=None, municipio=None,
                 num_auto=None, num_processo=None, fonte=None,
                 ano_inicio=None, ano_fim=None, limit=50):
    """Search SEMA-MT autos de infracao."""
    return _generic_search('sema_autos_infracao', 'sai',
                          nome=nome, cpf_cnpj=cpf_cnpj, municipio=municipio,
                          num_doc=num_auto, num_processo=num_processo,
                          fonte=fonte, ano_inicio=ano_inicio, ano_fim=ano_fim,
                          limit=limit)


def search_termos(nome=None, cpf_cnpj=None, municipio=None,
                  num_doc=None, num_processo=None, fonte=None,
                  ano_inicio=None, ano_fim=None, limit=50):
    """Search SEMA-MT outros termos (inspecao, notificacao, apreensao, etc.)."""
    return _generic_search('sema_outros_termos', 'sot',
                          nome=nome, cpf_cnpj=cpf_cnpj, municipio=municipio,
                          num_doc=num_doc, num_processo=num_processo,
                          fonte=fonte, ano_inicio=ano_inicio, ano_fim=ano_fim,
                          limit=limit)


def search_embargos(nome=None, cpf_cnpj=None, municipio=None,
                    num_embargo=None, num_processo=None, num_auto=None,
                    fonte=None, limit=50):
    """Search SEMA-MT areas embargadas."""
    return _generic_search('sema_embargos', 'sem',
                          nome=nome, cpf_cnpj=cpf_cnpj, municipio=municipio,
                          num_doc=num_embargo, num_processo=num_processo,
                          num_auto=num_auto, fonte=fonte,
                          limit=limit)


def search_desembargos(nome=None, cpf_cnpj=None, municipio=None,
                       num_doc=None, num_processo=None, num_auto=None,
                       fonte=None, limit=50):
    """Search SEMA-MT areas desembargadas."""
    return _generic_search('sema_desembargos', 'sde',
                          nome=nome, cpf_cnpj=cpf_cnpj, municipio=municipio,
                          num_doc=num_doc, num_processo=num_processo,
                          num_auto=num_auto, fonte=fonte,
                          limit=limit)


# ============================================================
# RESUMO (profile across all 4 tables)
# ============================================================

def resumo_autuado(nome=None, cpf_cnpj=None):
    """Full profile: all SEMA tables summary."""
    conn = get_conn()

    tables_config = [
        ('sema_autos_infracao', 'sai'),
        ('sema_outros_termos', 'sot'),
        ('sema_embargos', 'sem'),
        ('sema_desembargos', 'sde'),
    ]

    all_results = {}

    for table_name, prefix in tables_config:
        joins = []
        conditions = []
        params = []

        if nome:
            norm = strip_accents(nome).upper()
            match_expr = _fts_match_expr(norm)
            if match_expr:
                fts_table = f'fts_{prefix}_nome'
                joins.append(f'JOIN {fts_table} fn ON a.rowid = fn.rowid')
                conditions.append(f'fn.{fts_table} MATCH ?')
                params.append(match_expr)
        if cpf_cnpj:
            clean = re.sub(r'[^0-9]', '', cpf_cnpj)
            if len(clean) >= 11:
                conditions.append('a.CPF_CNPJ_NORM = ?')
            else:
                conditions.append('a.CPF_CNPJ_NORM LIKE ?')
                clean = f'%{clean}%'
            params.append(clean)

        join_clause = " ".join(joins)
        where_clause = " AND ".join(conditions) if conditions else "1=1"

        rows = conn.execute(f"""
            SELECT a.DATA_DOCUMENTO, a.SITUACAO, a.MUNICIPIO, a.TIPO_DOCUMENTO,
                   a.VALOR_TOTAL_MULTA, a.NUMERO_DOCUMENTO, a.NUMERO_AUTO_INFRACAO,
                   a.DESCRICAO_OCORRENCIA, a.FONTE, a.ATIVIDADE,
                   a.PROPRIEDADE, a.AREA_HA, a.DESCRICAO_DANO,
                   a.NUMERO_TERMO_EMBARGO, a.NUMERO_PROCESSO
            FROM {table_name} a {join_clause} WHERE {where_clause}
        """, params).fetchall()

        total = len(rows)
        municipios = set()
        datas = []
        total_valor = 0.0
        items = []

        for r in rows:
            dat, sit, mun, tipo, valor, num_doc, num_ai = r[0], r[1], r[2], r[3], r[4], r[5], r[6]
            desc, fonte, ativ = r[7], r[8], r[9]
            prop, area, dano, num_te, proc = r[10], r[11], r[12], r[13], r[14]

            if mun:
                municipios.add(mun)
            if dat:
                datas.append(dat)
            try:
                v = float(valor) if valor else 0.0
            except (ValueError, TypeError):
                v = 0.0
            total_valor += v

            items.append({
                'DATA_DOCUMENTO': dat, 'SITUACAO': sit, 'MUNICIPIO': mun,
                'TIPO_DOCUMENTO': tipo, 'VALOR_TOTAL_MULTA': valor,
                'NUMERO_DOCUMENTO': num_doc, 'NUMERO_AUTO_INFRACAO': num_ai,
                'DESCRICAO_OCORRENCIA': desc, 'FONTE': fonte, 'ATIVIDADE': ativ,
                'PROPRIEDADE': prop, 'AREA_HA': area, 'DESCRICAO_DANO': dano,
                'NUMERO_TERMO_EMBARGO': num_te, 'NUMERO_PROCESSO': proc,
            })

        items.sort(key=lambda r: r.get('DATA_DOCUMENTO', ''), reverse=True)

        all_results[table_name] = {
            'total': total,
            'municipios': ','.join(sorted(municipios)) if municipios else None,
            'primeiro': min(datas) if datas else None,
            'ultimo': max(datas) if datas else None,
            'valor_total': fmt_valor(total_valor),
            'valor_total_float': total_valor,
            'recentes': items,
        }

    conn.close()

    return {
        "autos_infracao": all_results.get('sema_autos_infracao', {}),
        "outros_termos": all_results.get('sema_outros_termos', {}),
        "embargos": all_results.get('sema_embargos', {}),
        "desembargos": all_results.get('sema_desembargos', {}),
    }


# ============================================================
# STATS
# ============================================================

def stats():
    """Database statistics."""
    conn = get_conn()
    result = {}

    tables = ['sema_autos_infracao', 'sema_outros_termos', 'sema_embargos', 'sema_desembargos']
    for table in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            result[table] = count
        except Exception:
            result[table] = 0

    # By fonte
    for fonte in ['SIGA', 'LEGADO', 'SIGA_TERMOS', 'DESCENTRALIZADO']:
        try:
            ai = conn.execute(
                "SELECT COUNT(*) FROM sema_autos_infracao WHERE FONTE = ?", [fonte]
            ).fetchone()[0]
            result[f'autos_{fonte.lower()}'] = ai
        except Exception:
            pass

    # Top municipios (across autos + embargos)
    try:
        top_mun = conn.execute("""
            SELECT MUNICIPIO, COUNT(*) as n FROM (
                SELECT MUNICIPIO FROM sema_autos_infracao WHERE MUNICIPIO != ''
                UNION ALL
                SELECT MUNICIPIO FROM sema_embargos WHERE MUNICIPIO != ''
            )
            GROUP BY MUNICIPIO ORDER BY n DESC LIMIT 10
        """).fetchall()
        result['top_municipios'] = [(r[0], r[1]) for r in top_mun]
    except Exception:
        result['top_municipios'] = []

    # Year range
    try:
        year_range = conn.execute("""
            SELECT MIN(DATA_DOCUMENTO), MAX(DATA_DOCUMENTO)
            FROM sema_autos_infracao WHERE DATA_DOCUMENTO != ''
        """).fetchone()
        result['period'] = (year_range[0], year_range[1])
    except Exception:
        result['period'] = (None, None)

    try:
        db_size = os.path.getsize(DB_PATH)
        result['db_size_mb'] = round(db_size / 1024 / 1024, 1)
    except Exception:
        result['db_size_mb'] = 0

    conn.close()
    return result


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 consulta_sema.py <command> [args]")
        print("Commands: stats, search_ai, search_te, search_ot, search_de, resumo")
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

    elif cmd == "search_ot":
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
        result = search_termos(**kwargs)
        print(json.dumps(result, indent=2, ensure_ascii=False))

    elif cmd == "search_de":
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
        result = search_desembargos(**kwargs)
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
