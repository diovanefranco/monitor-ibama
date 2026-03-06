#!/usr/bin/env python3
"""
IBAMA Data Loader - Extracts JSON/XML from ZIPs and loads into SQLite.
Builds into a temp file and atomically replaces the DB to avoid corruption.
"""
import os, sys, sqlite3, zipfile, json, xml.etree.ElementTree as ET, tempfile, shutil, unicodedata, re
from datetime import datetime

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PARENT_DIR = os.path.dirname(_SCRIPT_DIR)

DB_PATH = os.path.join(_SCRIPT_DIR, "ibama.db")
DATA_DIR = _PARENT_DIR

# Key fields for auto_infracao
AI_FIELDS = [
    "SEQ_AUTO_INFRACAO", "NUM_AUTO_INFRACAO", "SER_AUTO_INFRACAO",
    "DAT_HORA_AUTO_INFRACAO", "DT_LANCAMENTO", "DT_FATO_INFRACIONAL",
    "NOME_INFRATOR", "CPF_CNPJ_INFRATOR", "TP_PESSOA_INFRATOR", "NUM_PESSOA_INFRATOR",
    "VAL_AUTO_INFRACAO", "TIPO_MULTA", "TIPO_AUTO", "TIPO_INFRACAO",
    "UF", "MUNICIPIO", "COD_MUNICIPIO",
    "NUM_PROCESSO", "NU_PROCESSO_FORMATADO",
    "DES_AUTO_INFRACAO", "DES_INFRACAO", "DES_LOCAL_INFRACAO",
    "GRAVIDADE_INFRACAO", "CD_NIVEL_GRAVIDADE",
    "DS_ENQUADRAMENTO_ADMINISTRATIVO", "DS_ENQUADRAMENTO_NAO_ADMINISTRATIVO",
    "DS_ENQUADRAMENTO_COMPLEMENTAR",
    "CLASSIFICACAO_AREA", "UNIDADE_CONSERVACAO", "DS_BIOMAS_ATINGIDOS",
    "NUM_LATITUDE_AUTO", "NUM_LONGITUDE_AUTO",
    "OPERACAO", "ORDEM_FISCALIZACAO",
    "CD_TERMOS_EMBARGOS", "CD_TERMOS_APREENSAO",
    "DES_STATUS_FORMULARIO", "SIT_CANCELADO",
    "FUNDAMENTACAO_MULTA", "MOTIVACAO_CONDUTA",
    "EFEITO_MEIO_AMBIENTE", "EFEITO_SAUDE_PUBLICA",
    "SOLICITACAO_RECURSO", "OPERACAO_SOL_RECURSO",
    "ULTIMA_ATUALIZACAO_RELATORIO",
    "UNID_CONTROLE", "UNID_ORDENADORA", "UNID_ARRECADACAO",
    "QT_AREA", "INFRACAO_AREA",
    "DES_RECEITA", "CD_RECEITA_AUTO_INFRACAO",
    "PASSIVEL_RECUPERACAO",
]

TE_FIELDS = [
    "SEQ_TAD", "NUM_TAD", "SER_TAD",
    "DAT_EMBARGO", "DAT_DESEMBARGO",
    "NOME_EMBARGADO", "CPF_CNPJ_EMBARGADO", "NUM_PESSOA_EMBARGO",
    "NUM_PROCESSO",
    "DES_TAD", "DES_LOCALIZACAO",
    "UF", "MUNICIPIO", "COD_MUNICIPIO",
    "NUM_LATITUDE_TAD", "NUM_LONGITUDE_TAD",
    "QTD_AREA_EMBARGADA", "TIPO_AREA",
    "NUM_AUTO_INFRACAO", "SEQ_AUTO_INFRACAO",
    "SIT_DESEMBARGO", "TIPO_DESEMBARGO", "DES_DESEMBARGO",
    "DES_STATUS_FORMULARIO", "SIT_CANCELADO",
    "OPERACAO", "ORDEM_FISCALIZACAO",
    "UNID_APRESENTACAO", "UNID_CONTROLE",
    "SOLICITACAO_RECURSO", "OPERACAO_SOL_RECURSO",
    "ULTIMA_ATUALIZACAO_RELATORIO",
    "NOME_IMOVEL", "DETER_PRODES",
]


def strip_accents(s):
    """Remove accents/diacritics from string (ã→a, é→e, etc.)."""
    if not s:
        return ''
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )


def digits_only(s):
    """Extract only digits from string."""
    if not s:
        return ''
    return re.sub(r'[^0-9]', '', s)


def create_tables(conn):
    """Create tables with proper schema + pre-normalized search columns."""
    cols_ai = ", ".join([f'"{f}" TEXT' for f in AI_FIELDS])
    conn.execute(f'''CREATE TABLE autos_infracao (
        {cols_ai},
        NOME_INFRATOR_NORM TEXT,
        MUNICIPIO_NORM TEXT,
        CPF_CNPJ_NORM TEXT,
        NUM_PROCESSO_NORM TEXT
    )''')

    cols_te = ", ".join([f'"{f}" TEXT' for f in TE_FIELDS])
    conn.execute(f'''CREATE TABLE termos_embargo (
        {cols_te},
        NOME_EMBARGADO_NORM TEXT,
        MUNICIPIO_NORM TEXT,
        CPF_CNPJ_NORM TEXT,
        NUM_PROCESSO_NORM TEXT
    )''')

    conn.commit()


def create_indexes(conn):
    """Create indexes for fast searching."""
    indexes = [
        # Autos de infracao - normalized columns for fast search
        ("idx_ai_nome_norm", "autos_infracao", "NOME_INFRATOR_NORM"),
        ("idx_ai_cpf_norm", "autos_infracao", "CPF_CNPJ_NORM"),
        ("idx_ai_proc_norm", "autos_infracao", "NUM_PROCESSO_NORM"),
        ("idx_ai_mun_norm", "autos_infracao", "MUNICIPIO_NORM"),
        ("idx_ai_uf", "autos_infracao", "UF"),
        ("idx_ai_uf_mun", "autos_infracao", "UF, MUNICIPIO_NORM"),
        ("idx_ai_num", "autos_infracao", "NUM_AUTO_INFRACAO"),
        ("idx_ai_data", "autos_infracao", "DAT_HORA_AUTO_INFRACAO"),
        ("idx_ai_tipo", "autos_infracao", "TIPO_INFRACAO"),
        ("idx_ai_cancelado", "autos_infracao", "SIT_CANCELADO"),
        # Termos de embargo - normalized columns for fast search
        ("idx_te_nome_norm", "termos_embargo", "NOME_EMBARGADO_NORM"),
        ("idx_te_cpf_norm", "termos_embargo", "CPF_CNPJ_NORM"),
        ("idx_te_proc_norm", "termos_embargo", "NUM_PROCESSO_NORM"),
        ("idx_te_mun_norm", "termos_embargo", "MUNICIPIO_NORM"),
        ("idx_te_uf", "termos_embargo", "UF"),
        ("idx_te_uf_mun", "termos_embargo", "UF, MUNICIPIO_NORM"),
        ("idx_te_num", "termos_embargo", "NUM_TAD"),
        ("idx_te_data", "termos_embargo", "DAT_EMBARGO"),
        ("idx_te_ai", "termos_embargo", "NUM_AUTO_INFRACAO"),
        ("idx_te_cancelado", "termos_embargo", "SIT_CANCELADO"),
    ]
    for name, table, cols in indexes:
        conn.execute(f'CREATE INDEX IF NOT EXISTS {name} ON {table} ({cols})')
    conn.commit()


def create_fts(conn):
    """Create FTS5 virtual tables for full-text search on descriptions."""
    # FTS for autos de infracao descriptions
    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS fts_autos USING fts5(
            SEQ_AUTO_INFRACAO,
            NOME_INFRATOR,
            DES_AUTO_INFRACAO,
            DES_INFRACAO,
            DES_LOCAL_INFRACAO,
            DS_ENQUADRAMENTO_ADMINISTRATIVO,
            DS_ENQUADRAMENTO_NAO_ADMINISTRATIVO,
            OPERACAO,
            content='autos_infracao',
            content_rowid='rowid'
        )
    """)
    conn.execute("""
        INSERT INTO fts_autos(fts_autos) VALUES('rebuild')
    """)

    # FTS for termos de embargo descriptions
    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS fts_embargos USING fts5(
            SEQ_TAD,
            NOME_EMBARGADO,
            DES_TAD,
            DES_LOCALIZACAO,
            NOME_IMOVEL,
            OPERACAO,
            content='termos_embargo',
            content_rowid='rowid'
        )
    """)
    conn.execute("""
        INSERT INTO fts_embargos(fts_embargos) VALUES('rebuild')
    """)

    # FTS for fast name search on normalized names
    print("  Creating FTS name indexes...")
    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS fts_ai_nome USING fts5(
            NOME_INFRATOR_NORM,
            content='autos_infracao',
            content_rowid='rowid'
        )
    """)
    conn.execute("INSERT INTO fts_ai_nome(fts_ai_nome) VALUES('rebuild')")

    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS fts_te_nome USING fts5(
            NOME_EMBARGADO_NORM,
            content='termos_embargo',
            content_rowid='rowid'
        )
    """)
    conn.execute("INSERT INTO fts_te_nome(fts_te_nome) VALUES('rebuild')")

    # FTS for fast municipio search
    print("  Creating FTS municipio indexes...")
    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS fts_ai_mun USING fts5(
            MUNICIPIO_NORM,
            content='autos_infracao',
            content_rowid='rowid'
        )
    """)
    conn.execute("INSERT INTO fts_ai_mun(fts_ai_mun) VALUES('rebuild')")

    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS fts_te_mun USING fts5(
            MUNICIPIO_NORM,
            content='termos_embargo',
            content_rowid='rowid'
        )
    """)
    conn.execute("INSERT INTO fts_te_mun(fts_te_mun) VALUES('rebuild')")

    conn.commit()


def load_auto_infracao(conn):
    """Load auto_infracao from JSON ZIP."""
    zip_path = os.path.join(DATA_DIR, "auto_infracao_json.zip")
    if not os.path.exists(zip_path):
        print(f"SKIP: {zip_path} not found")
        return 0

    total = 0
    parse_errors = 0
    all_cols = AI_FIELDS + ['NOME_INFRATOR_NORM', 'MUNICIPIO_NORM', 'CPF_CNPJ_NORM', 'NUM_PROCESSO_NORM']
    placeholders = ", ".join(["?" for _ in all_cols])
    cols = ", ".join([f'"{f}"' for f in all_cols])
    sql = f'INSERT INTO autos_infracao ({cols}) VALUES ({placeholders})'

    with zipfile.ZipFile(zip_path) as zf:
        json_files = sorted([f for f in zf.namelist() if f.endswith('.json')])
        for jf in json_files:
            print(f"  Loading {jf}...", end=" ", flush=True)
            with zf.open(jf) as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError as e:
                    print(f"ERRO JSON: {e}")
                    parse_errors += 1
                    continue

                records = data.get('data', data) if isinstance(data, dict) else data

            rows = []
            for rec in records:
                vals = [str(rec.get(field, '') or '') for field in AI_FIELDS]
                nome = rec.get('NOME_INFRATOR', '') or ''
                mun = rec.get('MUNICIPIO', '') or ''
                cpf = rec.get('CPF_CNPJ_INFRATOR', '') or ''
                proc = rec.get('NUM_PROCESSO', '') or ''
                vals.extend([
                    strip_accents(nome).upper(),
                    strip_accents(mun).upper(),
                    digits_only(cpf),
                    digits_only(proc),
                ])
                rows.append(tuple(vals))

            conn.executemany(sql, rows)
            conn.commit()
            count = len(rows)
            total += count
            print(f"{count:,} records")

    if parse_errors:
        print(f"  AVISO: {parse_errors} arquivo(s) com erro de parse")
    return total


def load_termo_embargo(conn):
    """Load termo_embargo from XML ZIP."""
    zip_path = os.path.join(DATA_DIR, "termo_embargo_xml.zip")
    if not os.path.exists(zip_path):
        print(f"SKIP: {zip_path} not found")
        return 0

    total = 0
    parse_errors = 0
    all_cols = TE_FIELDS + ['NOME_EMBARGADO_NORM', 'MUNICIPIO_NORM', 'CPF_CNPJ_NORM', 'NUM_PROCESSO_NORM']
    placeholders = ", ".join(["?" for _ in all_cols])
    cols = ", ".join([f'"{f}"' for f in all_cols])
    sql = f'INSERT INTO termos_embargo ({cols}) VALUES ({placeholders})'

    with zipfile.ZipFile(zip_path) as zf:
        xml_files = [f for f in zf.namelist() if f.endswith('.xml')]
        for xf in xml_files:
            print(f"  Loading {xf}...", flush=True)
            with zf.open(xf) as f:
                batch = []
                try:
                    context = ET.iterparse(f, events=('end',))
                    for event, elem in context:
                        if elem.tag == 'itemRelatorio':
                            rec = {}
                            for child in elem:
                                rec[child.tag] = (child.text or '').strip()
                            vals = [rec.get(field, '') for field in TE_FIELDS]
                            nome = rec.get('NOME_EMBARGADO', '') or ''
                            mun = rec.get('MUNICIPIO', '') or ''
                            cpf = rec.get('CPF_CNPJ_EMBARGADO', '') or ''
                            proc = rec.get('NUM_PROCESSO', '') or ''
                            vals.extend([
                                strip_accents(nome).upper(),
                                strip_accents(mun).upper(),
                                digits_only(cpf),
                                digits_only(proc),
                            ])
                            batch.append(tuple(vals))

                            if len(batch) >= 10000:
                                conn.executemany(sql, batch)
                                total += len(batch)
                                print(f"    {total:,} records...", flush=True)
                                batch = []

                            elem.clear()
                except ET.ParseError as e:
                    print(f"  ERRO XML em {xf}: {e}")
                    parse_errors += 1

                if batch:
                    conn.executemany(sql, batch)
                    total += len(batch)

                conn.commit()

    if parse_errors:
        print(f"  AVISO: {parse_errors} arquivo(s) com erro de parse")
    return total


def main():
    print(f"=== IBAMA Data Loader ===")
    print(f"DB: {DB_PATH}")
    print(f"Data: {DATA_DIR}")
    print()

    # Build into a temp file, then atomically replace
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".db", dir=_SCRIPT_DIR)
    os.close(tmp_fd)

    try:
        conn = sqlite3.connect(tmp_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-64000")  # 64MB cache (safe for Render free tier)

        print("Creating tables...")
        create_tables(conn)

        print("\n[1/2] Loading Autos de Infracao (JSON)...")
        t0 = datetime.now()
        ai_count = load_auto_infracao(conn)
        t1 = datetime.now()
        print(f"  Total: {ai_count:,} records in {(t1-t0).seconds}s")

        print("\n[2/2] Loading Termos de Embargo (XML)...")
        t0 = datetime.now()
        te_count = load_termo_embargo(conn)
        t1 = datetime.now()
        print(f"  Total: {te_count:,} records in {(t1-t0).seconds}s")

        print("\nCreating indexes...")
        create_indexes(conn)

        print("Creating FTS indexes...")
        create_fts(conn)

        # Stats
        print("\n=== SUMMARY ===")
        for table in ['autos_infracao', 'termos_embargo']:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count:,} records")

        conn.close()

        # Atomic replace
        db_size = os.path.getsize(tmp_path)
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
        shutil.move(tmp_path, DB_PATH)

        print(f"  DB size: {db_size/1024/1024:.1f}MB")
        print(f"\nDone! DB ready at {DB_PATH}")

    except Exception as e:
        # Clean up temp file on failure
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        print(f"\nERRO FATAL: {e}")
        raise


if __name__ == "__main__":
    main()
