#!/usr/bin/env python3
"""
SEMA-MT Data Loader - Reads GeoJSON files from WFS downloads and loads into SQLite.
Merges two sources for Autos de Infracao and two sources for Areas Embargadas
into unified tables with a common schema.
"""
import os, sys, sqlite3, json, tempfile, shutil, unicodedata, re
from datetime import datetime

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(_SCRIPT_DIR, "sema.db")
DATA_DIR = _SCRIPT_DIR

# ============================================================
# UNIFIED SCHEMA - Autos de Infracao
# Merges SIGA (rich) + Legado MVW_TIT_AUTUACAO (simpler)
# ============================================================
SEMA_AI_FIELDS = [
    "FONTE",                    # 'SIGA' or 'LEGADO' - identifies origin
    "ID_ORIGINAL",              # Original ID from source
    "NUMERO_AUTO_INFRACAO",
    "NUMERO_PROCESSO",
    "DATA_AUTO",                # Unified date field (ISO format)
    "NOME_RAZAO_SOCIAL",
    "CPF_CNPJ",
    "MUNICIPIO",
    "TIPO_AUTO",                # 'Auto de Infracao', etc.
    "SUBTIPO",
    "SITUACAO",
    "VALOR_TOTAL_MULTA",        # Numeric value
    "DISPOSITIVO_LEGAL",
    "DESCRICAO_OCORRENCIA",
    "LATITUDE",
    "LONGITUDE",
    # SIGA-only fields (empty for legado)
    "NOME_FANTASIA",
    "FRENTE",
    "ATIVIDADE",
    "EMBARGADO",
    "NUMERO_TERMO_EMBARGO",
    "NUMERO_RELATORIO_TECNICO",
    "MATRICULA_TECNICO",
    "QUANTIDADE",
    "UNIDADE_MEDIDA",
    "VALOR_UNIDADE_MULTA",
    "DESCRICAO_AGRAVANTE",
    "PERCENTUAL_AGRAVANTE",
    # Legado-only fields (empty for SIGA)
    "SETOR",
    "AUTOR",
    "AREA_DESMATADA",
]

# ============================================================
# UNIFIED SCHEMA - Areas Embargadas
# Merges SIGA Ponto + Legado AREAS_EMBARGADAS_SEMA
# ============================================================
SEMA_TE_FIELDS = [
    "FONTE",                    # 'SIGA' or 'LEGADO'
    "ID_ORIGINAL",
    "NUMERO_TERMO_EMBARGO",
    "NUMERO_AUTO_INFRACAO",
    "NUMERO_PROCESSO",
    "DATA_EMBARGO",             # Unified date (ISO format)
    "NOME_RAZAO_SOCIAL",
    "CPF_CNPJ",
    "MUNICIPIO",
    "PROPRIEDADE",
    "DESCRICAO_DANO",
    "AREA_HA",
    "LATITUDE",
    "LONGITUDE",
    "SITUACAO",
    "VALOR_TOTAL_MULTA",
    # SIGA-only
    "TIPO_AUTO",
    "SUBTIPO",
    "FRENTE",
    "ATIVIDADE",
    "ATIVIDADE_EMBARGADA",
    "DISPOSITIVO_LEGAL",
    "NUMERO_RELATORIO_TECNICO",
    "MATRICULA_TECNICO",
    "QUANTIDADE",
    "UNIDADE_MEDIDA",
    "VALOR_UNIDADE_MULTA",
    "EMBARGADO",
    # Legado-only
    "ANO_DESMATAMENTO",
    "FONTE_DETECCAO",
    "OBSERVACAO",
]


def strip_accents(s):
    if not s:
        return ''
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )


def digits_only(s):
    if not s:
        return ''
    return re.sub(r'[^0-9]', '', s)


def parse_dms_to_decimal(dms_str):
    """Convert DMS string like '-16:15:43,70' to decimal degrees."""
    if not dms_str:
        return None
    try:
        # Handle format: -16:15:43,70 or 16:15:43,70
        s = dms_str.strip().replace(',', '.')
        negative = s.startswith('-')
        s = s.lstrip('-')
        parts = s.split(':')
        if len(parts) == 3:
            d, m, sec = float(parts[0]), float(parts[1]), float(parts[2])
            decimal = d + m / 60.0 + sec / 3600.0
            return -decimal if negative else decimal
    except (ValueError, IndexError):
        pass
    # Try direct float parse
    try:
        return float(dms_str.replace(',', '.'))
    except ValueError:
        return None


def parse_dms_legacy(coord_str):
    """Parse legacy DMS format like '51 31' 40,728\" W' to decimal."""
    if not coord_str or not coord_str.strip():
        return None
    try:
        s = coord_str.strip()
        # Detect direction
        direction = 1
        if s.endswith(('W', 'S', 'O')):
            direction = -1
            s = s[:-1].strip()
        elif s.endswith(('E', 'N')):
            s = s[:-1].strip()

        # Remove degree/minute/second symbols
        s = s.replace('°', ' ').replace("'", ' ').replace('"', ' ').replace(',', '.')
        parts = s.split()
        if len(parts) >= 3:
            d, m, sec = float(parts[0]), float(parts[1]), float(parts[2])
            return direction * (d + m / 60.0 + sec / 3600.0)
        elif len(parts) == 2:
            d, m = float(parts[0]), float(parts[1])
            return direction * (d + m / 60.0)
    except (ValueError, IndexError):
        pass
    return None


def normalize_date(date_str):
    """Normalize various date formats to ISO YYYY-MM-DD."""
    if not date_str or not date_str.strip():
        return ''
    s = date_str.strip()

    # ISO format: 2026-02-23T04:00:00Z
    if 'T' in s:
        return s[:10]

    # BR format: 13/12/2022
    if '/' in s:
        parts = s.split('/')
        if len(parts) == 3:
            d, m, y = parts
            if len(y) == 4:
                return f"{y}-{m.zfill(2)}-{d.zfill(2)}"

    return s


def load_geojson(filepath):
    """Load GeoJSON file and return features list."""
    if not os.path.exists(filepath):
        print(f"  SKIP: {filepath} not found")
        return []
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    features = data.get('features', [])
    print(f"  Loaded {len(features)} features from {os.path.basename(filepath)}")
    return features


def extract_coords_from_geometry(geometry):
    """Extract lat/lon from GeoJSON geometry."""
    if not geometry:
        return None, None
    coords = geometry.get('coordinates')
    if not coords:
        return None, None
    gtype = geometry.get('type', '')
    try:
        if gtype in ('Point',):
            return coords[1], coords[0]  # lat, lon
        elif gtype in ('MultiPoint',):
            return coords[0][1], coords[0][0]
        elif gtype in ('Polygon',):
            # Centroid approximation (first ring average)
            ring = coords[0]
            lat = sum(p[1] for p in ring) / len(ring)
            lon = sum(p[0] for p in ring) / len(ring)
            return lat, lon
        elif gtype in ('MultiPolygon',):
            ring = coords[0][0]
            lat = sum(p[1] for p in ring) / len(ring)
            lon = sum(p[0] for p in ring) / len(ring)
            return lat, lon
    except (IndexError, TypeError):
        pass
    return None, None


# ============================================================
# TRANSFORM FUNCTIONS - Convert source records to unified schema
# ============================================================

def transform_ai_siga(feature):
    """Transform SIGA Auto de Infracao feature to unified schema."""
    p = feature.get('properties', {})
    geom = feature.get('geometry')
    lat_geom, lon_geom = extract_coords_from_geometry(geom)

    # SIGA has DMS coords as text, also try geometry
    lat = parse_dms_to_decimal(p.get('LATITUDE'))
    lon = parse_dms_to_decimal(p.get('LONGITUDE'))
    if lat is None:
        lat = lat_geom
    if lon is None:
        lon = lon_geom

    return {
        'FONTE': 'SIGA',
        'ID_ORIGINAL': str(p.get('ID_PADRAO', '')),
        'NUMERO_AUTO_INFRACAO': p.get('NUMERO_AUTO_INFRACAO', ''),
        'NUMERO_PROCESSO': p.get('NUMERO_PROCESSO', ''),
        'DATA_AUTO': normalize_date(p.get('DATA_DO_AUTO', '')),
        'NOME_RAZAO_SOCIAL': p.get('NOME_RAZAO_SOCIAL', ''),
        'CPF_CNPJ': p.get('CPFCNPJ', ''),
        'MUNICIPIO': p.get('MUNICIPIO_DO_DANO', ''),
        'TIPO_AUTO': p.get('TIPO_DO_AUTO', ''),
        'SUBTIPO': p.get('SUBTIPO', ''),
        'SITUACAO': p.get('SITUACAO', ''),
        'VALOR_TOTAL_MULTA': str(p.get('VALOR_TOTAL_DA_MULTA', '') or ''),
        'DISPOSITIVO_LEGAL': p.get('DISPOSITIVO_LEGAL_INFRINGIDO', ''),
        'DESCRICAO_OCORRENCIA': p.get('DESCRICAO_DA_OCORRENCIA', ''),
        'LATITUDE': str(lat) if lat else '',
        'LONGITUDE': str(lon) if lon else '',
        'NOME_FANTASIA': p.get('NOME_FANTASIA', ''),
        'FRENTE': p.get('FRENTE', ''),
        'ATIVIDADE': p.get('ATIVIDADE', ''),
        'EMBARGADO': str(p.get('EMBARGADO', '') or ''),
        'NUMERO_TERMO_EMBARGO': p.get('NUMERO_TERMO_EMBARGO', ''),
        'NUMERO_RELATORIO_TECNICO': str(p.get('NUMERO_RELATORIO_TECNICO', '') or ''),
        'MATRICULA_TECNICO': p.get('MATRICULA_TECNICO', ''),
        'QUANTIDADE': str(p.get('QUANTIDADE', '') or ''),
        'UNIDADE_MEDIDA': p.get('UNIDADE_MEDIDA', ''),
        'VALOR_UNIDADE_MULTA': str(p.get('VALOR_DA_UNIDADE_MULTA', '') or ''),
        'DESCRICAO_AGRAVANTE': p.get('DESCRICAO_AGRAVANTE', ''),
        'PERCENTUAL_AGRAVANTE': str(p.get('PERCENTUAL_AGRAVANTE', '') or ''),
        'SETOR': '',
        'AUTOR': '',
        'AREA_DESMATADA': '',
    }


def transform_ai_legado(feature):
    """Transform Legado MVW_TIT_AUTUACAO feature to unified schema."""
    p = feature.get('properties', {})
    geom = feature.get('geometry')
    lat, lon = extract_coords_from_geometry(geom)

    return {
        'FONTE': 'LEGADO',
        'ID_ORIGINAL': str(p.get('OBJECTID', '')),
        'NUMERO_AUTO_INFRACAO': p.get('NUMERO', ''),
        'NUMERO_PROCESSO': p.get('PROCESSO', ''),
        'DATA_AUTO': normalize_date(p.get('DATA_EMISSAO', '')),
        'NOME_RAZAO_SOCIAL': p.get('RAZAO_SOCIAL', ''),
        'CPF_CNPJ': p.get('CPF_CNPJ', ''),
        'MUNICIPIO': p.get('MUNICIPIO', ''),
        'TIPO_AUTO': p.get('MODELO', ''),
        'SUBTIPO': p.get('SUBTIPO', ''),
        'SITUACAO': p.get('SITUACAO', ''),
        'VALOR_TOTAL_MULTA': '',  # Legado nao tem valor
        'DISPOSITIVO_LEGAL': '',
        'DESCRICAO_OCORRENCIA': p.get('NOME', ''),
        'LATITUDE': str(lat) if lat else '',
        'LONGITUDE': str(lon) if lon else '',
        'NOME_FANTASIA': '',
        'FRENTE': '',
        'ATIVIDADE': '',
        'EMBARGADO': '',
        'NUMERO_TERMO_EMBARGO': '',
        'NUMERO_RELATORIO_TECNICO': '',
        'MATRICULA_TECNICO': '',
        'QUANTIDADE': '',
        'UNIDADE_MEDIDA': '',
        'VALOR_UNIDADE_MULTA': '',
        'DESCRICAO_AGRAVANTE': '',
        'PERCENTUAL_AGRAVANTE': '',
        'SETOR': p.get('SETOR', ''),
        'AUTOR': p.get('AUTOR', ''),
        'AREA_DESMATADA': str(p.get('AREA_DESMATADA', '') or ''),
    }


def transform_te_siga(feature):
    """Transform SIGA Area Embargada feature to unified schema."""
    p = feature.get('properties', {})
    geom = feature.get('geometry')
    lat_geom, lon_geom = extract_coords_from_geometry(geom)

    lat = parse_dms_to_decimal(p.get('LATITUDE'))
    lon = parse_dms_to_decimal(p.get('LONGITUDE'))
    if lat is None:
        lat = lat_geom
    if lon is None:
        lon = lon_geom

    return {
        'FONTE': 'SIGA',
        'ID_ORIGINAL': str(p.get('ID_PADRAO', '')),
        'NUMERO_TERMO_EMBARGO': p.get('NUMERO_TERMO_EMBARGO', ''),
        'NUMERO_AUTO_INFRACAO': p.get('NUMERO_AUTO_INFRACAO', ''),
        'NUMERO_PROCESSO': p.get('NUMERO_PROCESSO', ''),
        'DATA_EMBARGO': normalize_date(p.get('DATA_DO_AUTO', '')),
        'NOME_RAZAO_SOCIAL': p.get('NOME_RAZAO_SOCIAL', ''),
        'CPF_CNPJ': p.get('CPFCNPJ', ''),
        'MUNICIPIO': p.get('MUNICIPIO_DO_DANO', ''),
        'PROPRIEDADE': p.get('NOME_FANTASIA', ''),
        'DESCRICAO_DANO': p.get('DESCRICAO_DA_OCORRENCIA', ''),
        'AREA_HA': str(p.get('QUANTIDADE', '') or ''),
        'LATITUDE': str(lat) if lat else '',
        'LONGITUDE': str(lon) if lon else '',
        'SITUACAO': p.get('SITUACAO', ''),
        'VALOR_TOTAL_MULTA': str(p.get('VALOR_TOTAL_DA_MULTA', '') or ''),
        'TIPO_AUTO': p.get('TIPO_DO_AUTO', ''),
        'SUBTIPO': p.get('SUBTIPO', ''),
        'FRENTE': p.get('FRENTE', ''),
        'ATIVIDADE': p.get('ATIVIDADE', ''),
        'ATIVIDADE_EMBARGADA': p.get('ATIVIDADE_EMBARGADA', ''),
        'DISPOSITIVO_LEGAL': p.get('DISPOSITIVO_LEGAL_INFRINGIDO', ''),
        'NUMERO_RELATORIO_TECNICO': str(p.get('NUMERO_RELATORIO_TECNICO', '') or ''),
        'MATRICULA_TECNICO': p.get('MATRICULA_TECNICO', ''),
        'QUANTIDADE': str(p.get('QUANTIDADE', '') or ''),
        'UNIDADE_MEDIDA': p.get('UNIDADE_MEDIDA', ''),
        'VALOR_UNIDADE_MULTA': str(p.get('VALOR_DA_UNIDADE_MULTA', '') or ''),
        'EMBARGADO': str(p.get('EMBARGADO', '') or ''),
        'ANO_DESMATAMENTO': '',
        'FONTE_DETECCAO': '',
        'OBSERVACAO': '',
    }


def transform_te_legado(feature):
    """Transform Legado AREAS_EMBARGADAS_SEMA feature to unified schema."""
    p = feature.get('properties', {})
    geom = feature.get('geometry')
    lat_geom, lon_geom = extract_coords_from_geometry(geom)

    # Legado has DMS in text format: '51 31' 40,728" W'
    lat = parse_dms_legacy(p.get('COORD_Y'))
    lon = parse_dms_legacy(p.get('COORD_X'))
    if lat is None:
        lat = lat_geom
    if lon is None:
        lon = lon_geom

    return {
        'FONTE': 'LEGADO',
        'ID_ORIGINAL': str(p.get('OBJECTID', '')),
        'NUMERO_TERMO_EMBARGO': p.get('T_EMBARGO', ''),
        'NUMERO_AUTO_INFRACAO': p.get('A_INFRAC', ''),
        'NUMERO_PROCESSO': p.get('N_PROCESSO', ''),
        'DATA_EMBARGO': normalize_date(p.get('DAT_LAVRAT', '')),
        'NOME_RAZAO_SOCIAL': p.get('NOME', ''),
        'CPF_CNPJ': p.get('CPF_CNPJ', ''),
        'MUNICIPIO': '',  # Legado nao tem municipio
        'PROPRIEDADE': p.get('PROPRIEDAD', ''),
        'DESCRICAO_DANO': p.get('DANO', ''),
        'AREA_HA': str(p.get('AREA_HA', '') or ''),
        'LATITUDE': str(lat) if lat else '',
        'LONGITUDE': str(lon) if lon else '',
        'SITUACAO': p.get('OBS', ''),
        'VALOR_TOTAL_MULTA': '',
        'TIPO_AUTO': '',
        'SUBTIPO': '',
        'FRENTE': '',
        'ATIVIDADE': '',
        'ATIVIDADE_EMBARGADA': '',
        'DISPOSITIVO_LEGAL': '',
        'NUMERO_RELATORIO_TECNICO': '',
        'MATRICULA_TECNICO': '',
        'QUANTIDADE': '',
        'UNIDADE_MEDIDA': '',
        'VALOR_UNIDADE_MULTA': '',
        'EMBARGADO': '',
        'ANO_DESMATAMENTO': p.get('ANO_DESMAT', ''),
        'FONTE_DETECCAO': p.get('FONTE', ''),
        'OBSERVACAO': p.get('OBS', ''),
    }


# ============================================================
# DATABASE CREATION
# ============================================================

def create_tables(conn):
    """Create unified tables with normalized search columns."""
    cols_ai = ", ".join([f'"{f}" TEXT' for f in SEMA_AI_FIELDS])
    conn.execute(f'''CREATE TABLE sema_autos_infracao (
        {cols_ai},
        NOME_NORM TEXT,
        MUNICIPIO_NORM TEXT,
        CPF_CNPJ_NORM TEXT,
        NUM_PROCESSO_NORM TEXT
    )''')

    cols_te = ", ".join([f'"{f}" TEXT' for f in SEMA_TE_FIELDS])
    conn.execute(f'''CREATE TABLE sema_areas_embargadas (
        {cols_te},
        NOME_NORM TEXT,
        MUNICIPIO_NORM TEXT,
        CPF_CNPJ_NORM TEXT,
        NUM_PROCESSO_NORM TEXT
    )''')

    conn.commit()


def create_indexes(conn):
    """Create indexes for fast searching."""
    indexes = [
        # Autos de infracao
        ("idx_sai_nome", "sema_autos_infracao", "NOME_NORM"),
        ("idx_sai_cpf", "sema_autos_infracao", "CPF_CNPJ_NORM"),
        ("idx_sai_proc", "sema_autos_infracao", "NUM_PROCESSO_NORM"),
        ("idx_sai_mun", "sema_autos_infracao", "MUNICIPIO_NORM"),
        ("idx_sai_num", "sema_autos_infracao", "NUMERO_AUTO_INFRACAO"),
        ("idx_sai_data", "sema_autos_infracao", "DATA_AUTO"),
        ("idx_sai_fonte", "sema_autos_infracao", "FONTE"),
        ("idx_sai_situacao", "sema_autos_infracao", "SITUACAO"),
        # Areas embargadas
        ("idx_sae_nome", "sema_areas_embargadas", "NOME_NORM"),
        ("idx_sae_cpf", "sema_areas_embargadas", "CPF_CNPJ_NORM"),
        ("idx_sae_proc", "sema_areas_embargadas", "NUM_PROCESSO_NORM"),
        ("idx_sae_mun", "sema_areas_embargadas", "MUNICIPIO_NORM"),
        ("idx_sae_num_te", "sema_areas_embargadas", "NUMERO_TERMO_EMBARGO"),
        ("idx_sae_num_ai", "sema_areas_embargadas", "NUMERO_AUTO_INFRACAO"),
        ("idx_sae_data", "sema_areas_embargadas", "DATA_EMBARGO"),
        ("idx_sae_fonte", "sema_areas_embargadas", "FONTE"),
    ]
    for name, table, cols in indexes:
        conn.execute(f'CREATE INDEX IF NOT EXISTS {name} ON {table} ({cols})')
    conn.commit()


def create_fts(conn):
    """Create FTS5 virtual tables for full-text search."""
    # FTS for autos
    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS fts_sema_autos USING fts5(
            NOME_RAZAO_SOCIAL,
            DESCRICAO_OCORRENCIA,
            DISPOSITIVO_LEGAL,
            ATIVIDADE,
            content='sema_autos_infracao',
            content_rowid='rowid'
        )
    """)
    conn.execute("INSERT INTO fts_sema_autos(fts_sema_autos) VALUES('rebuild')")

    # FTS for embargos
    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS fts_sema_embargos USING fts5(
            NOME_RAZAO_SOCIAL,
            DESCRICAO_DANO,
            PROPRIEDADE,
            ATIVIDADE,
            content='sema_areas_embargadas',
            content_rowid='rowid'
        )
    """)
    conn.execute("INSERT INTO fts_sema_embargos(fts_sema_embargos) VALUES('rebuild')")

    # FTS for name search (normalized)
    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS fts_sema_ai_nome USING fts5(
            NOME_NORM,
            content='sema_autos_infracao',
            content_rowid='rowid'
        )
    """)
    conn.execute("INSERT INTO fts_sema_ai_nome(fts_sema_ai_nome) VALUES('rebuild')")

    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS fts_sema_te_nome USING fts5(
            NOME_NORM,
            content='sema_areas_embargadas',
            content_rowid='rowid'
        )
    """)
    conn.execute("INSERT INTO fts_sema_te_nome(fts_sema_te_nome) VALUES('rebuild')")

    # FTS for municipio search
    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS fts_sema_ai_mun USING fts5(
            MUNICIPIO_NORM,
            content='sema_autos_infracao',
            content_rowid='rowid'
        )
    """)
    conn.execute("INSERT INTO fts_sema_ai_mun(fts_sema_ai_mun) VALUES('rebuild')")

    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS fts_sema_te_mun USING fts5(
            MUNICIPIO_NORM,
            content='sema_areas_embargadas',
            content_rowid='rowid'
        )
    """)
    conn.execute("INSERT INTO fts_sema_te_mun(fts_sema_te_mun) VALUES('rebuild')")

    conn.commit()


def load_autos_infracao(conn):
    """Load and merge both AI sources into sema_autos_infracao."""
    all_cols = SEMA_AI_FIELDS + ['NOME_NORM', 'MUNICIPIO_NORM', 'CPF_CNPJ_NORM', 'NUM_PROCESSO_NORM']
    placeholders = ", ".join(["?" for _ in all_cols])
    cols = ", ".join([f'"{f}"' for f in all_cols])
    sql = f'INSERT INTO sema_autos_infracao ({cols}) VALUES ({placeholders})'

    total = 0

    # Source 1: SIGA
    features = load_geojson(os.path.join(DATA_DIR, "sema_ai_siga.json"))
    rows = []
    for f in features:
        rec = transform_ai_siga(f)
        vals = [str(rec.get(field, '') or '') for field in SEMA_AI_FIELDS]
        vals.extend([
            strip_accents(rec.get('NOME_RAZAO_SOCIAL', '')).upper(),
            strip_accents(rec.get('MUNICIPIO', '')).upper(),
            digits_only(rec.get('CPF_CNPJ', '')),
            digits_only(rec.get('NUMERO_PROCESSO', '')),
        ])
        rows.append(tuple(vals))
    if rows:
        conn.executemany(sql, rows)
        conn.commit()
        total += len(rows)
        print(f"    SIGA: {len(rows):,} records")

    # Source 2: Legado
    features = load_geojson(os.path.join(DATA_DIR, "sema_ai_legado.json"))
    rows = []
    for f in features:
        rec = transform_ai_legado(f)
        vals = [str(rec.get(field, '') or '') for field in SEMA_AI_FIELDS]
        vals.extend([
            strip_accents(rec.get('NOME_RAZAO_SOCIAL', '')).upper(),
            strip_accents(rec.get('MUNICIPIO', '')).upper(),
            digits_only(rec.get('CPF_CNPJ', '')),
            digits_only(rec.get('NUMERO_PROCESSO', '')),
        ])
        rows.append(tuple(vals))
    if rows:
        conn.executemany(sql, rows)
        conn.commit()
        total += len(rows)
        print(f"    Legado: {len(rows):,} records")

    return total


def load_areas_embargadas(conn):
    """Load and merge both TE sources into sema_areas_embargadas."""
    all_cols = SEMA_TE_FIELDS + ['NOME_NORM', 'MUNICIPIO_NORM', 'CPF_CNPJ_NORM', 'NUM_PROCESSO_NORM']
    placeholders = ", ".join(["?" for _ in all_cols])
    cols = ", ".join([f'"{f}"' for f in all_cols])
    sql = f'INSERT INTO sema_areas_embargadas ({cols}) VALUES ({placeholders})'

    total = 0

    # Source 1: SIGA
    features = load_geojson(os.path.join(DATA_DIR, "sema_embargo_siga.json"))
    rows = []
    for f in features:
        rec = transform_te_siga(f)
        vals = [str(rec.get(field, '') or '') for field in SEMA_TE_FIELDS]
        vals.extend([
            strip_accents(rec.get('NOME_RAZAO_SOCIAL', '')).upper(),
            strip_accents(rec.get('MUNICIPIO', '')).upper(),
            digits_only(rec.get('CPF_CNPJ', '')),
            digits_only(rec.get('NUMERO_PROCESSO', '')),
        ])
        rows.append(tuple(vals))
    if rows:
        conn.executemany(sql, rows)
        conn.commit()
        total += len(rows)
        print(f"    SIGA: {len(rows):,} records")

    # Source 2: Legado
    features = load_geojson(os.path.join(DATA_DIR, "sema_embargo_legado.json"))
    rows = []
    for f in features:
        rec = transform_te_legado(f)
        vals = [str(rec.get(field, '') or '') for field in SEMA_TE_FIELDS]
        vals.extend([
            strip_accents(rec.get('NOME_RAZAO_SOCIAL', '')).upper(),
            strip_accents(rec.get('MUNICIPIO', '')).upper(),
            digits_only(rec.get('CPF_CNPJ', '')),
            digits_only(rec.get('NUMERO_PROCESSO', '')),
        ])
        rows.append(tuple(vals))
    if rows:
        conn.executemany(sql, rows)
        conn.commit()
        total += len(rows)
        print(f"    Legado: {len(rows):,} records")

    return total


def main():
    print(f"=== SEMA-MT Data Loader ===")
    print(f"DB: {DB_PATH}")
    print(f"Data: {DATA_DIR}")
    print()

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".db", dir=_SCRIPT_DIR)
    os.close(tmp_fd)

    try:
        conn = sqlite3.connect(tmp_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-64000")

        print("Creating tables...")
        create_tables(conn)

        print("\n[1/2] Loading Autos de Infracao (SIGA + Legado)...")
        t0 = datetime.now()
        ai_count = load_autos_infracao(conn)
        t1 = datetime.now()
        print(f"  Total: {ai_count:,} records in {(t1-t0).seconds}s")

        print("\n[2/2] Loading Areas Embargadas (SIGA + Legado)...")
        t0 = datetime.now()
        te_count = load_areas_embargadas(conn)
        t1 = datetime.now()
        print(f"  Total: {te_count:,} records in {(t1-t0).seconds}s")

        print("\nCreating indexes...")
        create_indexes(conn)

        print("Creating FTS indexes...")
        create_fts(conn)

        print("\n=== SUMMARY ===")
        for table in ['sema_autos_infracao', 'sema_areas_embargadas']:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count:,} records")

        conn.close()

        db_size = os.path.getsize(tmp_path)
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
        shutil.move(tmp_path, DB_PATH)

        print(f"  DB size: {db_size/1024/1024:.1f}MB")
        print(f"\nDone! DB ready at {DB_PATH}")

    except Exception as e:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        print(f"\nERRO FATAL: {e}")
        raise


if __name__ == "__main__":
    main()
