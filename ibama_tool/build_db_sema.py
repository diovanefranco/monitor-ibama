#!/usr/bin/env python3
"""
SEMA-MT Data Loader v2 - COMPREHENSIVE
Reads ALL 21 GeoJSON layers from sema_data/ and loads into SQLite.

Tables:
  - sema_autos_infracao   (AI SIGA ponto+polígono, AI Legado, AI Descentralizado)
  - sema_outros_termos    (Inspeção, Notificação, Apreensão, Depósito, Destruição, Soltura + Desc.)
  - sema_embargos         (Embargo SIGA ponto+polígono, Embargo Legado, Embargo Descentralizado)
  - sema_desembargos      (Desembargo SIGA ponto+polígono, Desembargo Legado)

Each table has a superset of fields covering ALL source schemas.
"""
import os, sys, sqlite3, json, tempfile, shutil, unicodedata, re
from datetime import datetime

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(_SCRIPT_DIR, "sema.db")
DATA_DIR = os.path.join(_SCRIPT_DIR, "sema_data")

# ============================================================
# UNIVERSAL FIELDS (superset of all schemas)
# Every table has these same columns for maximum flexibility.
# ============================================================
UNIVERSAL_FIELDS = [
    "FONTE",                    # SIGA / LEGADO / DESCENTRALIZADO / SIGA_TERMOS
    "CAMADA",                   # Original layer filename (e.g., ai_siga_ponto.json)
    "TIPO_DOCUMENTO",           # Auto de Infração, Notificação, Termo de Embargo, etc.
    "ID_ORIGINAL",              # Original ID from source
    "NUMERO_DOCUMENTO",         # Number of the auto/termo/notificação
    "NUMERO_PROCESSO",          # Process number
    "DATA_DOCUMENTO",           # Main date (ISO YYYY-MM-DD)
    "DATA_ENVIO",               # Send/submission date
    "NOME_RAZAO_SOCIAL",        # Person/company name
    "NOME_FANTASIA",            # Trade name / property name
    "CPF_CNPJ",                 # CPF or CNPJ
    "MUNICIPIO",                # Municipality
    "TIPO",                     # General type (e.g., DESMATAMENTO)
    "SUBTIPO",                  # Subtype detail
    "SITUACAO",                 # Status/situation
    "VALOR_TOTAL_MULTA",        # Total fine value
    "VALOR_UNIDADE_MULTA",      # Per-unit fine value
    "DISPOSITIVO_LEGAL",        # Legal provision violated
    "DESCRICAO_OCORRENCIA",     # Description of the infraction/occurrence
    "LATITUDE",
    "LONGITUDE",
    "FRENTE",                   # Front (Flora, Fauna, etc.)
    "ATIVIDADE",                # Activity
    "ATIVIDADE_EMBARGADA",      # Embargoed activity description
    "EMBARGADO",                # Embargo flag
    "NUMERO_TERMO_EMBARGO",     # Embargo term number
    "NUMERO_AUTO_INFRACAO",     # Related infraction auto number
    "NUMERO_RELATORIO_TECNICO", # Technical report number
    "MATRICULA_TECNICO",        # Technician badge number
    "QUANTIDADE",               # Quantity (area, items, etc.)
    "UNIDADE_MEDIDA",           # Unit of measure
    "DESCRICAO_AGRAVANTE",      # Aggravating factor description
    "PERCENTUAL_AGRAVANTE",     # Aggravating factor percentage
    "AREA_HA",                  # Area in hectares
    "PROPRIEDADE",              # Property name
    "DESCRICAO_DANO",           # Damage description
    # Legado AI specific
    "SETOR",
    "AUTOR",                    # Author (fiscal/technician)
    "AREA_DESMATADA",
    "ANO_DESMATAMENTO",
    "FONTE_DETECCAO",           # Detection source (SCCON, CAPEX, etc.)
    "OBSERVACAO",
    # Descentralizado specific
    "SERVIDOR",                 # Server/technician name
    "LINK_DOCUMENTO",           # Link to document
    "NIVEL_IMPACTO",            # Impact level
    "CODIGO_CNAI",              # CNAI code
    "STATUS_DESC",              # Status from Descentralizado
    "DATA_DENUNCIA",            # Denunciation date
    "DATA_VALIDADE",            # Validity date
    "PARAMETROS",               # Parameters
]


# ============================================================
# HELPER FUNCTIONS
# ============================================================

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

def safe_str(val):
    """Convert any value to a clean string, handling None/nan."""
    if val is None:
        return ''
    s = str(val).strip()
    if s.lower() in ('none', 'nan', 'null', 'nan'):
        return ''
    return s

def parse_dms_to_decimal(dms_str):
    """Convert DMS string like '-16:15:43,70' to decimal degrees."""
    if not dms_str:
        return None
    try:
        s = str(dms_str).strip().replace(',', '.')
        negative = s.startswith('-')
        s = s.lstrip('-')
        parts = s.split(':')
        if len(parts) == 3:
            d, m, sec = float(parts[0]), float(parts[1]), float(parts[2])
            decimal = d + m / 60.0 + sec / 3600.0
            return -decimal if negative else decimal
    except (ValueError, IndexError):
        pass
    try:
        return float(str(dms_str).replace(',', '.'))
    except ValueError:
        return None

def parse_dms_legacy(coord_str):
    """Parse legacy DMS format like '51° 31' 40,728\" W' to decimal."""
    if not coord_str or not str(coord_str).strip():
        return None
    try:
        s = str(coord_str).strip()
        direction = 1
        if s.endswith(('W', 'S', 'O')):
            direction = -1
            s = s[:-1].strip()
        elif s.endswith(('E', 'N')):
            s = s[:-1].strip()
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
    if not date_str or not str(date_str).strip():
        return ''
    s = str(date_str).strip()
    if s.lower() in ('nan', 'none', 'null'):
        return ''
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

def extract_coords_from_geometry(geometry):
    """Extract lat/lon from GeoJSON geometry (centroid for polygons)."""
    if not geometry:
        return None, None
    coords = geometry.get('coordinates')
    if not coords:
        return None, None
    gtype = geometry.get('type', '')
    try:
        if gtype == 'Point':
            return coords[1], coords[0]
        elif gtype == 'MultiPoint':
            return coords[0][1], coords[0][0]
        elif gtype == 'Polygon':
            ring = coords[0]
            lat = sum(p[1] for p in ring) / len(ring)
            lon = sum(p[0] for p in ring) / len(ring)
            return lat, lon
        elif gtype == 'MultiPolygon':
            ring = coords[0][0]
            lat = sum(p[1] for p in ring) / len(ring)
            lon = sum(p[0] for p in ring) / len(ring)
            return lat, lon
    except (IndexError, TypeError, ZeroDivisionError):
        pass
    return None, None

def load_geojson(filepath):
    """Load GeoJSON file and return features list."""
    if not os.path.exists(filepath):
        print(f"  SKIP: {filepath} not found")
        return []
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    features = data.get('features', [])
    print(f"  Loaded {len(features):,} features from {os.path.basename(filepath)}")
    return features

def empty_record():
    """Return an empty record dict."""
    return {f: '' for f in UNIVERSAL_FIELDS}


# ============================================================
# TRANSFORM FUNCTIONS - Convert source features to universal schema
# ============================================================

def transform_siga_30(feature, camada, tipo_documento):
    """Transform features with the 30-field SIGA schema.
    Used by: ai_siga_ponto, ai_siga_poligono, embargo_siga_ponto,
             embargo_siga_poligono, desembargo_siga_ponto, desembargo_siga_poligono
    """
    p = feature.get('properties', {})
    geom = feature.get('geometry')
    lat_geom, lon_geom = extract_coords_from_geometry(geom)

    lat = parse_dms_to_decimal(p.get('LATITUDE'))
    lon = parse_dms_to_decimal(p.get('LONGITUDE'))
    if lat is None: lat = lat_geom
    if lon is None: lon = lon_geom

    rec = empty_record()
    rec['FONTE'] = 'SIGA'
    rec['CAMADA'] = camada
    rec['TIPO_DOCUMENTO'] = safe_str(p.get('TIPO_DO_AUTO')) or tipo_documento
    rec['ID_ORIGINAL'] = safe_str(p.get('ID_PADRAO'))
    rec['NUMERO_DOCUMENTO'] = safe_str(p.get('NUMERO_AUTO_INFRACAO'))
    rec['NUMERO_PROCESSO'] = safe_str(p.get('NUMERO_PROCESSO'))
    rec['DATA_DOCUMENTO'] = normalize_date(p.get('DATA_DO_AUTO'))
    rec['DATA_ENVIO'] = normalize_date(p.get('DATA_ENVIO'))
    rec['NOME_RAZAO_SOCIAL'] = safe_str(p.get('NOME_RAZAO_SOCIAL'))
    rec['NOME_FANTASIA'] = safe_str(p.get('NOME_FANTASIA'))
    rec['CPF_CNPJ'] = safe_str(p.get('CPFCNPJ'))
    rec['MUNICIPIO'] = safe_str(p.get('MUNICIPIO_DO_DANO'))
    rec['TIPO'] = safe_str(p.get('TIPO'))
    rec['SUBTIPO'] = safe_str(p.get('SUBTIPO'))
    rec['SITUACAO'] = safe_str(p.get('SITUACAO'))
    rec['VALOR_TOTAL_MULTA'] = safe_str(p.get('VALOR_TOTAL_DA_MULTA'))
    rec['VALOR_UNIDADE_MULTA'] = safe_str(p.get('VALOR_DA_UNIDADE_MULTA'))
    rec['DISPOSITIVO_LEGAL'] = safe_str(p.get('DISPOSITIVO_LEGAL_INFRINGIDO'))
    rec['DESCRICAO_OCORRENCIA'] = safe_str(p.get('DESCRICAO_DA_OCORRENCIA'))
    rec['LATITUDE'] = str(lat) if lat else ''
    rec['LONGITUDE'] = str(lon) if lon else ''
    rec['FRENTE'] = safe_str(p.get('FRENTE'))
    rec['ATIVIDADE'] = safe_str(p.get('ATIVIDADE'))
    rec['ATIVIDADE_EMBARGADA'] = safe_str(p.get('ATIVIDADE_EMBARGADA'))
    rec['EMBARGADO'] = safe_str(p.get('EMBARGADO'))
    rec['NUMERO_TERMO_EMBARGO'] = safe_str(p.get('NUMERO_TERMO_EMBARGO'))
    rec['NUMERO_AUTO_INFRACAO'] = safe_str(p.get('NUMERO_AUTO_INFRACAO'))
    rec['NUMERO_RELATORIO_TECNICO'] = safe_str(p.get('NUMERO_RELATORIO_TECNICO'))
    rec['MATRICULA_TECNICO'] = safe_str(p.get('MATRICULA_TECNICO'))
    rec['QUANTIDADE'] = safe_str(p.get('QUANTIDADE'))
    rec['UNIDADE_MEDIDA'] = safe_str(p.get('UNIDADE_MEDIDA'))
    rec['DESCRICAO_AGRAVANTE'] = safe_str(p.get('DESCRICAO_AGRAVANTE'))
    rec['PERCENTUAL_AGRAVANTE'] = safe_str(p.get('PERCENTUAL_AGRAVANTE'))
    rec['AREA_HA'] = safe_str(p.get('QUANTIDADE'))  # Quantity often = area
    return rec


def transform_legado_ai(feature, camada):
    """Transform Legado AI features (MVW_TIT_AUTUACAO schema, 15 fields)."""
    p = feature.get('properties', {})
    geom = feature.get('geometry')
    lat, lon = extract_coords_from_geometry(geom)

    rec = empty_record()
    rec['FONTE'] = 'LEGADO'
    rec['CAMADA'] = camada
    rec['TIPO_DOCUMENTO'] = safe_str(p.get('MODELO')) or 'Auto de Infração'
    rec['ID_ORIGINAL'] = safe_str(p.get('OBJECTID'))
    rec['NUMERO_DOCUMENTO'] = safe_str(p.get('NUMERO'))
    rec['NUMERO_PROCESSO'] = safe_str(p.get('PROCESSO'))
    rec['DATA_DOCUMENTO'] = normalize_date(p.get('DATA_EMISSAO'))
    rec['NOME_RAZAO_SOCIAL'] = safe_str(p.get('RAZAO_SOCIAL'))
    rec['CPF_CNPJ'] = safe_str(p.get('CPF_CNPJ'))
    rec['MUNICIPIO'] = safe_str(p.get('MUNICIPIO'))
    rec['SUBTIPO'] = safe_str(p.get('SUBTIPO'))
    rec['SITUACAO'] = safe_str(p.get('SITUACAO'))
    rec['DESCRICAO_OCORRENCIA'] = safe_str(p.get('NOME'))  # 'NOME' = infraction desc
    rec['LATITUDE'] = str(lat) if lat else ''
    rec['LONGITUDE'] = str(lon) if lon else ''
    rec['SETOR'] = safe_str(p.get('SETOR'))
    rec['AUTOR'] = safe_str(p.get('AUTOR'))
    rec['AREA_DESMATADA'] = safe_str(p.get('AREA_DESMATADA'))
    return rec


def transform_legado_embargo(feature, camada, tipo_doc):
    """Transform Legado Embargo/Desembargo features (15 fields)."""
    p = feature.get('properties', {})
    geom = feature.get('geometry')
    lat_geom, lon_geom = extract_coords_from_geometry(geom)

    lat = parse_dms_legacy(p.get('COORD_Y'))
    lon = parse_dms_legacy(p.get('COORD_X'))
    if lat is None: lat = lat_geom
    if lon is None: lon = lon_geom

    rec = empty_record()
    rec['FONTE'] = 'LEGADO'
    rec['CAMADA'] = camada
    rec['TIPO_DOCUMENTO'] = tipo_doc
    rec['ID_ORIGINAL'] = safe_str(p.get('OBJECTID'))
    rec['NUMERO_TERMO_EMBARGO'] = safe_str(p.get('T_EMBARGO'))
    rec['NUMERO_AUTO_INFRACAO'] = safe_str(p.get('A_INFRAC'))
    rec['NUMERO_PROCESSO'] = safe_str(p.get('N_PROCESSO'))
    rec['DATA_DOCUMENTO'] = normalize_date(p.get('DAT_LAVRAT'))
    rec['NOME_RAZAO_SOCIAL'] = safe_str(p.get('NOME'))
    rec['CPF_CNPJ'] = safe_str(p.get('CPF_CNPJ'))
    rec['PROPRIEDADE'] = safe_str(p.get('PROPRIEDAD'))
    rec['DESCRICAO_DANO'] = safe_str(p.get('DANO'))
    rec['AREA_HA'] = safe_str(p.get('AREA_HA'))
    rec['LATITUDE'] = str(lat) if lat else ''
    rec['LONGITUDE'] = str(lon) if lon else ''
    rec['ANO_DESMATAMENTO'] = safe_str(p.get('ANO_DESMAT'))
    rec['FONTE_DETECCAO'] = safe_str(p.get('FONTE'))
    rec['OBSERVACAO'] = safe_str(p.get('OBS'))
    return rec


def transform_siga_termos(feature, camada):
    """Transform SIGA Termos features (15-field schema).
    Used by: inspecao_siga, notificacao_siga, termo_apreensao, termo_deposito,
             termo_destruicao, termo_soltura
    """
    p = feature.get('properties', {})
    geom = feature.get('geometry')
    lat_geom, lon_geom = extract_coords_from_geometry(geom)

    lat = parse_dms_to_decimal(p.get('LATITUDE'))
    lon = parse_dms_to_decimal(p.get('LONGITUDE'))
    if lat is None: lat = lat_geom
    if lon is None: lon = lon_geom

    rec = empty_record()
    rec['FONTE'] = 'SIGA_TERMOS'
    rec['CAMADA'] = camada
    rec['TIPO_DOCUMENTO'] = safe_str(p.get('TIPO_AUTO_TERMO'))
    rec['ID_ORIGINAL'] = safe_str(p.get('OBJECTID'))
    rec['NUMERO_DOCUMENTO'] = safe_str(p.get('NUM_AUTO_TERMO'))
    rec['NUMERO_PROCESSO'] = safe_str(p.get('NUM_PROCESSO'))
    rec['DATA_DOCUMENTO'] = normalize_date(p.get('DATA_CRIACAO'))
    rec['DATA_ENVIO'] = normalize_date(p.get('DATA_ENVIO'))
    rec['NOME_RAZAO_SOCIAL'] = safe_str(p.get('NOME_RAZAO'))
    rec['NOME_FANTASIA'] = safe_str(p.get('NOME_FANTASIA'))
    rec['CPF_CNPJ'] = safe_str(p.get('CPF_CNPJ'))
    rec['MUNICIPIO'] = safe_str(p.get('MUNICIPIO_DANO'))
    rec['ATIVIDADE'] = safe_str(p.get('ATIVIDADE'))
    rec['NUMERO_RELATORIO_TECNICO'] = safe_str(p.get('RELAT_TECNICO'))
    rec['MATRICULA_TECNICO'] = safe_str(p.get('MATRI_TECNICO'))
    rec['LATITUDE'] = str(lat) if lat else ''
    rec['LONGITUDE'] = str(lon) if lon else ''
    return rec


def transform_descentralizado(feature, camada):
    """Transform Descentralizado TDAD features (24-field schema).
    Used by: ai_descentralizado, inspecao_descentralizado, notificacao_descentralizado,
             embargo_descentralizado, fiscalizacao_descentralizado, las_descentralizado
    """
    p = feature.get('properties', {})
    geom = feature.get('geometry')
    lat_geom, lon_geom = extract_coords_from_geometry(geom)

    # TDAD has decimal lat/lon
    lat = None
    lon = None
    try:
        lat_val = p.get('LATITUDE')
        lon_val = p.get('LONGITUDE')
        if lat_val and str(lat_val).lower() not in ('nan', 'none', ''):
            lat = float(str(lat_val).replace(',', '.'))
        if lon_val and str(lon_val).lower() not in ('nan', 'none', ''):
            lon = float(str(lon_val).replace(',', '.'))
    except (ValueError, TypeError):
        pass
    if lat is None: lat = lat_geom
    if lon is None: lon = lon_geom

    rec = empty_record()
    rec['FONTE'] = 'DESCENTRALIZADO'
    rec['CAMADA'] = camada
    rec['TIPO_DOCUMENTO'] = safe_str(p.get('NOME_DOCUMENTO'))
    rec['ID_ORIGINAL'] = safe_str(p.get('ID'))
    rec['NUMERO_DOCUMENTO'] = safe_str(p.get('NUMERO_DOCUMENTO'))
    rec['NUMERO_PROCESSO'] = safe_str(p.get('NUMERO_PROCESSO'))
    rec['DATA_DOCUMENTO'] = normalize_date(p.get('DATA_DOCUMENTO'))
    rec['NOME_RAZAO_SOCIAL'] = safe_str(p.get('RAZAO_SOCIAL'))
    rec['CPF_CNPJ'] = safe_str(p.get('CPF_CNPJ'))
    rec['MUNICIPIO'] = safe_str(p.get('MUNICIPIO'))
    rec['ATIVIDADE'] = safe_str(p.get('ATIVIDADE'))
    rec['LATITUDE'] = str(lat) if lat else ''
    rec['LONGITUDE'] = str(lon) if lon else ''
    rec['SERVIDOR'] = safe_str(p.get('SERVIDOR'))
    rec['LINK_DOCUMENTO'] = safe_str(p.get('LINK_DOCUMENTO'))
    rec['NIVEL_IMPACTO'] = safe_str(p.get('NIVEL_IMPACTO'))
    rec['CODIGO_CNAI'] = safe_str(p.get('CODIGO_CNAI'))
    rec['STATUS_DESC'] = safe_str(p.get('STATUS'))
    rec['DATA_DENUNCIA'] = normalize_date(p.get('DATA_DENUNCIA'))
    rec['DATA_VALIDADE'] = normalize_date(p.get('DATA_VALIDADE'))
    rec['PARAMETROS'] = safe_str(p.get('PARAMETROS'))
    return rec


# ============================================================
# LAYER → TABLE MAPPING
# Each entry: (filename, transform_function, extra_args)
# ============================================================

AUTOS_INFRACAO_LAYERS = [
    ("ai_siga_ponto.json",    transform_siga_30,        ("ai_siga_ponto.json", "Auto de Infração")),
    ("ai_siga_poligono.json", transform_siga_30,        ("ai_siga_poligono.json", "Auto de Infração")),
    ("ai_legado.json",        transform_legado_ai,       ("ai_legado.json",)),
    ("ai_descentralizado.json", transform_descentralizado, ("ai_descentralizado.json",)),
]

OUTROS_TERMOS_LAYERS = [
    ("inspecao_siga.json",             transform_siga_termos,     ("inspecao_siga.json",)),
    ("inspecao_descentralizado.json",  transform_descentralizado, ("inspecao_descentralizado.json",)),
    ("notificacao_siga.json",          transform_siga_termos,     ("notificacao_siga.json",)),
    ("notificacao_descentralizado.json", transform_descentralizado, ("notificacao_descentralizado.json",)),
    ("termo_apreensao.json",           transform_siga_termos,     ("termo_apreensao.json",)),
    ("termo_deposito.json",            transform_siga_termos,     ("termo_deposito.json",)),
    ("termo_destruicao.json",          transform_siga_termos,     ("termo_destruicao.json",)),
    ("termo_soltura.json",             transform_siga_termos,     ("termo_soltura.json",)),
    ("fiscalizacao_descentralizado.json", transform_descentralizado, ("fiscalizacao_descentralizado.json",)),
    ("las_descentralizado.json",       transform_descentralizado, ("las_descentralizado.json",)),
]

EMBARGOS_LAYERS = [
    ("embargo_siga_ponto.json",    transform_siga_30,          ("embargo_siga_ponto.json", "Termo de embargo / Interdição")),
    ("embargo_siga_poligono.json", transform_siga_30,          ("embargo_siga_poligono.json", "Termo de embargo / Interdição")),
    ("embargo_legado.json",        transform_legado_embargo,   ("embargo_legado.json", "Termo de Embargo")),
    ("embargo_descentralizado.json", transform_descentralizado, ("embargo_descentralizado.json",)),
]

DESEMBARGOS_LAYERS = [
    ("desembargo_siga_ponto.json",    transform_siga_30,        ("desembargo_siga_ponto.json", "Desembargo")),
    ("desembargo_siga_poligono.json", transform_siga_30,        ("desembargo_siga_poligono.json", "Desembargo")),
    ("desembargo_legado.json",        transform_legado_embargo,  ("desembargo_legado.json", "Desembargo")),
]


# ============================================================
# DATABASE CREATION
# ============================================================

def create_table(conn, table_name):
    """Create a table with universal schema + normalized search columns."""
    cols = ", ".join([f'"{f}" TEXT' for f in UNIVERSAL_FIELDS])
    conn.execute(f'''CREATE TABLE IF NOT EXISTS {table_name} (
        {cols},
        NOME_NORM TEXT,
        MUNICIPIO_NORM TEXT,
        CPF_CNPJ_NORM TEXT,
        NUM_PROCESSO_NORM TEXT
    )''')
    conn.commit()


def create_indexes(conn, table_name, prefix):
    """Create indexes for fast searching."""
    indexes = [
        (f"idx_{prefix}_nome",     "NOME_NORM"),
        (f"idx_{prefix}_cpf",      "CPF_CNPJ_NORM"),
        (f"idx_{prefix}_proc",     "NUM_PROCESSO_NORM"),
        (f"idx_{prefix}_mun",      "MUNICIPIO_NORM"),
        (f"idx_{prefix}_numdoc",   "NUMERO_DOCUMENTO"),
        (f"idx_{prefix}_data",     "DATA_DOCUMENTO"),
        (f"idx_{prefix}_fonte",    "FONTE"),
        (f"idx_{prefix}_tipo",     "TIPO_DOCUMENTO"),
        (f"idx_{prefix}_sit",      "SITUACAO"),
    ]
    for name, col in indexes:
        conn.execute(f'CREATE INDEX IF NOT EXISTS {name} ON {table_name} ({col})')
    conn.commit()


def create_fts(conn, table_name, prefix):
    """Create FTS5 virtual tables for full-text search."""
    # FTS for name search (normalized)
    fts_nome = f"fts_{prefix}_nome"
    conn.execute(f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS {fts_nome} USING fts5(
            NOME_NORM,
            content='{table_name}',
            content_rowid='rowid'
        )
    """)
    conn.execute(f"INSERT INTO {fts_nome}({fts_nome}) VALUES('rebuild')")

    # FTS for municipio
    fts_mun = f"fts_{prefix}_mun"
    conn.execute(f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS {fts_mun} USING fts5(
            MUNICIPIO_NORM,
            content='{table_name}',
            content_rowid='rowid'
        )
    """)
    conn.execute(f"INSERT INTO {fts_mun}({fts_mun}) VALUES('rebuild')")

    conn.commit()


def load_layers(conn, table_name, layer_defs):
    """Load multiple layers into a single table."""
    all_cols = UNIVERSAL_FIELDS + ['NOME_NORM', 'MUNICIPIO_NORM', 'CPF_CNPJ_NORM', 'NUM_PROCESSO_NORM']
    placeholders = ", ".join(["?" for _ in all_cols])
    cols = ", ".join([f'"{f}"' for f in all_cols])
    sql = f'INSERT INTO {table_name} ({cols}) VALUES ({placeholders})'

    total = 0
    for filename, transform_fn, transform_args in layer_defs:
        filepath = os.path.join(DATA_DIR, filename)
        features = load_geojson(filepath)
        if not features:
            continue

        rows = []
        for feat in features:
            rec = transform_fn(feat, *transform_args)
            vals = [safe_str(rec.get(field, '')) for field in UNIVERSAL_FIELDS]
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
            print(f"    → {filename}: {len(rows):,} records loaded")

    return total


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 60)
    print("SEMA-MT Data Loader v2 - COMPREHENSIVE")
    print(f"DB: {DB_PATH}")
    print(f"Data: {DATA_DIR}")
    print("=" * 60)

    if not os.path.isdir(DATA_DIR):
        print(f"\nERRO: Data directory not found: {DATA_DIR}")
        print("Run download_all_sema.py first.")
        sys.exit(1)

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".db", dir=_SCRIPT_DIR)
    os.close(tmp_fd)

    try:
        conn = sqlite3.connect(tmp_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-64000")

        # Table definitions: (table_name, prefix, layer_defs)
        tables = [
            ("sema_autos_infracao", "sai", AUTOS_INFRACAO_LAYERS),
            ("sema_outros_termos", "sot", OUTROS_TERMOS_LAYERS),
            ("sema_embargos", "sem", EMBARGOS_LAYERS),
            ("sema_desembargos", "sde", DESEMBARGOS_LAYERS),
        ]

        grand_total = 0
        for i, (table_name, prefix, layer_defs) in enumerate(tables, 1):
            print(f"\n[{i}/{len(tables)}] {table_name}")
            print("-" * 40)

            create_table(conn, table_name)
            t0 = datetime.now()
            count = load_layers(conn, table_name, layer_defs)
            t1 = datetime.now()
            print(f"  Total: {count:,} records in {(t1-t0).seconds}s")
            grand_total += count

        # Create indexes
        print("\nCreating indexes...")
        for table_name, prefix, _ in tables:
            create_indexes(conn, table_name, prefix)
            print(f"  ✅ {table_name}")

        # Create FTS
        print("\nCreating FTS indexes...")
        for table_name, prefix, _ in tables:
            create_fts(conn, table_name, prefix)
            print(f"  ✅ {table_name}")

        # Summary
        print("\n" + "=" * 60)
        print("📊 DATABASE SUMMARY")
        print("=" * 60)
        for table_name, _, _ in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            # Count by TIPO_DOCUMENTO
            tipos = conn.execute(f"""
                SELECT TIPO_DOCUMENTO, COUNT(*)
                FROM {table_name}
                WHERE TIPO_DOCUMENTO != ''
                GROUP BY TIPO_DOCUMENTO
                ORDER BY COUNT(*) DESC
            """).fetchall()
            print(f"\n  {table_name}: {count:,} records")
            for tipo, cnt in tipos[:5]:
                print(f"    - {tipo}: {cnt:,}")
            if len(tipos) > 5:
                print(f"    ... +{len(tipos)-5} other types")

        conn.close()

        db_size = os.path.getsize(tmp_path)
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
        shutil.move(tmp_path, DB_PATH)

        print(f"\n  🏁 GRAND TOTAL: {grand_total:,} records")
        print(f"  💾 DB size: {db_size/1024/1024:.1f} MB")
        print(f"\n✅ Done! DB ready at {DB_PATH}")

    except Exception as e:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        print(f"\n❌ ERRO FATAL: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
