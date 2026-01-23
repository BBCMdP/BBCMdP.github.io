#!/usr/bin/env python3
import argparse
import csv
import os
import re
from typing import Dict, List, Tuple

import pymysql
from pymysql.err import ProgrammingError

PROTEOME_FIELD_MAP = {
    'Hash': 'hash',
    'Origin CP': 'origin_cp',
    'web': 'web',
    "In NCBI's ref seq": 'in_ncbi_refseq',
    'Species': 'species',
    'taxID': 'taxid',
    'species_taxid': 'species_taxid',
    'current_scientific_name': 'current_scientific_name',
    'common_names': 'common_names',
    'group_name': 'group_name',
    'informal_clade': 'informal_clade',
    'code_vFV': 'code_vfv',
    'File_name': 'file_name',
    'Filepath_original': 'filepath_original',
    'num_seqs': 'num_seqs',
    'sum_len': 'sum_len',
    'min_len': 'min_len',
    'avg_len': 'avg_len',
    'max_len': 'max_len',
    'File_snip_name': 'file_snip_name',
    'Filepath_snip_processed': 'filepath_snip_processed',
    'Filepath_renamed_vFV': 'filepath_renamed_vfv',
    'num_seqs_snip_processed': 'num_seqs_snip_processed',
    'sum_len_snip_processed': 'sum_len_snip_processed',
    'min_len_snip_processed': 'min_len_snip_processed',
    'avg_len_snip_processed': 'avg_len_snip_processed',
    'max_len_snip_processed': 'max_len_snip_processed',
    'post_snip': 'post_snip',
    'AssemblyID': 'assembly_id',
}

BUSCO_DOMAIN_MAP = {
    'Complete BUSCO Domain': 'complete',
    'Single BUSCO Domain': 'single_copy',
    'Duplicated BUSCO Domain': 'duplicated',
    'Fragmented BUSCO Domain': 'fragmented',
    'Missing BUSCO Domain': 'missing',
}

BUSCO_KINGDOM_MAP = {
    'Complete BUSCO Kingdom': 'complete',
    'Single BUSCO Kingdom': 'single_copy',
    'Duplicated BUSCO Kingdom': 'duplicated',
    'Fragmented BUSCO Kingdom': 'fragmented',
    'Missing BUSCO Kingdom': 'missing',
}

TAXONOMY_LEVELS = [
    'Domain','Realm','Kingdom','Subkingdom','Superphylum','Phylum','Subphylum','Infraphylum',
    'Superclass','Class','Subclass','Infraclass','Cohort','Subcohort','Superorder','Order',
    'Suborder','Infraorder','Parvorder','Superfamily','Family','Subfamily','Tribe','Subtribe',
    'Genus','Subgenus','Section','Subsection','Series','Subseries','Species_group','Species_subgroup',
    'Forma_specialis','Subspecies','VarietasSubvariety','Forma','Serogroup','Serotype','Strain','Isolate',
    'Species'
]

COLLECTION_COLUMNS = [
    'Archaea_NR','Archaea_Class','Archaea_Order','Archaea_Fam','Archaea_Genus',
    'Bacteria_NR','Bacteria_Phylum','Bacteria_Class','Bacteria_Order','Bacteria_Fam','Bacteria_Genus',
    'Chordata_NR','Chordata_Order','Chordata_Fam','Chordata_Genus',
    'Fungi_NR','Fungi_Class','Fungi_Order','Fungi_Fam','Fungi_Genus',
    'Metazoa_NR','Metazoa_Class','Metazoa_Order','Metazoa_Fam','Metazoa_Genus',
    'Opisthokonta_NR','Opisthokonta_Class','Opisthokonta_Order','Opisthokonta_Fam','Opisthokonta_Genus',
    'Protista_Class','Protista_NR','Protista_Order','Protista_Fam','Protista_Genus','OpistoProtist',
    'Viridiplantae_NR','Viridiplantae_Order','Viridiplantae_Fam','Viridiplantae_Genus',
    'Embryophyta_NR','Embryophyta_Order','Embryophyta_Fam','Embryophyta_Genus'
]


_DB_COL_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _infer_converter(mysql_type: str):
    t = (mysql_type or "").strip().lower()
    if "tinyint(1)" in t or t == "boolean" or t == "bool":
        return lambda v: 1 if truthy(v) else 0
    if "int" in t:
        return to_int
    if "double" in t or "float" in t or "decimal" in t:
        return to_float
    return lambda v: v if v is not None else None


def load_extra_proteome_columns(conn) -> List[Dict[str, str]]:
    """Load optional proteome-level columns from proteome_column_meta.

    Returns rows with keys: tsv_header, db_column, mysql_type, nullable, default_value
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT tsv_header, db_column, mysql_type, nullable, default_value "
            "FROM proteome_column_meta"
        )
        rows = cur.fetchall()

    extra: List[Dict[str, str]] = []
    for r in rows:
        db_col = str(r["db_column"])
        if not _DB_COL_RE.match(db_col):
            raise ValueError(f"Invalid db_column in proteome_column_meta: {db_col!r}")
        tsv_header = str(r["tsv_header"]).strip()
        if not tsv_header or "`" in tsv_header:
            raise ValueError(f"Invalid tsv_header in proteome_column_meta: {tsv_header!r}")
        extra.append(r)
    return extra

def truthy(val: str) -> bool:
    if val is None:
        return False
    s = str(val).strip().lower()
    if s in ['', '0', 'false', 'no', 'none', 'nan']:
        return False
    return True

def to_int(val: str):
    try:
        return int(float(val))
    except Exception:
        return None

def to_float(val: str):
    try:
        return float(str(val).replace(',', '.'))
    except Exception:
        return None

def connect(host: str, user: str, password: str, db: str, unix_socket: str | None = None):
    kwargs = {
        "user": user,
        "password": password,
        "database": db,
        "charset": "utf8mb4",
        "autocommit": False,
        "cursorclass": pymysql.cursors.DictCursor,
    }
    if unix_socket:
        kwargs["unix_socket"] = unix_socket
    else:
        kwargs["host"] = host
    return pymysql.connect(**kwargs)

def ensure_collections(conn):
    with conn.cursor() as cur:
        for name in COLLECTION_COLUMNS:
            cur.execute("INSERT IGNORE INTO collection(name) VALUES (%s)", (name,))
    conn.commit()

def cache_taxonomy_term(conn) -> Dict[Tuple[str, str], int]:
    cache = {}
    with conn.cursor() as cur:
        cur.execute("SELECT id, level, name FROM taxonomy_term")
        for row in cur.fetchall():
            cache[(row['level'], row['name'])] = row['id']
    return cache

def get_or_create_taxonomy_term(conn, cache: Dict[Tuple[str, str], int], level: str, name: str) -> int:
    key = (level, name)
    term_id = cache.get(key)
    if term_id:
        return term_id
    with conn.cursor() as cur:
        cur.execute("INSERT IGNORE INTO taxonomy_term(level, name) VALUES (%s, %s)", (level, name))
        cur.execute("SELECT id FROM taxonomy_term WHERE level=%s AND name=%s", (level, name))
        row = cur.fetchone()
        term_id = row['id']
        cache[key] = term_id
        return term_id

def upsert_proteome(conn, row: Dict[str, str], extra_cols: List[Dict[str, str]]):
    """Insert/update a proteome row.

    Semantics:
    - Missing TSV columns (not present in the header) do NOT modify DB fields.
    - Present-but-empty cells are applied as empty/NULL/0 depending on field type.
    """
    data = {}
    for k_src, k_dst in PROTEOME_FIELD_MAP.items():
        if k_src not in row:
            continue
        v = row.get(k_src)
        if k_dst in ('in_ncbi_refseq', 'post_snip'):
            # Empty => false
            data[k_dst] = 1 if truthy(v) else 0
        elif k_dst in (
            'num_seqs', 'sum_len', 'min_len', 'max_len',
            'num_seqs_snip_processed', 'sum_len_snip_processed',
            'min_len_snip_processed', 'max_len_snip_processed',
            'taxid', 'species_taxid',
        ):
            # Empty/non-numeric => NULL
            data[k_dst] = to_int(v)
        elif k_dst in ('avg_len', 'avg_len_snip_processed'):
            data[k_dst] = to_float(v)
        else:
            # Keep empty string as empty string (user asked: empty means empty)
            data[k_dst] = v if v is not None else None

    # Apply registered extra columns (proteome_column_meta)
    for meta in extra_cols:
        tsv_header = meta["tsv_header"]
        if tsv_header not in row:
            continue

        db_col = meta["db_column"]
        mysql_type = meta["mysql_type"]
        nullable = bool(int(meta.get("nullable", 1)))
        default_value = meta.get("default_value")

        raw = row.get(tsv_header)
        # Present-but-empty semantics:
        # - If nullable: empty -> NULL
        # - If NOT NULL: empty -> default (if provided) else error
        if raw is None or str(raw).strip() == "":
            if nullable:
                data[db_col] = None
            else:
                if default_value is None:
                    raise ValueError(
                        f"TSV column {tsv_header!r} is NOT NULL but value is empty for Hash={row.get('Hash')!r}"
                    )
                conv = _infer_converter(mysql_type)
                data[db_col] = conv(default_value)
            continue

        conv = _infer_converter(mysql_type)
        data[db_col] = conv(raw)

    if 'hash' not in data:
        raise ValueError('Missing Hash column/value in TSV row')

    with conn.cursor() as cur:
        if len(data) == 1:
            # Only the primary key is present; insert if missing, otherwise no-op.
            cur.execute("INSERT IGNORE INTO proteome(hash) VALUES (%s)", (data['hash'],))
            return

        placeholders = ','.join(['%s'] * len(data))
        columns = ','.join(data.keys())
        updates = ','.join([f"{col}=VALUES({col})" for col in data.keys() if col != 'hash'])
        sql = f"INSERT INTO proteome({columns}) VALUES({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
        cur.execute(sql, list(data.values()))

def upsert_busco(conn, hash_val: str, rank: str, metrics: Dict[str, float]):
    """Update BUSCO metrics for one rank.

    Only updates fields present in `metrics`.
    (Missing TSV columns should not overwrite existing DB values.)
    """
    if not metrics:
        return
    with conn.cursor() as cur:
        cur.execute("INSERT IGNORE INTO busco_summary(hash, `rank`) VALUES (%s, %s)", (hash_val, rank))
        set_sql = ', '.join([f"{k}=%s" for k in metrics.keys()])
        params = list(metrics.values()) + [hash_val, rank]
        cur.execute(
            f"UPDATE busco_summary SET {set_sql} WHERE hash=%s AND `rank`=%s",
            params,
        )

def upsert_taxonomy(conn, cache, hash_val: str, row: Dict[str, str]):
    with conn.cursor() as cur:
        for level in TAXONOMY_LEVELS:
            val = row.get(level)
            if val and str(val).strip():
                term_id = get_or_create_taxonomy_term(conn, cache, level, str(val).strip())
                cur.execute(
                    "INSERT IGNORE INTO proteome_taxonomy(hash, term_id) VALUES (%s, %s)",
                    (hash_val, term_id)
                )

def upsert_collections(conn, hash_val: str, row: Dict[str, str]):
    with conn.cursor() as cur:
        for name in COLLECTION_COLUMNS:
            val = row.get(name)
            if truthy(val):
                cur.execute("SELECT id FROM collection WHERE name=%s", (name,))
                cid = cur.fetchone()['id']
                cur.execute(
                    "INSERT IGNORE INTO collection_membership(collection_id, hash) VALUES (%s, %s)",
                    (cid, hash_val)
                )

def process_file(args):
    conn = connect(args.host, args.user, args.password, args.db, unix_socket=args.unix_socket)
    if not args.no_ensure_collections:
        ensure_collections(conn)
    tax_cache = cache_taxonomy_term(conn)
    try:
        extra_cols = load_extra_proteome_columns(conn)
    except ProgrammingError as e:
        # Table missing: allow running imports even before optional-column feature is installed.
        # Anything else (permissions, SQL errors) should be surfaced.
        code = e.args[0] if e.args else None
        if code == 1146:
            extra_cols = []
        else:
            raise

    if extra_cols:
        print(
            "Optional proteome columns enabled: "
            + ", ".join([str(r['tsv_header']) for r in extra_cols])
        )

    with open(args.tsv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        count = 0
        for row in reader:
            hash_val = row.get('Hash')
            if not hash_val:
                continue

            upsert_proteome(conn, row, extra_cols)

            # BUSCO: only update metrics whose TSV columns are present.
            busco_d = {
                k_dst: to_float(row.get(k_src))
                for k_src, k_dst in BUSCO_DOMAIN_MAP.items()
                if k_src in row
            }
            upsert_busco(conn, hash_val, 'Domain', busco_d)

            busco_k = {
                k_dst: to_float(row.get(k_src))
                for k_src, k_dst in BUSCO_KINGDOM_MAP.items()
                if k_src in row
            }
            upsert_busco(conn, hash_val, 'Kingdom', busco_k)

            upsert_taxonomy(conn, tax_cache, hash_val, row)
            upsert_collections(conn, hash_val, row)

            count += 1
            if count % args.commit_every == 0:
                conn.commit()
        conn.commit()
    conn.close()
    print(f'Import completed. {count} proteomes processed.')

def main():
    parser = argparse.ArgumentParser(description='Import BBC Complete Proteomes TSV into MySQL.')
    parser.add_argument('--tsv', required=True, help='Path to BBC_Complete_Proteomes_DB.tsv')
    parser.add_argument('--host', default=os.getenv('DB_HOST', 'localhost'))
    parser.add_argument('--user', default=os.getenv('DB_USER', 'root'))
    parser.add_argument('--password', default=os.getenv('DB_PASSWORD', ''))
    parser.add_argument('--db', default=os.getenv('DB_NAME', 'bbc_proteomes'))
    parser.add_argument('--unix-socket', dest='unix_socket', default=os.getenv('DB_UNIX_SOCKET'))
    parser.add_argument('--commit-every', type=int, default=1000, help='Commit every N rows for speed and safety')
    parser.add_argument(
        '--no-ensure-collections',
        action='store_true',
        help='Do not auto-create the preset collections list (lets you permanently remove a collection name)',
    )
    args = parser.parse_args()

    process_file(args)

if __name__ == '__main__':
    main()
