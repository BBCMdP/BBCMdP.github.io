import argparse
import os
import re
import time

import pymysql
from pymysql.err import ProgrammingError


_DB_COL_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _load_extra_proteome_columns(conn):
    """Return list of meta rows: tsv_header, db_column, mysql_type, nullable, default_value."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT tsv_header, db_column, mysql_type, nullable, default_value "
            "FROM proteome_column_meta"
        )
        rows = cur.fetchall()

    extras = []
    for r in rows:
        tsv_header = str(r["tsv_header"]).strip()
        db_column = str(r["db_column"]).strip()
        mysql_type = str(r["mysql_type"]).strip()
        if not tsv_header or "`" in tsv_header or ";" in tsv_header:
            raise ValueError(f"Invalid tsv_header in proteome_column_meta: {tsv_header!r}")
        if not _DB_COL_RE.match(db_column):
            raise ValueError(f"Invalid db_column in proteome_column_meta: {db_column!r}")
        if "`" in mysql_type or ";" in mysql_type or not mysql_type:
            raise ValueError(f"Invalid mysql_type in proteome_column_meta for {db_column!r}")
        extras.append(r)
    return extras


def _mat_column_exists(conn, column_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME='proteomes_flat_mat' AND COLUMN_NAME=%s LIMIT 1",
            (conn.db.decode() if isinstance(conn.db, (bytes, bytearray)) else conn.db, column_name),
        )
        return cur.fetchone() is not None


def _ensure_mat_has_extra_columns(conn, extras):
    if not extras:
        return
    added = []
    with conn.cursor() as cur:
        for meta in extras:
            col_name = str(meta["tsv_header"]).strip()
            if _mat_column_exists(conn, col_name):
                continue

            mysql_type = str(meta["mysql_type"]).strip()
            nullable = bool(int(meta.get("nullable", 1)))
            default_value = meta.get("default_value")
            null_sql = "NULL" if nullable else "NOT NULL"
            ddl = f"ALTER TABLE proteomes_flat_mat ADD COLUMN `{col_name}` {mysql_type} {null_sql}"
            if default_value is not None:
                ddl += " DEFAULT %s"
                cur.execute(ddl, (default_value,))
            else:
                cur.execute(ddl)
            added.append(col_name)

    if added:
        print(f"Added {len(added)} column(s) to proteomes_flat_mat: {', '.join(added)}")


def _build_refresh_sql(extras):
    """Build the refresh SQL, appending any registered optional proteome columns."""

    base_insert_cols = [
        "`hash`",
        "`Origin CP`",
        "`web`",
        "`In NCBI's ref seq`",
        "`Species`",
        "`taxID`",
        "`species_taxid`",
        "`current_scientific_name`",
        "`common_names`",
        "`group_name`",
        "`informal_clade`",
        "`code_vFV`",
        "`File_name`",
        "`Filepath_original`",
        "`num_seqs`",
        "`sum_len`",
        "`min_len`",
        "`avg_len`",
        "`max_len`",
        "`File_snip_name`",
        "`Filepath_snip_processed`",
        "`Filepath_renamed_vFV`",
        "`num_seqs_snip_processed`",
        "`sum_len_snip_processed`",
        "`min_len_snip_processed`",
        "`avg_len_snip_processed`",
        "`max_len_snip_processed`",
        "`post_snip`",
        "`AssemblyID`",
        "`Complete BUSCO Domain`",
        "`Single BUSCO Domain`",
        "`Duplicated BUSCO Domain`",
        "`Fragmented BUSCO Domain`",
        "`Missing BUSCO Domain`",
        "`Complete BUSCO Kingdom`",
        "`Single BUSCO Kingdom`",
        "`Duplicated BUSCO Kingdom`",
        "`Fragmented BUSCO Kingdom`",
        "`Missing BUSCO Kingdom`",
        "`Domain`",
        "`Realm`",
        "`Kingdom`",
        "`Subkingdom`",
        "`Superphylum`",
        "`Phylum`",
        "`Subphylum`",
        "`Infraphylum`",
        "`Superclass`",
        "`Class`",
        "`Subclass`",
        "`Infraclass`",
        "`Cohort`",
        "`Subcohort`",
        "`Superorder`",
        "`Order`",
        "`Suborder`",
        "`Infraorder`",
        "`Parvorder`",
        "`Superfamily`",
        "`Family`",
        "`Subfamily`",
        "`Tribe`",
        "`Subtribe`",
        "`Genus`",
        "`Subgenus`",
        "`Section`",
        "`Subsection`",
        "`Series`",
        "`Subseries`",
        "`Species_group`",
        "`Species_subgroup`",
        "`Forma_specialis`",
        "`Subspecies`",
        "`VarietasSubvariety`",
        "`Forma`",
        "`Serogroup`",
        "`Serotype`",
        "`Strain`",
        "`Isolate`",
    ]

    base_select_exprs = [
        "p.hash",
        "p.origin_cp AS `Origin CP`",
        "p.web AS `web`",
        "p.in_ncbi_refseq AS `In NCBI's ref seq`",
        "p.species AS `Species`",
        "p.taxid AS `taxID`",
        "p.species_taxid AS `species_taxid`",
        "p.current_scientific_name AS `current_scientific_name`",
        "p.common_names AS `common_names`",
        "p.group_name AS `group_name`",
        "p.informal_clade AS `informal_clade`",
        "p.code_vfv AS `code_vFV`",
        "p.file_name AS `File_name`",
        "p.filepath_original AS `Filepath_original`",
        "p.num_seqs AS `num_seqs`",
        "p.sum_len AS `sum_len`",
        "p.min_len AS `min_len`",
        "p.avg_len AS `avg_len`",
        "p.max_len AS `max_len`",
        "p.file_snip_name AS `File_snip_name`",
        "p.filepath_snip_processed AS `Filepath_snip_processed`",
        "p.filepath_renamed_vfv AS `Filepath_renamed_vFV`",
        "p.num_seqs_snip_processed AS `num_seqs_snip_processed`",
        "p.sum_len_snip_processed AS `sum_len_snip_processed`",
        "p.min_len_snip_processed AS `min_len_snip_processed`",
        "p.avg_len_snip_processed AS `avg_len_snip_processed`",
        "p.max_len_snip_processed AS `max_len_snip_processed`",
        "p.post_snip AS `post_snip`",
        "p.assembly_id AS `AssemblyID`",
        "bsD.complete AS `Complete BUSCO Domain`",
        "bsD.single_copy AS `Single BUSCO Domain`",
        "bsD.duplicated AS `Duplicated BUSCO Domain`",
        "bsD.fragmented AS `Fragmented BUSCO Domain`",
        "bsD.missing AS `Missing BUSCO Domain`",
        "bsK.complete AS `Complete BUSCO Kingdom`",
        "bsK.single_copy AS `Single BUSCO Kingdom`",
        "bsK.duplicated AS `Duplicated BUSCO Kingdom`",
        "bsK.fragmented AS `Fragmented BUSCO Kingdom`",
        "bsK.missing AS `Missing BUSCO Kingdom`",
        "tx.`Domain`",
        "tx.`Realm`",
        "tx.`Kingdom`",
        "tx.`Subkingdom`",
        "tx.`Superphylum`",
        "tx.`Phylum`",
        "tx.`Subphylum`",
        "tx.`Infraphylum`",
        "tx.`Superclass`",
        "tx.`Class`",
        "tx.`Subclass`",
        "tx.`Infraclass`",
        "tx.`Cohort`",
        "tx.`Subcohort`",
        "tx.`Superorder`",
        "tx.`Order`",
        "tx.`Suborder`",
        "tx.`Infraorder`",
        "tx.`Parvorder`",
        "tx.`Superfamily`",
        "tx.`Family`",
        "tx.`Subfamily`",
        "tx.`Tribe`",
        "tx.`Subtribe`",
        "tx.`Genus`",
        "tx.`Subgenus`",
        "tx.`Section`",
        "tx.`Subsection`",
        "tx.`Series`",
        "tx.`Subseries`",
        "tx.`Species_group`",
        "tx.`Species_subgroup`",
        "tx.`Forma_specialis`",
        "tx.`Subspecies`",
        "tx.`VarietasSubvariety`",
        "tx.`Forma`",
        "tx.`Serogroup`",
        "tx.`Serotype`",
        "tx.`Strain`",
        "tx.`Isolate`",
    ]

    extra_insert_cols = []
    extra_select_exprs = []
    for meta in extras:
        header = str(meta["tsv_header"]).strip()
        db_col = str(meta["db_column"]).strip()
        extra_insert_cols.append(f"`{header}`")
        extra_select_exprs.append(f"p.`{db_col}` AS `{header}`")

    insert_cols_sql = ",\n  ".join(base_insert_cols + extra_insert_cols)
    select_exprs_sql = ",\n  ".join(base_select_exprs + extra_select_exprs)

    return f"""
DELETE FROM proteomes_flat_mat;

INSERT INTO proteomes_flat_mat (
  {insert_cols_sql}
)
SELECT
  {select_exprs_sql}
FROM proteome p
LEFT JOIN busco_summary bsD ON bsD.hash = p.hash AND bsD.`rank` = 'Domain'
LEFT JOIN busco_summary bsK ON bsK.hash = p.hash AND bsK.`rank` = 'Kingdom'
LEFT JOIN (
  SELECT
    pt.hash,
    MAX(CASE WHEN tt.level='Domain' THEN tt.name END) AS `Domain`,
    MAX(CASE WHEN tt.level='Realm' THEN tt.name END) AS `Realm`,
    MAX(CASE WHEN tt.level='Kingdom' THEN tt.name END) AS `Kingdom`,
    MAX(CASE WHEN tt.level='Subkingdom' THEN tt.name END) AS `Subkingdom`,
    MAX(CASE WHEN tt.level='Superphylum' THEN tt.name END) AS `Superphylum`,
    MAX(CASE WHEN tt.level='Phylum' THEN tt.name END) AS `Phylum`,
    MAX(CASE WHEN tt.level='Subphylum' THEN tt.name END) AS `Subphylum`,
    MAX(CASE WHEN tt.level='Infraphylum' THEN tt.name END) AS `Infraphylum`,
    MAX(CASE WHEN tt.level='Superclass' THEN tt.name END) AS `Superclass`,
    MAX(CASE WHEN tt.level='Class' THEN tt.name END) AS `Class`,
    MAX(CASE WHEN tt.level='Subclass' THEN tt.name END) AS `Subclass`,
    MAX(CASE WHEN tt.level='Infraclass' THEN tt.name END) AS `Infraclass`,
    MAX(CASE WHEN tt.level='Cohort' THEN tt.name END) AS `Cohort`,
    MAX(CASE WHEN tt.level='Subcohort' THEN tt.name END) AS `Subcohort`,
    MAX(CASE WHEN tt.level='Superorder' THEN tt.name END) AS `Superorder`,
    MAX(CASE WHEN tt.level='Order' THEN tt.name END) AS `Order`,
    MAX(CASE WHEN tt.level='Suborder' THEN tt.name END) AS `Suborder`,
    MAX(CASE WHEN tt.level='Infraorder' THEN tt.name END) AS `Infraorder`,
    MAX(CASE WHEN tt.level='Parvorder' THEN tt.name END) AS `Parvorder`,
    MAX(CASE WHEN tt.level='Superfamily' THEN tt.name END) AS `Superfamily`,
    MAX(CASE WHEN tt.level='Family' THEN tt.name END) AS `Family`,
    MAX(CASE WHEN tt.level='Subfamily' THEN tt.name END) AS `Subfamily`,
    MAX(CASE WHEN tt.level='Tribe' THEN tt.name END) AS `Tribe`,
    MAX(CASE WHEN tt.level='Subtribe' THEN tt.name END) AS `Subtribe`,
    MAX(CASE WHEN tt.level='Genus' THEN tt.name END) AS `Genus`,
    MAX(CASE WHEN tt.level='Subgenus' THEN tt.name END) AS `Subgenus`,
    MAX(CASE WHEN tt.level='Section' THEN tt.name END) AS `Section`,
    MAX(CASE WHEN tt.level='Subsection' THEN tt.name END) AS `Subsection`,
    MAX(CASE WHEN tt.level='Series' THEN tt.name END) AS `Series`,
    MAX(CASE WHEN tt.level='Subseries' THEN tt.name END) AS `Subseries`,
    MAX(CASE WHEN tt.level='Species_group' THEN tt.name END) AS `Species_group`,
    MAX(CASE WHEN tt.level='Species_subgroup' THEN tt.name END) AS `Species_subgroup`,
    MAX(CASE WHEN tt.level='Forma_specialis' THEN tt.name END) AS `Forma_specialis`,
    MAX(CASE WHEN tt.level='Subspecies' THEN tt.name END) AS `Subspecies`,
    MAX(CASE WHEN tt.level='VarietasSubvariety' THEN tt.name END) AS `VarietasSubvariety`,
    MAX(CASE WHEN tt.level='Forma' THEN tt.name END) AS `Forma`,
    MAX(CASE WHEN tt.level='Serogroup' THEN tt.name END) AS `Serogroup`,
    MAX(CASE WHEN tt.level='Serotype' THEN tt.name END) AS `Serotype`,
    MAX(CASE WHEN tt.level='Strain' THEN tt.name END) AS `Strain`,
    MAX(CASE WHEN tt.level='Isolate' THEN tt.name END) AS `Isolate`
  FROM proteome_taxonomy pt
  JOIN taxonomy_term tt ON tt.id = pt.term_id
  GROUP BY pt.hash
) tx ON tx.hash = p.hash;
"""


REFRESH_SQL = None


def _connect(args):
    kwargs = {
        "user": args.user,
        "password": args.password,
        "database": args.db,
        "charset": "utf8mb4",
        "autocommit": True,
        "cursorclass": pymysql.cursors.DictCursor,
    }
    if getattr(args, "unix_socket", None):
        kwargs["unix_socket"] = args.unix_socket
    else:
        kwargs["host"] = args.host
    return pymysql.connect(**kwargs)


def main():
    parser = argparse.ArgumentParser(description="Refresh materialized proteomes flat table")
    parser.add_argument('--host', '--db-host', dest='host', default=os.getenv('DB_HOST', 'localhost'))
    parser.add_argument('--user', '--db-user', dest='user', default=os.getenv('DB_USER', 'root'))
    parser.add_argument('--password', '--db-password', dest='password', default=os.getenv('DB_PASSWORD', ''))
    parser.add_argument('--db', '--db-name', dest='db', default=os.getenv('DB_NAME', 'bbc_proteomes'))
    parser.add_argument('--unix-socket', dest='unix_socket', default=os.getenv('DB_UNIX_SOCKET'))
    parser.add_argument('--create-ddl', action='store_true', help='Also apply db/materialized_flat.sql before refreshing')
    parser.add_argument('--ddl-path', default=os.path.join(os.path.dirname(__file__), '..', 'db', 'materialized_flat.sql'))
    args = parser.parse_args()

    start = time.time()
    with _connect(args) as conn:
        with conn.cursor() as cur:
            if args.create_ddl:
                ddl_path = os.path.abspath(args.ddl_path)
                with open(ddl_path, 'r', encoding='utf-8') as f:
                    ddl_lines = [
                        ln
                        for ln in f.read().splitlines()
                        if not ln.lstrip().startswith('--')
                    ]
                ddl = "\n".join(ddl_lines)
                for statement in [s.strip() for s in ddl.split(';') if s.strip()]:
                    cur.execute(statement)

            try:
                extras = _load_extra_proteome_columns(conn)
            except ProgrammingError as e:
                code = e.args[0] if e.args else None
                if code == 1146:
                    extras = []
                else:
                    raise

            if extras:
                print(f"Optional proteome columns registered: {', '.join([str(r['tsv_header']) for r in extras])}")
            _ensure_mat_has_extra_columns(conn, extras)

            refresh_sql = _build_refresh_sql(extras)
            for statement in [s.strip() for s in refresh_sql.split(';') if s.strip()]:
                cur.execute(statement)

            cur.execute('SELECT COUNT(*) FROM proteomes_flat_mat')
            row = cur.fetchone()
            # DictCursor returns {'COUNT(*)': N}
            count = next(iter(row.values())) if isinstance(row, dict) else row[0]

    elapsed = time.time() - start
    print(f"Refreshed proteomes_flat_mat: {count} rows in {elapsed:.2f}s")


if __name__ == '__main__':
    main()
