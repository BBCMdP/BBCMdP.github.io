#!/usr/bin/env python3
"""Manage optional proteome-level columns.

Goal:
- Allow adding/removing new proteome columns without editing importer code.
- Persist a registry in `proteome_column_meta` describing TSV header -> DB column mapping.

Notes:
- This script makes schema changes (ALTER TABLE). Use with care.
- `tsv_header` is also used as the display column name in `proteomes_flat_mat`.
"""

from __future__ import annotations

import argparse
import os
import re
from typing import Any, Dict, List, Optional

import pymysql


_DB_COL_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_db_column(name: str) -> str:
    if not _DB_COL_RE.match(name or ""):
        raise ValueError(f"Invalid db column name: {name!r} (must match {_DB_COL_RE.pattern})")
    return name


def _validate_header(header: str) -> str:
    header = (header or "").strip()
    if not header:
        raise ValueError("tsv_header cannot be empty")
    if "`" in header or ";" in header:
        raise ValueError("tsv_header cannot contain backticks or semicolons")
    return header


def _validate_mysql_type(mysql_type: str) -> str:
    t = (mysql_type or "").strip()
    if not t:
        raise ValueError("mysql_type cannot be empty")
    if "`" in t or ";" in t:
        raise ValueError("mysql_type cannot contain backticks or semicolons")
    return t


def connect(host: str, user: str, password: str, db: str, unix_socket: Optional[str] = None):
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


def _column_exists(cur, table: str, column: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s LIMIT 1",
        (cur.connection.db.decode() if isinstance(cur.connection.db, (bytes, bytearray)) else cur.connection.db, table, column),
    )
    return cur.fetchone() is not None


def _ensure_meta_table(cur):
    cur.execute(
        "CREATE TABLE IF NOT EXISTS proteome_column_meta ("
        "  id BIGINT AUTO_INCREMENT PRIMARY KEY,"
        "  tsv_header VARCHAR(255) NOT NULL,"
        "  db_column VARCHAR(255) NOT NULL,"
        "  mysql_type VARCHAR(255) NOT NULL,"
        "  nullable TINYINT(1) NOT NULL DEFAULT 1,"
        "  default_value TEXT NULL,"
        "  UNIQUE KEY uniq_tsv_header (tsv_header),"
        "  UNIQUE KEY uniq_db_column (db_column)"
        ")"
    )


def list_columns(conn):
    with conn.cursor() as cur:
        _ensure_meta_table(cur)
        cur.execute(
            "SELECT tsv_header, db_column, mysql_type, nullable, default_value "
            "FROM proteome_column_meta ORDER BY tsv_header"
        )
        rows = cur.fetchall()
    for r in rows:
        nullable = "NULL" if int(r["nullable"]) == 1 else "NOT NULL"
        dv = r["default_value"]
        dv_s = "" if dv is None else str(dv)
        print(f"{r['tsv_header']}\t{r['db_column']}\t{r['mysql_type']}\t{nullable}\t{dv_s}")


def add_column(
    conn,
    *,
    tsv_header: str,
    db_column: str,
    mysql_type: str,
    nullable: bool,
    default_value: Optional[str],
):
    tsv_header = _validate_header(tsv_header)
    db_column = _validate_db_column(db_column)
    mysql_type = _validate_mysql_type(mysql_type)

    # For NOT NULL columns, we require an explicit default so existing rows can be backfilled.
    # An empty-string default (""), common for VARCHAR, is valid and should be allowed.
    if not nullable and default_value is None:
        raise ValueError("NOT NULL columns require --default to backfill existing rows safely")

    with conn.cursor() as cur:
        _ensure_meta_table(cur)

        # Register/overwrite meta.
        cur.execute(
            "INSERT INTO proteome_column_meta(tsv_header, db_column, mysql_type, nullable, default_value) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE db_column=VALUES(db_column), mysql_type=VALUES(mysql_type), "
            "nullable=VALUES(nullable), default_value=VALUES(default_value)",
            (tsv_header, db_column, mysql_type, 1 if nullable else 0, default_value),
        )

        # Ensure DB column exists.
        if not _column_exists(cur, "proteome", db_column):
            null_sql = "NULL" if nullable else "NOT NULL"
            ddl = f"ALTER TABLE proteome ADD COLUMN `{db_column}` {mysql_type} {null_sql}"
            if default_value is not None:
                ddl += " DEFAULT %s"
                cur.execute(ddl, (default_value,))
            else:
                cur.execute(ddl)
        else:
            # Optionally enforce NOT NULL/DEFAULT changes could be added later.
            pass

    conn.commit()


def remove_column(conn, *, key: str, drop_db_column: bool, drop_mat_column: bool):
    key = (key or "").strip()
    if not key:
        raise ValueError("key cannot be empty")

    with conn.cursor() as cur:
        _ensure_meta_table(cur)
        cur.execute(
            "SELECT tsv_header, db_column FROM proteome_column_meta "
            "WHERE tsv_header=%s OR db_column=%s LIMIT 1",
            (key, key),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"No meta entry found for {key!r}")

        tsv_header = row["tsv_header"]
        db_column = row["db_column"]

        cur.execute(
            "DELETE FROM proteome_column_meta WHERE tsv_header=%s",
            (tsv_header,),
        )

        if drop_db_column and _column_exists(cur, "proteome", db_column):
            cur.execute(f"ALTER TABLE proteome DROP COLUMN `{db_column}`")

        # `proteomes_flat_mat` exposes optional columns using the TSV header as the column name.
        # If we remove the meta entry, the refresh script won't populate the column anymore,
        # but the column would still exist and would keep showing up via SHOW COLUMNS.
        if drop_mat_column and _column_exists(cur, "proteomes_flat_mat", tsv_header):
            cur.execute(f"ALTER TABLE proteomes_flat_mat DROP COLUMN `{tsv_header}`")

    conn.commit()


def main():
    parser = argparse.ArgumentParser(description="Manage optional proteome-level columns")
    parser.add_argument("--host", default=os.getenv("DB_HOST", "localhost"))
    parser.add_argument("--user", default=os.getenv("DB_USER", "root"))
    parser.add_argument("--password", default=os.getenv("DB_PASSWORD", ""))
    parser.add_argument("--db", default=os.getenv("DB_NAME", "bbc_proteomes"))
    parser.add_argument(
        "--unix-socket",
        default=os.getenv("DB_UNIX_SOCKET"),
        help="Connect via local MySQL UNIX socket instead of TCP host (useful for auth_socket)"
    )

    sub = parser.add_subparsers(dest="cmd", required=True)

    p_list = sub.add_parser("list", help="List registered optional columns")

    p_add = sub.add_parser("add", help="Register a new optional column and ALTER TABLE proteome")
    p_add.add_argument("--tsv-header", required=True, help="Exact TSV header name (also mat column label)")
    p_add.add_argument("--db-column", required=True, help="DB column name in proteome table (snake_case)")
    p_add.add_argument("--mysql-type", required=True, help="MySQL type, e.g. VARCHAR(128) or DOUBLE")
    p_add.add_argument("--nullable", action="store_true", help="Allow NULL values")
    p_add.add_argument("--not-null", action="store_true", help="Disallow NULL values")
    p_add.add_argument("--default", default=None, help="Default value for the column")

    p_rm = sub.add_parser("remove", help="Remove a registered optional column")
    p_rm.add_argument("key", help="tsv_header or db_column")
    p_rm.add_argument(
        "--drop-db-column",
        action="store_true",
        help="Also ALTER TABLE proteome DROP COLUMN (destructive)",
    )
    p_rm.add_argument(
        "--drop-mat-column",
        action="store_true",
        help="Also ALTER TABLE proteomes_flat_mat DROP COLUMN (uses the TSV header as column name)",
    )

    args = parser.parse_args()

    if args.cmd == "add":
        if args.nullable and args.not_null:
            raise SystemExit("Choose only one of --nullable / --not-null")
        nullable = True
        if args.not_null:
            nullable = False

        conn = connect(args.host, args.user, args.password, args.db, unix_socket=args.unix_socket)
        try:
            add_column(
                conn,
                tsv_header=args.tsv_header,
                db_column=args.db_column,
                mysql_type=args.mysql_type,
                nullable=nullable,
                default_value=args.default,
            )
        finally:
            conn.close()
        return

    if args.cmd == "remove":
        conn = connect(args.host, args.user, args.password, args.db, unix_socket=args.unix_socket)
        try:
            remove_column(
                conn,
                key=args.key,
                drop_db_column=args.drop_db_column,
                drop_mat_column=args.drop_mat_column,
            )
        finally:
            conn.close()
        return

    if args.cmd == "list":
        conn = connect(args.host, args.user, args.password, args.db, unix_socket=args.unix_socket)
        try:
            list_columns(conn)
        finally:
            conn.close()
        return


if __name__ == "__main__":
    main()
