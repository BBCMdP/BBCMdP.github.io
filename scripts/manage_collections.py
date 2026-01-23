#!/usr/bin/env python3
"""Manage collections in the Proteomes DB.

This is intentionally a server-side admin tool (not exposed via the public API).

Examples:
  # Create/replace a collection from a file with one hash per line
  python scripts/manage_collections.py replace \
    --name New_collection_plants \
    --hashes-file hashes.txt

  # Add members to an existing collection
  python scripts/manage_collections.py add-members \
    --name New_collection_plants \
    --hashes hash1 hash2 hash3

  # Remove specific members
  python scripts/manage_collections.py remove-members \
    --name New_collection_plants \
    --hashes hash2

  # Delete a collection entirely (and its memberships)
  python scripts/manage_collections.py delete --name Archaea_NR

  # Bulk import memberships (TSV: collection<TAB>hash)
  python scripts/manage_collections.py import-tsv --tsv collections.tsv --mode replace

DB connection is controlled via flags or env vars:
  DB_HOST, DB_USER, DB_PASSWORD, DB_NAME
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from typing import Iterable, List, Optional, Sequence, Tuple

import pymysql


def _connect(args):
    return pymysql.connect(
        host=args.host,
        user=args.user,
        password=args.password,
        database=args.db,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )


def _fetchone_value(cur, sql: str, params: Sequence):
    cur.execute(sql, params)
    row = cur.fetchone()
    if not row:
        return None
    # Support both dict cursor and tuple cursor
    return next(iter(row.values())) if isinstance(row, dict) else row[0]


def _get_or_create_collection_id(cur, name: str) -> int:
    cur.execute("INSERT IGNORE INTO collection(name) VALUES (%s)", (name,))
    cid = _fetchone_value(cur, "SELECT id FROM collection WHERE name=%s", (name,))
    if cid is None:
        raise RuntimeError(f"Failed to resolve collection id for {name!r}")
    return int(cid)


def _get_collection_id(cur, name: str) -> Optional[int]:
    cid = _fetchone_value(cur, "SELECT id FROM collection WHERE name=%s", (name,))
    return int(cid) if cid is not None else None


def _normalize_hashes(hashes: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for h in hashes:
        if h is None:
            continue
        s = str(h).strip()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _read_hashes_file(path: str) -> List[str]:
    hashes: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            hashes.append(s)
    return _normalize_hashes(hashes)


def _apply_membership_changes(
    cur,
    *,
    collection_id: int,
    add: Sequence[str] = (),
    remove: Sequence[str] = (),
    replace: Optional[Sequence[str]] = None,
) -> Tuple[int, int]:
    """Apply membership changes. Returns (added_count, removed_count)."""

    added = 0
    removed_count = 0

    if replace is not None:
        cur.execute("DELETE FROM collection_membership WHERE collection_id=%s", (collection_id,))
        removed_count = cur.rowcount
        add = replace

    if remove:
        for h in _normalize_hashes(remove):
            cur.execute(
                "DELETE FROM collection_membership WHERE collection_id=%s AND hash=%s",
                (collection_id, h),
            )
            removed_count += cur.rowcount

    if add:
        for h in _normalize_hashes(add):
            cur.execute(
                "INSERT IGNORE INTO collection_membership(collection_id, hash) VALUES (%s, %s)",
                (collection_id, h),
            )
            # INSERT IGNORE sets rowcount to 1 if inserted, 0 if ignored
            added += int(cur.rowcount or 0)

    return added, removed_count


def cmd_list(args) -> int:
    with _connect(args) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT c.name, COUNT(cm.hash) AS members "
                "FROM collection c LEFT JOIN collection_membership cm ON cm.collection_id=c.id "
                "GROUP BY c.id, c.name ORDER BY c.name"
            )
            rows = cur.fetchall()

    if not rows:
        print("(no collections)")
        return 0

    for r in rows:
        print(f"{r['name']}\t{r['members']}")
    return 0


def cmd_delete(args) -> int:
    with _connect(args) as conn:
        with conn.cursor() as cur:
            cid = _get_collection_id(cur, args.name)
            if cid is None:
                print(f"Collection not found: {args.name}")
                return 1

            # membership rows will be removed via FK ON DELETE CASCADE if present;
            # otherwise, we remove them explicitly first.
            cur.execute("DELETE FROM collection_membership WHERE collection_id=%s", (cid,))
            cur.execute("DELETE FROM collection WHERE id=%s", (cid,))

        conn.commit()

    print(f"Deleted collection: {args.name}")
    return 0


def cmd_replace(args) -> int:
    hashes = _normalize_hashes(args.hashes or [])
    if args.hashes_file:
        hashes.extend(_read_hashes_file(args.hashes_file))
        hashes = _normalize_hashes(hashes)

    if not hashes:
        print("No hashes provided (refusing to create an empty collection).")
        return 2

    with _connect(args) as conn:
        with conn.cursor() as cur:
            cid = _get_or_create_collection_id(cur, args.name)
            added, removed = _apply_membership_changes(cur, collection_id=cid, replace=hashes)
        conn.commit()

    print(f"Collection {args.name}: removed {removed}, added {added}")
    return 0


def cmd_add_members(args) -> int:
    hashes = _normalize_hashes(args.hashes or [])
    if args.hashes_file:
        hashes.extend(_read_hashes_file(args.hashes_file))
        hashes = _normalize_hashes(hashes)

    if not hashes:
        print("No hashes provided.")
        return 2

    with _connect(args) as conn:
        with conn.cursor() as cur:
            cid = _get_or_create_collection_id(cur, args.name)
            added, _ = _apply_membership_changes(cur, collection_id=cid, add=hashes)
        conn.commit()

    print(f"Collection {args.name}: added {added}")
    return 0


def cmd_remove_members(args) -> int:
    hashes = _normalize_hashes(args.hashes or [])
    if args.hashes_file:
        hashes.extend(_read_hashes_file(args.hashes_file))
        hashes = _normalize_hashes(hashes)

    if not hashes:
        print("No hashes provided.")
        return 2

    with _connect(args) as conn:
        with conn.cursor() as cur:
            cid = _get_collection_id(cur, args.name)
            if cid is None:
                print(f"Collection not found: {args.name}")
                return 1
            _, removed = _apply_membership_changes(cur, collection_id=cid, remove=hashes)
        conn.commit()

    print(f"Collection {args.name}: removed {removed}")
    return 0


def _read_memberships_tsv(path: str) -> List[Tuple[str, str]]:
    memberships: List[Tuple[str, str]] = []
    with open(path, "r", encoding="utf-8") as f:
        # Allow either headerless TSV with 2 columns, or a header containing name/hash columns.
        sample = f.read(4096)
        f.seek(0)

        has_header = "collection" in sample.lower() and "hash" in sample.lower()
        reader = csv.reader(f, delimiter="\t")

        if has_header:
            dict_reader = csv.DictReader(f, delimiter="\t")
            for row in dict_reader:
                cname = (row.get("collection") or row.get("name") or "").strip()
                h = (row.get("hash") or "").strip()
                if cname and h:
                    memberships.append((cname, h))
            return memberships

        for row in reader:
            if not row:
                continue
            if row[0].lstrip().startswith("#"):
                continue
            if len(row) < 2:
                continue
            cname = str(row[0]).strip()
            h = str(row[1]).strip()
            if cname and h:
                memberships.append((cname, h))
    return memberships


def cmd_import_tsv(args) -> int:
    memberships = _read_memberships_tsv(args.tsv)
    if not memberships:
        print("No memberships found in TSV.")
        return 2

    # Group by collection name
    grouped = {}
    for cname, h in memberships:
        grouped.setdefault(cname, []).append(h)

    with _connect(args) as conn:
        with conn.cursor() as cur:
            for cname, hashes in grouped.items():
                cid = _get_or_create_collection_id(cur, cname)
                if args.mode == "replace":
                    _apply_membership_changes(cur, collection_id=cid, replace=_normalize_hashes(hashes))
                else:
                    _apply_membership_changes(cur, collection_id=cid, add=_normalize_hashes(hashes))
        conn.commit()

    print(f"Imported memberships for {len(grouped)} collections ({len(memberships)} rows)")
    return 0


def build_parser() -> argparse.ArgumentParser:
    # Global connection args.
    # We parse these separately so they can appear either before or after the subcommand.
    global_parser = argparse.ArgumentParser(add_help=False)
    global_parser.add_argument("--host", default=os.getenv("DB_HOST", "localhost"))
    global_parser.add_argument("--user", default=os.getenv("DB_USER", "root"))
    global_parser.add_argument("--password", default=os.getenv("DB_PASSWORD", ""))
    global_parser.add_argument("--db", default=os.getenv("DB_NAME", "bbc_proteomes"))

    p = argparse.ArgumentParser(
        description="Manage collections (create/update/delete) in the proteomes DB",
        parents=[global_parser],
    )

    sub = p.add_subparsers(dest="command", required=True)

    sp = sub.add_parser("list", help="List collections and member counts")
    sp.set_defaults(func=cmd_list)

    sp = sub.add_parser("delete", help="Delete a collection entirely")
    sp.add_argument("--name", required=True)
    sp.set_defaults(func=cmd_delete)

    def add_hash_args(sp2):
        sp2.add_argument("--name", required=True)
        sp2.add_argument("--hashes", nargs="*", default=[])
        sp2.add_argument("--hashes-file", help="File with one hash per line")

    sp = sub.add_parser("replace", help="Replace all members of a collection (refuses empty)")
    add_hash_args(sp)
    sp.set_defaults(func=cmd_replace)

    sp = sub.add_parser("add-members", help="Add members to a collection")
    add_hash_args(sp)
    sp.set_defaults(func=cmd_add_members)

    sp = sub.add_parser("remove-members", help="Remove specific members from a collection")
    add_hash_args(sp)
    sp.set_defaults(func=cmd_remove_members)

    sp = sub.add_parser("import-tsv", help="Bulk import memberships from TSV (collection<TAB>hash)")
    sp.add_argument("--tsv", required=True)
    sp.add_argument("--mode", choices=["add", "replace"], default="add")
    sp.set_defaults(func=cmd_import_tsv)

    return p


def _parse_args(argv: Optional[Sequence[str]] = None):
        """Parse args allowing global options before *or* after the subcommand.

        Example supported:
            manage_collections.py replace --name X --hashes-file f --host localhost
        as well as:
            manage_collections.py --host localhost replace --name X --hashes-file f
        """
        if argv is None:
                argv = sys.argv[1:]

        global_parser = argparse.ArgumentParser(add_help=False)
        global_parser.add_argument("--host", default=os.getenv("DB_HOST", "localhost"))
        global_parser.add_argument("--user", default=os.getenv("DB_USER", "root"))
        global_parser.add_argument("--password", default=os.getenv("DB_PASSWORD", ""))
        global_parser.add_argument("--db", default=os.getenv("DB_NAME", "bbc_proteomes"))

        global_args, remaining = global_parser.parse_known_args(list(argv))

        parser = build_parser()
        args = parser.parse_args(remaining)

        # If globals were provided, propagate them onto the final args namespace.
        for k in ("host", "user", "password", "db"):
                setattr(args, k, getattr(global_args, k))

        return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)

    try:
        return int(args.func(args))
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
