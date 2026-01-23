from typing import Optional, List, Dict
import os
import csv
import io

import pymysql
from fastapi import FastAPI, HTTPException, Query, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pymysql.err import OperationalError


DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = os.getenv('DB_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DB_NAME = os.getenv('DB_NAME', 'bbc_proteomes')
VIEW_NAME = os.getenv('VIEW_NAME', 'view_proteomes_flat_v2')
DATA_SOURCE = os.getenv('DATA_SOURCE')
ADMIN_TOKEN = os.getenv('ADMIN_TOKEN')

_RESOLVED_SOURCE_NAME: Optional[str] = None
_AVAILABLE_COLS_CACHE: Dict[str, set] = {}


def _check_admin_token(token: Optional[str]):
    if not ADMIN_TOKEN:
        return
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Forbidden")


def _resolve_source_name(cur) -> str:
    """Resolve the fastest available data source.

    Prefers a materialized table when present, otherwise falls back to the view.
    """
    global _RESOLVED_SOURCE_NAME
    if _RESOLVED_SOURCE_NAME:
        return _RESOLVED_SOURCE_NAME

    candidates: List[str] = []
    if DATA_SOURCE:
        candidates.append(DATA_SOURCE)
    candidates.extend([
        'proteomes_flat_mat',
        VIEW_NAME,
    ])

    for name in candidates:
        cur.execute(f"SHOW FULL TABLES IN `{DB_NAME}` LIKE %s", (name,))
        if cur.fetchone():
            _RESOLVED_SOURCE_NAME = name
            return name

    # Fallback to configured view name even if not found (lets MySQL error clearly)
    _RESOLVED_SOURCE_NAME = VIEW_NAME
    return _RESOLVED_SOURCE_NAME


def _get_available_columns(cur, source: str) -> set:
    cached = _AVAILABLE_COLS_CACHE.get(source)
    if cached is not None:
        return cached
    cur.execute(f"SHOW COLUMNS FROM `{source}`")
    cols = {row['Field'] for row in cur.fetchall()}
    _AVAILABLE_COLS_CACHE[source] = cols
    return cols


def get_conn():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


app = FastAPI(title="BBC Proteomes API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(OperationalError)
def _handle_db_operational_error(request, exc: OperationalError):
    # Avoid leaking secrets; provide actionable error message.
    code = exc.args[0] if exc.args else None
    msg = exc.args[1] if len(exc.args) > 1 else str(exc)
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Database connection/query failed",
            "mysql_error": {"code": code, "message": msg},
        },
    )


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/columns")
def list_columns():
    """Get all available columns from the resolved data source."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            source = _resolve_source_name(cur)
            columns = sorted(_get_available_columns(cur, source))
            return {"columns": columns}


@app.post("/admin/reload")
def admin_reload(x_admin_token: Optional[str] = Header(None)):
    """Clear cached source/column info.

    Useful after schema changes or materialized-table column additions.
    If ADMIN_TOKEN is set, require X-Admin-Token header.
    """
    _check_admin_token(x_admin_token)
    global _RESOLVED_SOURCE_NAME
    _RESOLVED_SOURCE_NAME = None
    _AVAILABLE_COLS_CACHE.clear()
    return {"status": "ok"}


@app.get("/collections")
def list_collections():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT c.id, c.name, COUNT(cm.hash) AS count "
                "FROM collection c LEFT JOIN collection_membership cm ON cm.collection_id=c.id "
                "GROUP BY c.id, c.name ORDER BY c.name"
            )
            return cur.fetchall()


@app.get("/taxonomy/levels")
def list_taxonomy_levels():
    """Get all taxonomy levels with counts"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT level, COUNT(DISTINCT name) as count "
                "FROM taxonomy_term GROUP BY level ORDER BY level"
            )
            return cur.fetchall()


@app.get("/taxonomy/{level}")
def list_taxonomy_names(level: str):
    """Get all unique names for a taxonomy level"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT name FROM taxonomy_term WHERE level=%s ORDER BY name",
                (level,)
            )
            return [row['name'] for row in cur.fetchall()]


@app.get("/proteomes")
def list_proteomes(
    q: Optional[str] = Query(None, description="Search in species, code_vFV, current_scientific_name"),
    collection: Optional[str] = Query(None, description="Filter by collection name"),
    taxonomy_level: Optional[str] = Query(None, description="Taxonomy level, e.g., Phylum"),
    taxonomy_name: Optional[str] = Query(None, description="Taxon name at the given level"),
    busco_column: Optional[str] = Query(None, description="BUSCO column to filter on"),
    busco_min_value: Optional[float] = Query(None, description="Minimum BUSCO value (>=)"),
    sort_column: Optional[str] = Query(None, description="Column to sort by"),
    sort_order: str = Query("asc", pattern="^(asc|desc)$", description="Sort order: asc or desc"),
    columns: Optional[str] = Query(None, description="Comma-separated column names to return"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    # Parse columns
    if columns:
        selected_columns = [c.strip() for c in columns.split(',') if c.strip()]
    else:
        selected_columns = ['hash', 'Species', 'taxID', 'code_vFV', 'current_scientific_name']

    with get_conn() as conn:
        with conn.cursor() as cur:
            source = _resolve_source_name(cur)
            available_cols = _get_available_columns(cur, source)

            select_cols = ', '.join([f"s.`{c}`" for c in selected_columns])

            params: List = []
            from_sql = f"FROM `{source}` s"
            if collection:
                from_sql = (
                    "FROM (SELECT hash FROM collection_membership cm "
                    "JOIN collection c ON c.id = cm.collection_id WHERE c.name = %s) ch "
                    f"JOIN `{source}` s ON s.`hash` COLLATE utf8mb4_unicode_ci = ch.`hash` COLLATE utf8mb4_unicode_ci"
                )
                params.append(collection)

            where_clauses = []

            # Search filter
            if q:
                where_clauses.append("(s.`Species` LIKE %s OR s.`code_vFV` LIKE %s OR s.`current_scientific_name` LIKE %s)")
                like = f"%{q}%"
                params.extend([like, like, like])

            # Taxonomy filter
            # Fast-path: if the flattened column exists (Domain/Phylum/... as a column), filter directly.
            if taxonomy_level and taxonomy_name:
                if taxonomy_level in available_cols:
                    where_clauses.append(f"s.`{taxonomy_level}` = %s")
                    params.append(taxonomy_name)
                else:
                    where_clauses.append(
                        "s.`hash` COLLATE utf8mb4_unicode_ci IN ("
                        "SELECT pt.`hash` COLLATE utf8mb4_unicode_ci "
                        "FROM proteome_taxonomy pt "
                        "JOIN taxonomy_term tt ON tt.id=pt.term_id "
                        "WHERE tt.level=%s AND tt.name=%s)"
                    )
                    params.extend([taxonomy_level, taxonomy_name])

            # BUSCO filter
            if busco_column and busco_min_value is not None:
                if busco_column not in available_cols:
                    raise HTTPException(status_code=400, detail=f"Unknown column: {busco_column}")
                where_clauses.append(f"s.`{busco_column}` >= %s")
                params.append(busco_min_value)

            where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

            # Count total before pagination (do NOT wrap the full SELECT; avoid computing view expressions)
            count_sql = f"SELECT COUNT(*) as total {from_sql}{where_sql}"

            # Add sorting
            if sort_column:
                if sort_column not in available_cols:
                    raise HTTPException(status_code=400, detail=f"Unknown sort column: {sort_column}")
                sort_col = f"s.`{sort_column}`"
                sort_direction = "DESC" if sort_order == "desc" else "ASC"
                order_sql = f" ORDER BY {sort_col} {sort_direction}"
            else:
                order_sql = " ORDER BY s.`Species`, s.`code_vFV`"

            data_sql = f"SELECT {select_cols} {from_sql}{where_sql}{order_sql} LIMIT %s OFFSET %s"
            data_params = params + [limit, offset]

            cur.execute(count_sql, params)
            total = cur.fetchone()['total']

            cur.execute(data_sql, data_params)
            rows = cur.fetchall()
            return {"items": rows, "limit": limit, "offset": offset, "total": total}


@app.get("/proteomes/{hash}")
def get_proteome(hash: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            source = _resolve_source_name(cur)
            cur.execute(f"SELECT * FROM `{source}` WHERE hash=%s", (hash,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Proteome not found")
            # Include taxonomy terms
            cur.execute(
                "SELECT tt.level, tt.name FROM proteome_taxonomy pt JOIN taxonomy_term tt ON tt.id=pt.term_id WHERE pt.hash=%s ORDER BY tt.level",
                (hash,)
            )
            taxa = cur.fetchall()
            # Collections
            cur.execute(
                "SELECT c.name FROM collection_membership cm JOIN collection c ON c.id=cm.collection_id WHERE cm.hash=%s ORDER BY c.name",
                (hash,)
            )
            cols = [r['name'] for r in cur.fetchall()]
            return {"proteome": row, "taxonomy": taxa, "collections": cols}


@app.get("/export")
def export_proteomes(
    q: Optional[str] = Query(None),
    collection: Optional[str] = Query(None),
    taxonomy_level: Optional[str] = Query(None),
    taxonomy_name: Optional[str] = Query(None),
    busco_column: Optional[str] = Query(None),
    busco_min_value: Optional[float] = Query(None),
    sort_column: Optional[str] = Query(None, description="Column to sort by"),
    sort_order: str = Query("asc", pattern="^(asc|desc)$"),
    columns: Optional[str] = Query(None, description="Comma-separated column names"),
    format: str = Query("tsv", pattern="^(tsv|csv)$"),
):
    """Export filtered proteomes as TSV or CSV"""
    if columns:
        selected_columns = [c.strip() for c in columns.split(',') if c.strip()]
    else:
        selected_columns = []

    with get_conn() as conn:
        with conn.cursor() as cur:
            source = _resolve_source_name(cur)
            available_cols = _get_available_columns(cur, source)

            if selected_columns:
                for col in selected_columns:
                    if col not in available_cols:
                        raise HTTPException(status_code=400, detail=f"Unknown column: {col}")
                select_cols = ', '.join([f"s.`{c}`" for c in selected_columns])
            else:
                select_cols = "s.*"

            params: List = []
            from_sql = f"FROM `{source}` s"
            if collection:
                from_sql = (
                    "FROM (SELECT hash FROM collection_membership cm "
                    "JOIN collection c ON c.id = cm.collection_id WHERE c.name = %s) ch "
                    f"JOIN `{source}` s ON s.`hash` COLLATE utf8mb4_unicode_ci = ch.`hash` COLLATE utf8mb4_unicode_ci"
                )
                params.append(collection)

            where_clauses = []

            if q:
                where_clauses.append("(s.`Species` LIKE %s OR s.`code_vFV` LIKE %s OR s.`current_scientific_name` LIKE %s)")
                like = f"%{q}%"
                params.extend([like, like, like])

            if taxonomy_level and taxonomy_name:
                if taxonomy_level in available_cols:
                    where_clauses.append(f"s.`{taxonomy_level}` = %s")
                    params.append(taxonomy_name)
                else:
                    where_clauses.append(
                        "s.`hash` COLLATE utf8mb4_unicode_ci IN ("
                        "SELECT pt.`hash` COLLATE utf8mb4_unicode_ci "
                        "FROM proteome_taxonomy pt "
                        "JOIN taxonomy_term tt ON tt.id=pt.term_id "
                        "WHERE tt.level=%s AND tt.name=%s)"
                    )
                    params.extend([taxonomy_level, taxonomy_name])

            if busco_column and busco_min_value is not None:
                if busco_column not in available_cols:
                    raise HTTPException(status_code=400, detail=f"Unknown column: {busco_column}")
                where_clauses.append(f"s.`{busco_column}` >= %s")
                params.append(busco_min_value)

            where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

            if sort_column:
                if sort_column not in available_cols:
                    raise HTTPException(status_code=400, detail=f"Unknown sort column: {sort_column}")
                sort_col = f"s.`{sort_column}`"
                sort_direction = "DESC" if sort_order == "desc" else "ASC"
                order_sql = f" ORDER BY {sort_col} {sort_direction}"
            else:
                order_sql = " ORDER BY s.`Species`, s.`code_vFV`"

            sql = f"SELECT {select_cols} {from_sql}{where_sql}{order_sql}"
            cur.execute(sql, params)
            rows = cur.fetchall()
            
            if not rows:
                raise HTTPException(status_code=404, detail="No data matching filters")
            
            # Generate CSV/TSV
            output = io.StringIO()
            delimiter = '\t' if format == 'tsv' else ','
            writer = csv.DictWriter(output, fieldnames=rows[0].keys(), delimiter=delimiter)
            writer.writeheader()
            writer.writerows(rows)
            
            content = output.getvalue()
            output.close()
            
            media_type = "text/tab-separated-values" if format == 'tsv' else "text/csv"
            filename = f"bbc_proteomes_export.{format}"
            
            return StreamingResponse(
                io.StringIO(content),
                media_type=media_type,
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )


@app.post("/collections")
def create_or_update_collection(payload: Dict):
    name = payload.get('name')
    hashes: List[str] = payload.get('hashes', [])
    if not name:
        raise HTTPException(status_code=400, detail="Missing collection name")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT IGNORE INTO collection(name) VALUES (%s)", (name,))
            cur.execute("SELECT id FROM collection WHERE name=%s", (name,))
            cid = cur.fetchone()['id']
            # Replace membership if provided
            if hashes:
                cur.execute("DELETE FROM collection_membership WHERE collection_id=%s", (cid,))
                for h in hashes:
                    cur.execute(
                        "INSERT IGNORE INTO collection_membership(collection_id, hash) VALUES (%s, %s)",
                        (cid, h)
                    )
    return {"status": "ok", "collection": name, "count": len(hashes)}
