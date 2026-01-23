# BBC Proteomes Database - Deployment Guide

## Overview
This system consists of:
1. **MySQL Database** - Stores 35K+ proteomes with taxonomy, BUSCO metrics, and collections
2. **FastAPI Backend** - REST API for querying and exporting data
3. **HTML/JS Frontend** - Interactive web interface on GitHub Pages

## Local Development Setup

### 1. Start the API Server

```bash
cd /home/fer/Desktop/BBC_proteomes_DB

# Activate virtual environment
source .venv/bin/activate

# Set environment variables
export DB_HOST=localhost
export DB_USER=root
export DB_PASSWORD=YOUR_MYSQL_PASSWORD
export DB_NAME=bbc_proteomes

# If your MySQL root uses auth_socket (common on Ubuntu/Debian), prefer a UNIX socket:
# export DB_UNIX_SOCKET=/var/run/mysqld/mysqld.sock

# Optional (recommended): use the materialized flat table for fast filtering
# If unset, the API will auto-pick `proteomes_flat_mat` when present.
export DATA_SOURCE=proteomes_flat_mat

# Start the API server
uvicorn api.server:app --host 0.0.0.0 --port 8000 --reload
```

The API will be available at `http://localhost:8000`

### Optional but Recommended: Materialize the Flat Table

The original `view_proteomes_flat_v2` is built from many correlated subqueries and can be slow.
For fast filtering/sorting, create and refresh the materialized table once after importing data:

```bash
# Create the table (idempotent)
mysql -h "$DB_HOST" -u "$DB_USER" -p "$DB_NAME" < db/materialized_flat.sql

# Populate it
python scripts/refresh_flat_table.py --create-ddl
```

### 2. Configure Frontend

Edit `assets/proteomes.js` line 3:
```javascript
const API_BASE = 'http://localhost:8000';
```

### 3. Test Locally

Open `proteomes.html` in a browser. You should be able to:
- Load collections and taxonomy levels
- Apply filters
- View paginated results
- Export TSV/CSV files

## Production Deployment

### Option A: Deploy API to a Cloud VM (Recommended)

#### 1. Set up a Linux VM (Ubuntu/Debian)

```bash
# Install dependencies
sudo apt update
sudo apt install python3-pip python3-venv mysql-client nginx

# Copy project files
scp -r /home/fer/Desktop/BBC_proteomes_DB user@your-server:/opt/bbc_proteomes_db

# On the server
cd /opt/bbc_proteomes_db
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

#### 2. Set up MySQL

Either:
- **Option 1**: Use the same server (install MySQL locally)
- **Option 2**: Use a managed database (AWS RDS, Google Cloud SQL, etc.)
- **Option 3**: Keep database on your local machine and use SSH tunnel

For SSH tunnel (if DB is local):
```bash
ssh -L 3306:localhost:3306 user@your-local-machine -N
```

#### 3. Configure systemd service

Create `/etc/systemd/system/bbc-proteomes-api.service`:

```ini
[Unit]
Description=BBC Proteomes API
After=network.target

[Service]
Type=simple
User=www-data
WorkingDirectory=/opt/bbc_proteomes_db
Environment="DB_HOST=localhost"
Environment="DB_USER=root"
Environment="DB_PASSWORD=YOUR_PASSWORD"
Environment="DB_NAME=bbc_proteomes"
ExecStart=/opt/bbc_proteomes_db/.venv/bin/uvicorn api.server:app --host 0.0.0.0 --port 8000
Restart=always

[Install]
WantedBy=multi-user.target
```

Start the service:
```bash
sudo systemctl daemon-reload
sudo systemctl enable bbc-proteomes-api
sudo systemctl start bbc-proteomes-api
sudo systemctl status bbc-proteomes-api
```

#### 4. Configure Nginx reverse proxy

Create `/etc/nginx/sites-available/bbc-proteomes`:

```nginx
server {
    listen 80;
    server_name proteomes.yourdomain.com;

    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        
        # CORS headers (if needed)
        add_header 'Access-Control-Allow-Origin' '*' always;
        add_header 'Access-Control-Allow-Methods' 'GET, POST, OPTIONS' always;
        add_header 'Access-Control-Allow-Headers' 'Content-Type' always;
    }
}
```

Enable and restart:
```bash
sudo ln -s /etc/nginx/sites-available/bbc-proteomes /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

#### 5. Add SSL (recommended)

```bash
sudo apt install certbot python3-certbot-nginx
sudo certbot --nginx -d proteomes.yourdomain.com
```

#### 6. Update Frontend

Edit `assets/proteomes.js`:
```javascript
const API_BASE = 'https://proteomes.yourdomain.com';
```

Commit and push to GitHub Pages.

### Option B: Keep API Local + ngrok (Quick Test)

For testing without a server:

```bash
# Install ngrok
wget https://bin.equinox.io/c/bNyj1mQVY4c/ngrok-v3-stable-linux-amd64.tgz
tar -xvzf ngrok-v3-stable-linux-amd64.tgz
sudo mv ngrok /usr/local/bin/

# Start API locally
uvicorn api.server:app --host 0.0.0.0 --port 8000

# In another terminal, expose via ngrok
ngrok http 8000
```

Copy the ngrok URL (e.g., `https://abc123.ngrok.io`) and update `assets/proteomes.js`:
```javascript
const API_BASE = 'https://abc123.ngrok.io';
```

**Note**: Free ngrok URLs change on restart.

### Option C: Static Export (No Backend Required)

If you can't host a backend, export collections as static JSON:

```python
# Add to scripts/export_static_json.py
import json
import pymysql

conn = pymysql.connect(host='localhost', user='root', password='...', database='bbc_proteomes')

collections = {}
with conn.cursor() as cur:
    cur.execute("SELECT DISTINCT name FROM collection")
    for row in cur.fetchall():
        name = row['name']
        cur.execute("""
            SELECT * FROM view_proteomes_flat 
            WHERE hash IN (
                SELECT cm.hash FROM collection_membership cm 
                JOIN collection c ON c.id=cm.collection_id 
                WHERE c.name=%s
            )
        """, (name,))
        collections[name] = cur.fetchall()

with open('assets/data/proteomes_collections.json', 'w') as f:
    json.dump(collections, f)

conn.close()
```

Then modify the frontend to load from JSON instead of API calls.

## API Endpoints

- `GET /health` - Health check
- `GET /columns` - List all columns
- `GET /collections` - List all collections with counts
- `GET /taxonomy/levels` - List taxonomy levels
- `GET /taxonomy/{level}` - List names for a level
- `GET /proteomes` - Search/filter proteomes (paginated)
- `GET /proteomes/{hash}` - Get full proteome details

## Optional Proteome Columns (TSV-driven)

You can add new proteome-level columns (string/float/int/bool) without editing the importer by registering them in the DB metadata table `proteome_column_meta`.

### Add a new column

Important: in bash/zsh you must quote MySQL types that contain parentheses.

```bash
python3 scripts/manage_proteome_columns.py add \
    --tsv-header code_vAth \
    --db-column code_vath \
    --mysql-type 'VARCHAR(128)' \
    --not-null \
    --default ''
```

### Populate/update values

Create a delta TSV with at least `Hash` and your new TSV header (e.g. `code_vAth`), then run:

```bash
python3 scripts/import_tsv.py --tsv path/to/delta.tsv --host "$DB_HOST" --user "$DB_USER" --password "$DB_PASSWORD" --db "$DB_NAME"
python3 scripts/refresh_flat_table.py --create-ddl

# If using auth_socket:
# python3 scripts/import_tsv.py --tsv path/to/delta.tsv --unix-socket "$DB_UNIX_SOCKET" --user root --db "$DB_NAME"
# python3 scripts/refresh_flat_table.py --create-ddl --unix-socket "$DB_UNIX_SOCKET" --user root --db "$DB_NAME"
```

If the API is running and you want it to pick up new columns immediately, call `POST /admin/reload` (and send `X-Admin-Token` if you set `ADMIN_TOKEN`).

### Remove a column

Unregister the column (and optionally drop it from the `proteome` table):

```bash
python3 scripts/manage_proteome_columns.py remove code_vAth
# or destructive:
python3 scripts/manage_proteome_columns.py remove code_vAth --drop-db-column
```
- `GET /export` - Export as TSV/CSV
- `POST /collections` - Create/update custom collection

## Monitoring & Maintenance

### Check API logs
```bash
sudo journalctl -u bbc-proteomes-api -f
```

### Update data
Re-run the importer:
```bash
python scripts/import_tsv.py --tsv updated_data.tsv --host localhost --user root --password ... --db bbc_proteomes
```

Importer semantics (important for incremental updates):
- You can import a TSV with *only the rows you want to add/update* (keyed by `Hash`).
- Missing columns (not present in the TSV header) do **not** modify existing DB fields.
- Empty cells in present columns are applied as empty/NULL/0 depending on the field.

After updating data, refresh the materialized table used by the API:

```bash
python scripts/refresh_flat_table.py
```

### Manage collections (add/replace/delete)

Collections live in the database as:
- `collection` (collection names)
- `collection_membership` (members, as `hash` values)

To add a new collection from a list of hashes:

```bash
python scripts/manage_collections.py replace \
    --name New_collection_plants \
    --hashes-file hashes.txt
```

To delete a collection entirely:

```bash
python scripts/manage_collections.py delete --name Archaea_NR
```

Note: `scripts/import_tsv.py` will (by default) auto-create the preset collection names it knows about.
If you intentionally deleted a preset collection name and want it to stay deleted, run imports with:

```bash
python scripts/import_tsv.py --tsv updated_data.tsv --no-ensure-collections
```

### Backup database
```bash
mysqldump -u root -p bbc_proteomes > backup_$(date +%F).sql
```

## Troubleshooting

### CORS errors
Make sure Nginx or API has proper CORS headers (already configured in `api/server.py`).

### Database connection errors
- Check `DB_HOST`, `DB_USER`, `DB_PASSWORD` environment variables
- Verify MySQL is running: `sudo systemctl status mysql`
- Test connection: `mysql -h localhost -u root -p bbc_proteomes`

### Frontend not loading data
- Open browser DevTools (F12) → Console tab
- Check for errors (e.g., "Failed to fetch")
- Verify API URL in `assets/proteomes.js`
- Test API directly: `curl http://localhost:8000/health`
