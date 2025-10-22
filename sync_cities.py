# sync_cities.py
import os, io, re, requests, psycopg2
import pandas as pd
import numpy as np

# Новая таблица — Google Sheets (лист "Города")
SHEET_URL    = os.environ.get("CITIES_SHEET_URL")  # Укажи в Secrets
DATABASE_URL = os.environ["DATABASE_URL"]
TARGET_TABLE = "lop_cities"                         # Название таблицы в БД

def make_csv_url(u: str) -> str:
    m_id  = re.search(r"/spreadsheets/d/([^/]+)/", u)
    m_gid = re.search(r"[?&]gid=(\d+)", u)
    if not m_id or not m_gid:
        raise ValueError("Bad Google Sheets URL (need gid=...)")
    return f"https://docs.google.com/spreadsheets/d/{m_id.group(1)}/export?format=csv&gid={m_gid.group(1)}"

def get_csv_df(url: str) -> pd.DataFrame:
    r = requests.get(make_csv_url(url), timeout=30, headers={"User-Agent": "GH Actions sync"})
    r.raise_for_status()
    return pd.read_csv(io.BytesIO(r.content), encoding="utf-8")

def clean_headers(cols):
    return (pd.Index(cols)
              .astype(str)
              .str.replace(r"[\r\n]+", " ", regex=True)
              .str.replace(r"\s+", " ", regex=True)
              .str.strip())

def to_date_iso(v):
    if v is None or v == "": return None
    d = pd.to_datetime(v, dayfirst=True, errors="coerce")
    return None if pd.isna(d) else d.strftime("%Y-%m-%d")

def to_int_or_none(v):
    if v is None or (isinstance(v, float) and np.isnan(v)): return None
    s = str(v).strip().replace("\u00a0","")
    if s == "" or s.lower() in ("none","nan"): return None
    s = s.replace(" ", "").replace(",", ".")
    try:
        f = float(s)
        if np.isnan(f): return None
        return int(f)
    except:
        m = re.match(r"^\s*([+-]?\d+)", s)
        return int(m.group(1)) if m else None

def main():
    if not SHEET_URL:
        raise RuntimeError("CITIES_SHEET_URL is not set.")
    df = get_csv_df(SHEET_URL)
    df.columns = clean_headers(df.columns)

    schema, table = TARGET_TABLE.split('.', 1)
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
        ORDER BY ordinal_position
    """, (schema, table))
    cols_db = cur.fetchall()
    db_cols = [c for c, _ in cols_db]

    df = df[[c for c in df.columns if c in db_cols]]

    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)

    cur.execute("BEGIN;")
    cur.execute(f"TRUNCATE {TARGET_TABLE};")
    copy_sql = f"COPY {TARGET_TABLE} ({', '.join(df.columns)}) FROM STDIN WITH CSV HEADER ENCODING 'UTF8'"
    cur.copy_expert(copy_sql, buf)
    cur.execute("COMMIT;")
    cur.close(); conn.close()

    print(f"✅ Loaded {len(df)} rows into {TARGET_TABLE}")

if __name__ == "__main__":
    main()
