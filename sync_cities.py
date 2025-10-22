# sync_cities.py
import os, io, re, requests, psycopg2
import pandas as pd
import numpy as np

# --- конфиг из GitHub Secrets ---
SHEET_URL    = os.environ.get("CITIES_SHEET_URL")   # ссылка на лист "Города" (с gid=...)
DATABASE_URL = os.environ["DATABASE_URL"]           # строка подключения к Postgres
TARGET_TABLE = os.environ.get("TARGET_TABLE", "hr.lop_cities")  # можно переопределить секретом/vars

# ---------- утилиты ----------
def make_csv_url(u: str) -> str:
    """Превращает UI-ссылку вида .../edit?gid=0#gid=0 в CSV: .../export?format=csv&gid=0"""
    m_id  = re.search(r"/spreadsheets/d/([^/]+)/", u or "")
    m_gid = re.search(r"[?&]gid=(\d+)", u or "")
    if not m_id or not m_gid:
        raise ValueError("Bad Google Sheets URL (ожидаю /spreadsheets/d/<ID>/... и ?gid=...).")
    return f"https://docs.google.com/spreadsheets/d/{m_id.group(1)}/export?format=csv&gid={m_gid.group(1)}"

def get_csv_df(url_ui: str) -> pd.DataFrame:
    csv_url = make_csv_url(url_ui)
    r = requests.get(csv_url, timeout=30, headers={"User-Agent": "GH Actions sync"})
    r.raise_for_status()
    return pd.read_csv(io.BytesIO(r.content), encoding="utf-8")

def clean_headers(cols: pd.Index) -> pd.Index:
    # Склеиваем переносы, сжимаем пробелы, обрезаем
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
    s = str(v).strip().replace("\u00a0", "")
    if s == "" or s.lower() in ("none", "nan"): return None
    s = s.replace(" ", "").replace(",", ".")
    try:
        f = float(s)
        if np.isnan(f): return None
        return int(f)
    except Exception:
        m = re.match(r"^\s*([+-]?\d+)", s)
        return int(m.group(1)) if m else None

# ---------- основной сценарий ----------
def main():
    if not SHEET_URL:
        raise RuntimeError("CITIES_SHEET_URL не задан. Укажи ссылку на лист Google Sheets в Secrets.")

    # 1) читаем лист
    df_raw = get_csv_df(SHEET_URL)
    df = df_raw.copy()
    df.columns = clean_headers(df.columns)

    # 2) определяем схему и таблицу по TARGET_TABLE
    if '.' in TARGET_TABLE:
        schema, table = TARGET_TABLE.split('.', 1)
    else:
        schema, table = 'public', TARGET_TABLE  # если схемы нет — используем public

    # 3) соединяемся с БД и читаем фактическую схему таблицы
    conn = psycopg2.connect(
        DATABASE_URL,
        connect_timeout=10,
        options="-c lock_timeout=5000 -c statement_timeout=120000 -c application_name=gh_cities_sync"
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
        ORDER BY ordinal_position
    """, (schema, table))
    cols_db = cur.fetchall()
    if not cols_db:
        raise RuntimeError(f"Таблица {schema}.{table} не найдена или нет доступа.")

    db_types = {c: t for c, t in cols_db}
    db_cols_order = [c for c, _ in cols_db]

    # 4) оставляем только пересечение колонок (как есть по именам)
    keep = [c for c in df.columns if c in db_cols_order]
    if not keep:
        raise RuntimeError("Нет пересечения колонок между листом и таблицей БД. Проверь заголовки.")

    df = df[keep].copy()

    # 5) приведение типов под схему БД
    for c, t in db_types.items():
        if c in df.columns and t == 'date':
            df[c] = df[c].map(to_date_iso)
    for c, t in db_types.items():
        if c in df.columns and t in {'integer', 'bigint', 'smallint'}:
            df[c] = df[c].map(to_int_or_none).astype("Int64")

    # итоговый порядок колонок как в БД (исключая служебные вроде updated_at)
    load_cols = [c for c in db_cols_order if c in df.columns and c != 'updated_at']
    if not load_cols:
        raise RuntimeError("После мэппинга не осталось колонок для загрузки (кроме updated_at).")

    # 6) готовим CSV-буфер для COPY
    buf = io.StringIO()
    df[load_cols].to_csv(buf, index=False)
    buf.seek(0)

    full_table = f"{schema}.{table}"
    print(f"CSV rows: {len(df)} | target: {full_table} | cols: {len(load_cols)}")

    # 7) мягкая очистка: TRUNCATE (NOWAIT) -> при блокировке откат на DELETE
    used_delete = False
    try:
        cur.execute("BEGIN;")
        cur.execute(f"LOCK TABLE {full_table} IN ACCESS EXCLUSIVE MODE NOWAIT;")
        cur.execute(f"TRUNCATE {full_table};")
        cur.execute("COMMIT;")
    except Exception as e:
        conn.rollback()
        used_delete = True
        print(f"TRUNCATE недоступен ({type(e).__name__}): переключаюсь на DELETE ...")
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {full_table};")
        cur.execute("COMMIT;")

    # 8) copy
    cur = conn.cursor()
    cur.execute("BEGIN;")
    copy_sql = f"COPY {full_table} ({', '.join(load_cols)}) FROM STDIN WITH CSV HEADER ENCODING 'UTF8'"
    cur.copy_expert(copy_sql, buf)
    cur.execute("COMMIT;")
    cur.close(); conn.close()

    print(f"OK | rows={len(df)} | mode={'DELETE' if used_delete else 'TRUNCATE'}")

if __name__ == "__main__":
    main()
