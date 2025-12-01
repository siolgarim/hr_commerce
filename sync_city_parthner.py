# sync_city_parthner.py
import os, io, re, requests, psycopg2
import pandas as pd
import numpy as np

# ── Конфиг из GitHub Secrets / env ──────────────────────────────────────
CITIES_PARTHNER_SHEET_URL = os.environ.get("CITIES_PARTHNER_SHEET_URL")  # ссылка на лист партнёров (обязательно с gid=...)
DATABASE_URL = os.environ["DATABASE_URL"]                               # строка подключения к Postgres

# целевая таблица: можно задать секретом/переменной TARGET_TABLE,
# иначе будет дефолт:
TARGET_TABLE = os.environ.get("TARGET_TABLE", "analytics.partner_cities_oc_rate")
# ────────────────────────────────────────────────────────────────────────

# Ожидаемые колонки на листе и в БД (если у партнёров другие — поменяй тут)
EXPECTED_COLS = ["city", "region", "type_city", "lop", "static", "digital"]

def make_csv_url(u: str) -> str:
    m_id  = re.search(r"/spreadsheets/d/([^/]+)/", u or "")
    m_gid = re.search(r"[?&]gid=(\d+)", u or "")
    if not m_id or not m_gid:
        raise ValueError("Bad Google Sheets URL (нужен gid=...)")
    return f"https://docs.google.com/spreadsheets/d/{m_id.group(1)}/export?format=csv&gid={m_gid.group(1)}"

def get_csv_df(url_ui: str) -> pd.DataFrame:
    csv_url = make_csv_url(url_ui)
    r = requests.get(csv_url, timeout=30, headers={"User-Agent":"GH Actions sync"})
    r.raise_for_status()
    return pd.read_csv(io.BytesIO(r.content), encoding="utf-8")

def clean_headers(cols):
    return (pd.Index(cols)
              .astype(str)
              .str.replace(r"[\r\n]+", " ", regex=True)
              .str.replace(r"\s+", " ", regex=True)
              .str.strip())

def to_int_or_none(v):
    if v is None or (isinstance(v, float) and np.isnan(v)): 
        return None
    s = str(v).strip().replace("\u00a0","")
    if s == "" or s.lower() in ("none","nan"): 
        return None
    s = s.replace(" ", "").replace(",", ".")
    try:
        f = float(s)
        if np.isnan(f): 
            return None
        return int(f)
    except Exception:
        m = re.match(r"^\s*([+-]?\d+)", s)
        return int(m.group(1)) if m else None

def main():
    # 1) читаем лист
    if not CITIES_PARTHNER_SHEET_URL:
        raise RuntimeError(
            "CITIES_PARTHNER_SHEET_URL не задан. "
            "Укажи ссылку на Google Sheets (с gid=...) в Secrets."
        )

    df_raw = get_csv_df(CITIES_PARTHNER_SHEET_URL)
    df = df_raw.copy()
    df.columns = clean_headers(df.columns)

    # 2) проверим и оставим только нужные колонки
    present = [c for c in EXPECTED_COLS if c in df.columns]
    if not present:
        raise RuntimeError(
            f"На листе нет ожидаемых колонок. "
            f"Нужны хотя бы из: {EXPECTED_COLS}. "
            f"Есть: {list(df.columns)}"
        )

    load_cols = [c for c in EXPECTED_COLS if c in present]
    df = df[load_cols].copy()

    # 3) привести числовые поля к int (если есть)
    for num_col in ("static", "digital"):
        if num_col in df.columns:
            df[num_col] = df[num_col].map(to_int_or_none).astype("Int64")

    # 4) подключение к БД и сверка схемы
    if "." not in TARGET_TABLE:
        raise RuntimeError("TARGET_TABLE должен быть в формате schema.table")

    schema, table = TARGET_TABLE.split('.', 1)
    conn = psycopg2.connect(
        DATABASE_URL,
        connect_timeout=10,
        options="-c lock_timeout=5000 -c statement_timeout=120000 -c application_name=gh_city_parthner_sync"
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
        raise RuntimeError(f"Table {TARGET_TABLE} not found or no access.")

    db_cols_order = [c for c, _ in cols_db]

    # пересечение колонок листа/БД
    load_cols = [c for c in EXPECTED_COLS if c in df.columns and c in db_cols_order]
    if not load_cols:
        raise RuntimeError(
            "Нет пересечения колонок между листом и таблицей БД. "
            f"Лист: {list(df.columns)}; БД: {db_cols_order}"
        )

    # 5) буфер для COPY
    buf = io.StringIO()
    df[load_cols].to_csv(buf, index=False)
    buf.seek(0)

    # 6) очистка и COPY
    used_delete = False
    try:
        cur.execute("BEGIN;")
        cur.execute(f"LOCK TABLE {TARGET_TABLE} IN ACCESS EXCLUSIVE MODE NOWAIT;")
        cur.execute(f"TRUNCATE {TARGET_TABLE};")
        cur.execute("COMMIT;")
    except Exception:
        conn.rollback()
        used_delete = True
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {TARGET_TABLE};")
        cur.execute("COMMIT;")

    cur = conn.cursor()
    cur.execute("BEGIN;")
    copy_sql = f"COPY {TARGET_TABLE} ({', '.join(load_cols)}) FROM STDIN WITH CSV HEADER ENCODING 'UTF8'"
    cur.copy_expert(copy_sql, buf)
    cur.execute("COMMIT;")
    cur.close()
    conn.close()

    print(
        f"OK | rows={len(df)} | cols={len(load_cols)} | "
        f"target={TARGET_TABLE} | mode={'DELETE' if used_delete else 'TRUNCATE'}"
    )

if __name__ == "__main__":
    main()
