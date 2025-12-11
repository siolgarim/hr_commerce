# sync_city_parthner.py
# Загружает ровно колонки:
# city (text), operator (text), type (text), format (text),
# oc_rate_ps_min (float8), oc_rate_ps_max (float8)

import os, io, re, requests, psycopg2
import pandas as pd
import numpy as np

CITIES_PARTHNER_SHEET_URL = os.environ.get("CITIES_PARTHNER_SHEET_URL")
DATABASE_URL = os.environ["DATABASE_URL"]
TARGET_TABLE = os.environ.get("TARGET_TABLE") or "analytics.partner_cities_oc_rate"

# Жёстко ожидаемые колонки в таблице БД и какие форматы они должны иметь
EXPECTED_DB_COLS = ["city", "operator", "type", "format", "oc_rate_ps_min", "oc_rate_ps_max"]

# Алиасы для заголовков листа (если у вас русский/вариативные названия)
HEADER_ALIASES = {
    "город": "city",
    "city": "city",
    "town": "city",
    "operator": "operator",
    "оператор": "operator",
    "type": "type",
    "тип": "type",
    "format": "format",
    "формат": "format",
    "oc_rate_ps_min": "oc_rate_ps_min",
    "oc_rate_ps_max": "oc_rate_ps_max",
    "min": "oc_rate_ps_min",
    "max": "oc_rate_ps_max",
}

def make_csv_url(u: str) -> str:
    m_id  = re.search(r"/spreadsheets/d/([^/]+)/", u or "")
    m_gid = re.search(r"[?&]gid=(\d+)", u or "")
    if not m_id or not m_gid:
        raise ValueError(f"Bad Google Sheets URL (нужен gid=...). Получено: {u}")
    return f"https://docs.google.com/spreadsheets/d/{m_id.group(1)}/export?format=csv&gid={m_gid.group(1)}"

def get_csv_df(url_ui: str) -> pd.DataFrame:
    csv_url = make_csv_url(url_ui)
    print(f"[INFO] CSV export url = {csv_url}")
    r = requests.get(csv_url, timeout=30, headers={"User-Agent":"GH Actions sync"})
    r.raise_for_status()
    head = r.content[:200].lower()
    if b"<html" in head or b"doctype html" in head:
        raise RuntimeError("Google Sheets вернул HTML вместо CSV. Скорее всего лист приватный или ссылка неверная.")
    return pd.read_csv(io.BytesIO(r.content))

def clean_headers(cols):
    return (pd.Index(cols)
            .astype(str)
            .str.replace(r"[\r\n]+", " ", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
            .str.lower())

def map_headers(cols_index):
    mapped = []
    for c in cols_index:
        n = c.lower().strip()
        mapped.append(HEADER_ALIASES.get(n, n))
    return pd.Index(mapped)

def to_float_or_none(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    s = str(v).strip().replace("\u00a0", "")
    if s == "" or s.lower() in ("none","nan"):
        return None
    s = s.replace(" ", "").replace(",", ".")
    try:
        f = float(s)
        if np.isnan(f):
            return None
        return f
    except Exception:
        return None

def main():
    if not CITIES_PARTHNER_SHEET_URL:
        raise RuntimeError("CITIES_PARTHNER_SHEET_URL не задан.")

    # 1) загрузить лист и нормализовать заголовки
    df_raw = get_csv_df(CITIES_PARTHNER_SHEET_URL)
    df = df_raw.copy()
    df.columns = clean_headers(df.columns)
    df.columns = map_headers(df.columns)

    print(f"[INFO] sheet columns = {list(df.columns)}")
    print(f"[INFO] rows in sheet = {len(df)}")
    print(df.head(3))

    # 2) подключение к БД и проверка структуры — ожидаем ровно EXPECTED_DB_COLS
    if "." not in TARGET_TABLE:
        raise RuntimeError(f"TARGET_TABLE должен быть в формате schema.table. Получено: {TARGET_TABLE}")
    schema, table = TARGET_TABLE.split(".", 1)
    print(f"[INFO] target table = {schema}.{table}")

    conn = psycopg2.connect(
        DATABASE_URL,
        connect_timeout=10,
        options="-c lock_timeout=5000 -c statement_timeout=120000 -c application_name=gh_city_partner_oc_rate_sync"
    )

    try:
        with conn.cursor() as cur:
            cur.execute("""
              SELECT column_name
              FROM information_schema.columns
              WHERE table_schema=%s AND table_name=%s
              ORDER BY ordinal_position
            """, (schema, table))
            cols_db = [r[0].lower() for r in cur.fetchall()]

        if not cols_db:
            raise RuntimeError(f"Table {TARGET_TABLE} not found or no access.")

        print(f"[INFO] db columns = {cols_db}")

        # Требуем точное соответствие набора колонок
        if cols_db != EXPECTED_DB_COLS:
            raise RuntimeError(
                "Структура таблицы отличается от ожидаемой. Ожидается ровно колонки: "
                f"{EXPECTED_DB_COLS}. Текущие колонки: {cols_db}"
            )

        # 3) сформируем DF для загрузки строго в порядке cols_db
        # Если в листе нет какой-то колонки — создаём с None (NULL)
        df_upload = pd.DataFrame(index=df.index)
        for col in cols_db:
            if col in df.columns:
                df_upload[col] = df[col]
            else:
                df_upload[col] = None  # будет \N -> NULL

        # 4) конвертируем только числовые поля
        for num_col in ("oc_rate_ps_min", "oc_rate_ps_max"):
            df_upload[num_col] = df_upload[num_col].map(to_float_or_none).astype("Float64")

        # 5) подготовка CSV: используем '\N' как маркер NULL
        buf = io.StringIO()
        df_upload.to_csv(buf, index=False, header=True, na_rep="\\N")
        buf.seek(0)

        # 6) очистка таблицы и COPY
        used_delete = False
        try:
            with conn.cursor() as cur_lock:
                cur_lock.execute("BEGIN;")
                cur_lock.execute(f"LOCK TABLE {TARGET_TABLE} IN ACCESS EXCLUSIVE MODE NOWAIT;")
                cur_lock.execute(f"TRUNCATE {TARGET_TABLE};")
                cur_lock.execute("COMMIT;")
        except Exception:
            conn.rollback()
            used_delete = True
            with conn.cursor() as cur_del:
                cur_del.execute("BEGIN;")
                cur_del.execute(f"DELETE FROM {TARGET_TABLE};")
                cur_del.execute("COMMIT;")

        with conn.cursor() as cur_copy:
            cur_copy.execute("BEGIN;")
            copy_sql = f"COPY {TARGET_TABLE} ({', '.join(cols_db)}) FROM STDIN WITH CSV HEADER NULL '\\N' ENCODING 'UTF8'"
            cur_copy.copy_expert(copy_sql, buf)
            cur_copy.execute("COMMIT;")

    finally:
        try:
            conn.close()
        except Exception:
            pass

    print(f"OK | rows={len(df_upload)} | cols={len(cols_db)} | target={TARGET_TABLE} | mode={'DELETE' if used_delete else 'TRUNCATE'}")

if __name__ == "__main__":
    main()
