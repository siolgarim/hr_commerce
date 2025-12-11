# sync_city_parthner.py  (версия под analytics.partner_cities_oc_rate)
# Исправлена обработка заголовков (синонимы), парсинг чисел, логика выбора колонок,
# а также улучшено управление транзакциями/ресурсами.

import os, io, re, requests, psycopg2
import pandas as pd
import numpy as np

# ── Конфиг из GitHub Secrets / env ──────────────────────────────────────
CITIES_PARTHNER_SHEET_URL = os.environ.get("CITIES_PARTHNER_SHEET_URL")
DATABASE_URL = os.environ["DATABASE_URL"]

# дефолтная таблица; если секрет TARGET_TABLE пустой — всё равно возьмём дефолт
TARGET_TABLE = os.environ.get("TARGET_TABLE") or "analytics.partner_cities_oc_rate"
# ────────────────────────────────────────────────────────────────────────

# Ожидаемые колонки на листе и в БД
EXPECTED_COLS = ["region", "type_city", "oc_rate_ps", "oc_rate_ps_min", "oc_rate_ps_max"]

# Словарь соответствий: возможные заголовки на листе -> требуемые имена
HEADER_ALIASES = {
    "city": "region",
    "town": "region",
    "город": "region",
    "type": "type_city",
    "тип": "type_city",
    # oc_rate_ps может называться по-разному; оставим несколько вариантов
    "oc_rate_ps": "oc_rate_ps",
    "oc_rate": "oc_rate_ps",
    "rate": "oc_rate_ps",
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
    return (
        f"https://docs.google.com/spreadsheets/d/{m_id.group(1)}"
        f"/export?format=csv&gid={m_gid.group(1)}"
    )

def get_csv_df(url_ui: str) -> pd.DataFrame:
    csv_url = make_csv_url(url_ui)
    print(f"[INFO] CSV export url = {csv_url}")

    r = requests.get(csv_url, timeout=30, headers={"User-Agent":"GH Actions sync"})
    r.raise_for_status()

    # если пришёл HTML — лист закрыт/неправильные права
    head = r.content[:200].lower()
    if b"<html" in head or b"doctype html" in head:
        raise RuntimeError(
            "Google Sheets вернул HTML вместо CSV. "
            "Скорее всего лист приватный или ссылка неверная."
        )

    # pandas сам определит кодировку в большинстве случаев; если есть байты — используем BytesIO
    return pd.read_csv(io.BytesIO(r.content))

def clean_headers(cols):
    return (
        pd.Index(cols)
          .astype(str)
          .str.replace(r"[\r\n]+", " ", regex=True)
          .str.replace(r"\s+", " ", regex=True)
          .str.strip()
          .str.lower()
    )

def map_headers(cols_index):
    """
    Приводит колонки листа к ожидаемым именам, используя HEADER_ALIASES.
    Если колонка уже совпадает с ожидаемой — оставляет как есть.
    """
    mapped = []
    for c in cols_index:
        name = c.lower().strip()
        if name in HEADER_ALIASES:
            mapped.append(HEADER_ALIASES[name])
        else:
            # если уже совпадает с ожидаемой формой
            if name in EXPECTED_COLS:
                mapped.append(name)
            else:
                mapped.append(name)  # оставляем оригинал (вдруг нужен)
    return pd.Index(mapped)

def to_float_or_none(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    s = str(v).strip().replace("\u00a0","")
    if s == "" or s.lower() in ("none","nan"):
        return None
    # Убираем пробелы в числе и меняем запятую на точку
    s = s.replace(" ", "").replace(",", ".")
    try:
        f = float(s)
        if np.isnan(f):
            return None
        return f
    except Exception:
        return None

def main():
    # 1) читаем лист
    if not CITIES_PARTHNER_SHEET_URL:
        raise RuntimeError(
            "CITIES_PARTHNER_SHEET_URL не задан. "
            "Укажи ссылку на Google Sheets (с gid=...) в Secrets."
        )

    df_raw = get_csv_df(CITIES_PARTHNER_SHEET_URL)
    df = df_raw.copy()
    # нормализуем заголовки
    df.columns = clean_headers(df.columns)
    # затем сопоставляем с нужными именами (city -> region, type -> type_city и т.д.)
    df.columns = map_headers(df.columns)

    print(f"[INFO] sheet columns = {list(df.columns)}")
    print(f"[INFO] rows in sheet = {len(df)}")
    print("[INFO] sheet head:\n", df.head(5))

    # 2) оставляем только нужные колонки (попытка взять синонимы)
    present = [c for c in EXPECTED_COLS if c in df.columns]
    if not present:
        # Если нет точного пересечения, попробуем попытаться сопоставить частично
        # (например, в листе есть oc_rate_ps_min/max, но нет region)
        existing = list(df.columns)
        raise RuntimeError(
            f"На листе нет ожидаемых колонок. Нужны хотя бы из: {EXPECTED_COLS}. Есть: {existing}"
        )

    # load_cols = [c for c in EXPECTED_COLS if c in present]  <-- был баг: дублировали фильтр
    load_cols = present[:]  # берем все найденные ожидаемые колонки, в порядке EXPECTED_COLS
    # Сохраним порядок EXPECTED_COLS
    load_cols = [c for c in EXPECTED_COLS if c in load_cols]

    # Если колонок oc_rate_ps нет — добавим её с None (чтобы совпадать со схемой БД при необходимости)
    if "oc_rate_ps" not in df.columns and "oc_rate_ps" in EXPECTED_COLS:
        df["oc_rate_ps"] = None

    df = df[load_cols].copy()

    # 3) float-поля -> Float64 (в Postgres попадут как float8)
    for num_col in ("oc_rate_ps", "oc_rate_ps_min", "oc_rate_ps_max"):
        if num_col in df.columns:
            df[num_col] = df[num_col].map(to_float_or_none).astype("Float64")

    # 4) подключение к БД и сверка схемы
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
        cur = conn.cursor()

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

        # пересечение листа/БД
        load_cols = [c for c in EXPECTED_COLS if c in df.columns and c in cols_db]
        if not load_cols:
            raise RuntimeError(
                "Нет пересечения колонок между листом и таблицей БД. "
                f"Лист: {list(df.columns)}; БД: {cols_db}"
            )

        print(f"[INFO] will load cols = {load_cols}")

        # 5) буфер для COPY
        buf = io.StringIO()
        # to_csv — по умолчанию пишет пустые для NA; явно укажем header и index=False
        df[load_cols].to_csv(buf, index=False, header=True, na_rep="")
        buf.seek(0)

        # 6) очистка и COPY
        used_delete = False
        try:
            # Попытка TRUNCATE с эксклюзивной блокировкой
            with conn.cursor() as cur_lock:
                cur_lock.execute("BEGIN;")
                cur_lock.execute(f"LOCK TABLE {TARGET_TABLE} IN ACCESS EXCLUSIVE MODE NOWAIT;")
                cur_lock.execute(f"TRUNCATE {TARGET_TABLE};")
                cur_lock.execute("COMMIT;")
        except Exception:
            # если не получилось взять эксклюзивную блокировку — откат и DELETE как fallback
            conn.rollback()
            used_delete = True
            with conn.cursor() as cur_del:
                cur_del.execute("BEGIN;")
                cur_del.execute(f"DELETE FROM {TARGET_TABLE};")
                cur_del.execute("COMMIT;")

        # COPY
        with conn.cursor() as cur_copy:
            cur_copy.execute("BEGIN;")
            copy_sql = f"COPY {TARGET_TABLE} ({', '.join(load_cols)}) FROM STDIN WITH CSV HEADER ENCODING 'UTF8'"
            cur_copy.copy_expert(copy_sql, buf)
            cur_copy.execute("COMMIT;")

    finally:
        try:
            conn.close()
        except Exception:
            pass

    print(
        f"OK | rows={len(df)} | cols={len(load_cols)} | "
        f"target={TARGET_TABLE} | mode={'DELETE' if used_delete else 'TRUNCATE'}"
    )

if __name__ == "__main__":
    main()
