# sync_hr.py
import os, io, re, requests, psycopg2
import pandas as pd
import numpy as np

# В секретах укажи ссылку на лист "Численность РИМ" (gid=0)
SHEET_URL    = os.environ.get("SHEET_URL")     # из GitHub Secrets, например: .../edit?gid=0#gid=0
DATABASE_URL = os.environ["DATABASE_URL"]      # из GitHub Secrets
TARGET_TABLE = "analytics.hr_employees"        # целевая таблица в БД

def make_csv_url(u: str) -> str:
    m_id  = re.search(r"/spreadsheets/d/([^/]+)/", u)
    m_gid = re.search(r"[?&]gid=(\d+)", u)
    if not m_id or not m_gid:
        raise ValueError("Bad Google Sheets URL (need gid=...)")
    return f"https://docs.google.com/spreadsheets/d/{m_id.group(1)}/export?format=csv&gid={m_gid.group(1)}"

def get_csv_df(url: str) -> pd.DataFrame:
    r = requests.get(make_csv_url(url), timeout=30, headers={"User-Agent":"GH Actions sync"})
    r.raise_for_status()
    return pd.read_csv(io.BytesIO(r.content), encoding="utf-8")

def clean_headers(cols):
    # Убираем \r/\n, сжимаем пробелы, обрезаем по краям
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

# Русские -> английские имена после «склейки» заголовков
RENAME_MAP = {
    '№ авто':'row_no',
    'фио':'full_name','ФИО':'full_name',
    'Статус работы':'job_status',
    'Юр лицо':'legal_entity',
    'город':'city','Город':'city',
    'Подразделение (из штата)':'department_staff',
    'должность':'position','Должность':'position',

    # даты
    'дата выхода на работу ПЛАН':'start_date_plan',
    'Дата приема ФАКТ':'start_date_fact',
    'Дата оффера':'offer_date',
    'Дата велкома':'welcome_date',

    # флаги/описания
    'Оффер отклонен':'offer_rejected',
    'Формат велкома':'welcome_format',
    'тренер':'trainer','Тренер':'trainer',
    'hr':'hr_manager','HR':'hr_manager',
    'Источник найма':'hire_source',
    'ФИО реферальный':'referral_name',
    'Непосредственный руководитель':'direct_manager',
    'Функциональный руководитель':'functional_manager',

    # увольнение/стаж
    'Дата увольнения или перевода':'termination_or_transfer_date',
    'Стаж на СЕГОДНЯ или дату увольнения':'seniority_today_or_term',

    # обучение/адаптация
    'адаптация':'adaptation','Адаптация':'adaptation',
    'обучение':'training','Обучение':'training',

    'Причина отказа от оффера':'offer_rejection_reason',
    'Причина ухода':'resignation_reason',
    'EXIT интервью 1':'exit_interview_1',
    'Тип трудоустройства':'employment_type',

    # календари
    'Месяц приема':'hire_month',
    'Месяц увольнения':'termination_month',
    'Месяц оффера':'offer_month',
    'Год приема':'hire_year',
    'Год увольнения':'termination_year',
    'Год оффера':'offer_year',

    # месяцы-колонки
    '1':'m1','2':'m2','3':'m3','4':'m4','5':'m5','6':'m6',
    '7':'m7','8':'m8','9':'m9','10':'m10','11':'m11','12':'m12',

    # агрегаты/прочее
    'Прием в текущем месяце':'hired_current_month',
    'Уволено в текущем месяце':'terminated_current_month',
    'АУП/ОП':'aup_op',
    'Руководящая должность':'managerial_position',
    'Период от':'period_from',
    'Период до':'period_to',
    'Фактическая дата приема':'actual_hire_date',
    'Новый/Старый сотрудник':'employee_type',
    'Подразделение (обобщ.)':'department_general',
    'Отдел найма':'recruiting_department',
    'Испытательный срок':'probation',
    'Продажи КС':'sales_ks',
    'Должность КС':'position_ks',
    'Отдел продаж':'sales_department',
}

def main():
    # 1) читаем лист
    if not SHEET_URL:
        raise RuntimeError("SHEET_URL is not set. Put your Google Sheets link with gid=... into GitHub Secrets.")
    df_raw = get_csv_df(SHEET_URL)
    df = df_raw.copy()
    df.columns = clean_headers(df.columns)

    # 2) схема БД
    schema, table = TARGET_TABLE.split('.', 1)
    conn = psycopg2.connect(
        DATABASE_URL,
        connect_timeout=10,
        options="-c lock_timeout=5000 -c statement_timeout=120000 -c application_name=gh_hr_sync"
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
    db_types = {c: t for c, t in cols_db}
    db_cols_order = [c for c, _ in cols_db]

    # 3) построение маппинга колонок (пересечение по имени + карта RENAME_MAP)
    present = {}
    for col in df.columns:
        if col in db_cols_order:
            present[col] = col
        elif col in RENAME_MAP:
            present[col] = RENAME_MAP[col]
    keep = [c for c in df.columns if c in present]
    if not keep:
        raise RuntimeError("No columns matched between sheet headers and DB schema/RENAME_MAP.")
    df = df[keep].rename(columns=present).copy()

    # 4) приведение типов под схему БД
    for c, t in db_types.items():
        if c in df.columns and t == 'date':
            df[c] = df[c].map(to_date_iso)
    for c, t in db_types.items():
        if c in df.columns and t in {'integer','bigint','smallint'}:
            df[c] = df[c].map(to_int_or_none).astype("Int64")

    # итоговая последовательность колонок — как в БД, без updated_at
    load_cols = [c for c in db_cols_order if c in df.columns and c != 'updated_at']
    if not load_cols:
        raise RuntimeError("No common columns after mapping (after excluding updated_at).")

    # 5) соберём CSV-буфер для COPY
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
    
    # ИСПРАВЛЕНИЕ: заключаем имена колонок в двойные кавычки
    quoted_cols = [f'"{col}"' for col in load_cols]
    copy_sql = f"COPY {TARGET_TABLE} ({', '.join(quoted_cols)}) FROM STDIN WITH CSV HEADER ENCODING 'UTF8'"
    
    cur.copy_expert(copy_sql, buf)
    cur.execute("COMMIT;")
    cur.close()
    conn.close()

    print(f"OK | rows={len(df)} | cols={len(load_cols)} | mode={'DELETE' if used_delete else 'TRUNCATE'}")

if __name__ == "__main__":
    main()
