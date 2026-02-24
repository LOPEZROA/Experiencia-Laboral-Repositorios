import os
import re
import io
import time
import pandas as pd
import psycopg2

# ========= CONFIG =========
PG_HOST = "localhost"
PG_PORT = 5433
PG_DB   = "postgres"       # <-- OJO: usa la DB donde creaste raw (en tus capturas era postgres)
PG_USER = "postgres"
PG_PASS = "1234"    # <-- pon tu clave real

EXCEL_DIR = r"C:\LE_NOGES\RAW_EXCEL"   # <-- cambia a tu carpeta real
SCHEMA = "raw"

# Mapeo tabla -> archivo (AJUSTA si tus nombres difieren)
FILES = {
    "cgr": "CGR.xlsx",
    "defunciones": "Defunciones.xlsx",
    "establecimientos": "Establecimientos.xlsx",

    "nomina_ic_abiertas": "Nomina_IC_22012026 por IDSIGTE_abiertas.xlsx",
    "nomina_ic_cerradas": "Nomina_IC_22012026 por IDSIGTE_cerradas.xlsx",

    "nomina_iq_abiertas": "Nomina_IQ_22012026 por IDSIGTE_abiertas.xlsx",
    "nomina_iq_cerradas": "Nomina_IQ_22012026 por IDSIGTE_cerradas.xlsx",

    "nomina_proc_abiertas": "Nomina_PROC_22012026 por IDSIGTE_abiertas.xlsx",
    "nomina_proc_cerradas": "Nomina_PROC_22012026 por IDSIGTE_cerradas.xlsx",

    "ss06_cerradas_historicas": "SS06 Valparaíso Cerradas Historicas Anterior 2020 Minsal 10.06.2024.xlsx",
}

# Tablas que deben leerse desde TODAS las hojas y apilarse
# None = todas las hojas; o puedes poner lista con nombres exactos: ["CGR 399","CGR 84"]
SHEETS = {
    "cgr": None
}

# Si True, TRUNCATE antes de cargar cada tabla
TRUNCATE_BEFORE_LOAD = True
# =======================


def norm_col(x: str) -> str:
    s = str(x).strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    return s


def get_table_cols(conn, table: str):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            ORDER BY ordinal_position;
        """, (SCHEMA, table))
        cols = [r[0] for r in cur.fetchall()]
    if not cols:
        raise RuntimeError(f"No encontré columnas para {SCHEMA}.{table}. ¿Existe la tabla?")
    return cols


def best_read_excel_sheet(path: str, sheet_name, expected_cols_norm):
    """
    Lee 1 hoja probando headers 0..4 y elige el que maximiza match con columnas esperadas.
    Retorna: df, header_used, score
    """
    best = None
    best_score = -1
    best_header = None

    for header in (0, 1, 2, 3, 4):
        try:
            df = pd.read_excel(path, sheet_name=sheet_name, header=header, engine="openpyxl")
        except Exception:
            continue

        cols = [norm_col(c) for c in df.columns]
        score = len(set(cols) & set(expected_cols_norm))

        if score > best_score:
            best_score = score
            best = df
            best_header = header

    if best is None:
        raise RuntimeError(f"No pude leer {path} hoja={sheet_name} con headers 0..4")

    best.columns = [norm_col(c) for c in best.columns]
    return best, best_header, best_score


def align_df(df: pd.DataFrame, expected_cols_norm):
    # agrega faltantes
    for c in expected_cols_norm:
        if c not in df.columns:
            df[c] = None

    # descarta extras y ordena
    df = df[expected_cols_norm].copy()

    # normaliza valores a texto, manteniendo None
    for c in df.columns:
        df[c] = df[c].map(lambda v: None if pd.isna(v) else str(v).strip())

    return df


def read_excel_for_table(path: str, table: str, expected_cols_norm):
    """
    Si la tabla está en SHEETS -> lee múltiples hojas y apila.
    Retorna: (df_final, resumen_por_hoja:list[dict])
    """
    if table in SHEETS:
        xls = pd.ExcelFile(path, engine="openpyxl")
        sheet_list = xls.sheet_names if SHEETS[table] is None else SHEETS[table]

        dfs = []
        resumen = []
        for sh in sheet_list:
            df_sh, header_used, score = best_read_excel_sheet(path, sh, expected_cols_norm)
            df_sh = align_df(df_sh, expected_cols_norm)

            # Si existe columna "origen", usa nombre de hoja para rastrear
            if "origen" in df_sh.columns:
                # rellena si está vacío
                col = df_sh["origen"].astype(str).str.strip()
                if (col == "").all() or (col == "None").all():
                    df_sh["origen"] = sh

            dfs.append(df_sh)
            resumen.append({
                "hoja": str(sh),
                "header": int(header_used),
                "match": int(score),
                "filas": int(len(df_sh)),
            })

        if not dfs:
            raise RuntimeError(f"No se pudo leer ninguna hoja de {path} para {table}")

        df_final = pd.concat(dfs, ignore_index=True)
        return df_final, resumen

    # Caso normal: solo primera hoja (index 0)
    df, header_used, score = best_read_excel_sheet(path, 0, expected_cols_norm)
    df = align_df(df, expected_cols_norm)
    resumen = [{
        "hoja": "0",
        "header": int(header_used),
        "match": int(score),
        "filas": int(len(df)),
    }]
    return df, resumen


def truncate_and_copy(conn, table: str, df: pd.DataFrame):
    with conn.cursor() as cur:
        if TRUNCATE_BEFORE_LOAD:
            cur.execute(f'TRUNCATE "{SCHEMA}"."{table}";')

        buf = io.StringIO()
        df.to_csv(buf, index=False, header=True, sep="\t", na_rep="", lineterminator="\n")
        buf.seek(0)

        col_list = ",".join([f'"{c}"' for c in df.columns])
        copy_sql = f'''
            COPY "{SCHEMA}"."{table}" ({col_list})
            FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER E'\\t', NULL '');
        '''
        cur.copy_expert(copy_sql, buf)


def count_table(conn, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."{table}";')
        return int(cur.fetchone()[0])


def main():
    t0 = time.time()

    # Validación rápida de carpeta
    if not os.path.isdir(EXCEL_DIR):
        raise FileNotFoundError(f"EXCEL_DIR no existe: {EXCEL_DIR}")

    # Conexión PG
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    conn.autocommit = False

    try:
        print(f"Conectado a DB='{PG_DB}' como user='{PG_USER}' en {PG_HOST}:{PG_PORT}")
        print(f"Cargando esquema: {SCHEMA}")
        print(f"Carpeta Excel: {EXCEL_DIR}")

        for table, fname in FILES.items():
            path = os.path.join(EXCEL_DIR, fname)
            if not os.path.exists(path):
                raise FileNotFoundError(f"No existe el archivo para {SCHEMA}.{table}: {path}")

            cols = get_table_cols(conn, table)
            expected_cols_norm = [norm_col(c) for c in cols]

            print(f"\n=== Cargando {SCHEMA}.{table} <= {fname}")

            # Leer excel (1 o muchas hojas)
            df, resumen = read_excel_for_table(path, table, expected_cols_norm)

            # Imprimir resumen por hoja (sin variables fuera de scope)
            for r in resumen:
                print(f"  - hoja='{r['hoja']}' | header={r['header']} | match={r['match']}/{len(expected_cols_norm)} | filas={r['filas']}")

            # Cargar
            before = count_table(conn, table)
            truncate_and_copy(conn, table, df)
            after = count_table(conn, table)

            print(f"OK {SCHEMA}.{table}: filas antes={before} -> después={after}")

        conn.commit()
        print(f"\n✅ Carga completa (10 tablas) en {time.time() - t0:.1f} segundos.")

    except Exception as e:
        conn.rollback()
        print("\n❌ ERROR. Se hizo ROLLBACK.")
        raise e
    finally:
        conn.close()


if __name__ == "__main__":
    main()