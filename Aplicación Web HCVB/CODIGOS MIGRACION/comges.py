import io
import os
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import psycopg2

# =========================
# Configuracion
# =========================
PG_HOST = str(os.getenv("POSTGRES_HOST", "127.0.0.1")).strip()
PG_PORT = int(str(os.getenv("POSTGRES_PORT", "5433")).strip())
PG_DB = str(os.getenv("POSTGRES_DB", "postgres")).strip()
PG_USER = str(os.getenv("POSTGRES_USER", "postgres")).strip()
PG_PASS = str(os.getenv("POSTGRES_PASSWORD", "1234")).strip()

SCHEMA = "raw"
TABLE = "comges_especiales"
TRUNCATE_BEFORE_LOAD = True
HEADER_SCAN_MAX = 4

DEFAULT_XLSX = Path(__file__).resolve().parents[1] / "comges_especiales.xlsx"
XLSX_PATH = Path(r"C:\Users\nicol\OneDrive\Desktop\apps HCVB\App MVP SQL\comges_especiales.xlsx")

TARGET_COLUMNS = [
    "sigte_id",
    "run",
    "dv",
    "sexo",
    "estab_dest",
    "f_entrada",
    "f_salida",
    "c_salida",
]

COLUMN_CANDIDATES: Dict[str, List[str]] = {
    "sigte_id": ["sigte_id", "sigte_id2", "sigte_id.1", "sigte_id22"],
    "run": ["run", "run_persona"],
    "dv": ["dv", "dv_persona"],
    "sexo": ["sexo", "genero"],
    "estab_dest": ["estab_dest", "establecimiento de destino", "estab destino"],
    "f_entrada": ["f_entrada", "fechaingreso", "fecha_entrada", "fentrada"],
    "f_salida": ["f_salida", "fecha_salida", "fsalida"],
    "c_salida": ["c_salida", "csalida", "codigo_salida"],
}


def norm_col(value: str) -> str:
    s = str(value).strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-z0-9_\.]", "", s)
    return s


def clean_text(value: object) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    txt = str(value).strip()
    return txt if txt else None


def clean_id_like(value: object) -> Optional[str]:
    txt = clean_text(value)
    if txt is None:
        return None
    txt = txt.replace(" ", "")
    if txt.endswith(".0"):
        txt = txt[:-2]
    txt = re.sub(r"\.0+$", "", txt)
    txt = txt.strip()
    return txt if txt else None


def clean_dv(value: object) -> Optional[str]:
    txt = clean_text(value)
    if txt is None:
        return None
    txt = txt.replace(".", "").replace("-", "").replace(" ", "").upper()
    return txt if txt else None


def clean_date_text(value: object) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()

    if isinstance(value, (int, float)):
        num = float(value)
        if 1 <= num <= 200000:
            dt = pd.to_datetime(num, unit="D", origin="1899-12-30", errors="coerce")
            if pd.notna(dt):
                return pd.Timestamp(dt).date().isoformat()

    txt = str(value).strip()
    if not txt:
        return None

    dt = pd.to_datetime(txt, errors="coerce", dayfirst=True)
    if pd.notna(dt):
        return pd.Timestamp(dt).date().isoformat()
    return txt


def pick_first_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    norm_to_real: Dict[str, str] = {}
    for col in df.columns:
        key = norm_col(str(col))
        if key not in norm_to_real:
            norm_to_real[key] = str(col)

    for cand in candidates:
        key = norm_col(cand)
        if key in norm_to_real:
            return norm_to_real[key]

    for cand in candidates:
        key = norm_col(cand)
        for ncol, real_col in norm_to_real.items():
            if key and key in ncol:
                return real_col
    return None


def best_read_sheet(path: Path, sheet_name: str) -> Tuple[pd.DataFrame, int, int]:
    expected_keys = {
        norm_col(cand)
        for candidates in COLUMN_CANDIDATES.values()
        for cand in candidates
    }
    best_df: Optional[pd.DataFrame] = None
    best_header = 0
    best_score = -1

    for header in range(0, HEADER_SCAN_MAX + 1):
        try:
            df = pd.read_excel(path, sheet_name=sheet_name, header=header, engine="openpyxl", dtype=object)
        except Exception:
            continue
        if df is None:
            continue
        cols_norm = {norm_col(c) for c in df.columns}
        score = len(cols_norm & expected_keys)
        if score > best_score:
            best_df = df
            best_header = header
            best_score = score

    if best_df is None:
        raise RuntimeError(f"No se pudo leer la hoja '{sheet_name}'")

    best_df.columns = [str(c).strip() for c in best_df.columns]
    return best_df, best_header, best_score


def project_sheet(df_raw: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df_raw.index)
    for target in TARGET_COLUMNS:
        src_col = pick_first_column(df_raw, COLUMN_CANDIDATES[target])
        if src_col and src_col in df_raw.columns:
            out[target] = df_raw[src_col]
        else:
            out[target] = None

    out["sigte_id"] = out["sigte_id"].map(clean_id_like)
    out["run"] = out["run"].map(clean_id_like)
    out["dv"] = out["dv"].map(clean_dv)
    out["sexo"] = out["sexo"].map(clean_text)
    out["estab_dest"] = out["estab_dest"].map(clean_id_like)
    out["f_entrada"] = out["f_entrada"].map(clean_date_text)
    out["f_salida"] = out["f_salida"].map(clean_date_text)
    out["c_salida"] = out["c_salida"].map(clean_id_like)

    return out


def count_table(conn: psycopg2.extensions.connection) -> int:
    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."{TABLE}"')
        return int(cur.fetchone()[0])


def copy_table(conn: psycopg2.extensions.connection, df: pd.DataFrame) -> None:
    with conn.cursor() as cur:
        if TRUNCATE_BEFORE_LOAD:
            cur.execute(f'TRUNCATE TABLE "{SCHEMA}"."{TABLE}"')

        payload = io.StringIO()
        df.to_csv(payload, index=False, header=True, sep="\t", na_rep="", lineterminator="\n")
        payload.seek(0)
        cols = ",".join(f'"{c}"' for c in TARGET_COLUMNS)
        cur.copy_expert(
            f'COPY "{SCHEMA}"."{TABLE}" ({cols}) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER E\'\\t\', NULL \'\')',
            payload,
        )


def main() -> None:
    t0 = time.time()

    if not XLSX_PATH.exists() or not XLSX_PATH.is_file():
        raise FileNotFoundError(f"No existe el archivo Excel: {XLSX_PATH}")

    xls = pd.ExcelFile(XLSX_PATH, engine="openpyxl")
    if not xls.sheet_names:
        raise RuntimeError("El archivo no contiene hojas.")

    frames: List[pd.DataFrame] = []
    per_sheet: List[Dict[str, object]] = []

    for sheet_name in xls.sheet_names:
        df_raw, header_used, score = best_read_sheet(XLSX_PATH, sheet_name)
        df_proj = project_sheet(df_raw)
        rows_before = int(len(df_proj))
        df_proj = df_proj[df_proj["sigte_id"].notna()].copy()
        df_proj = df_proj[df_proj["sigte_id"].astype(str).str.strip() != ""].copy()
        rows_after = int(len(df_proj))
        if rows_after > 0:
            frames.append(df_proj)
        per_sheet.append(
            {
                "hoja": sheet_name,
                "header": header_used,
                "score": score,
                "filas_leidas": rows_before,
                "filas_validas": rows_after,
            }
        )

    if not frames:
        raise RuntimeError("No se encontraron registros con SIGTE_ID para cargar.")

    final_df = pd.concat(frames, ignore_index=True, sort=False)
    final_df = final_df[TARGET_COLUMNS].copy()

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
    )
    conn.autocommit = False

    try:
        print(f"Conectado a {PG_HOST}:{PG_PORT}/{PG_DB} como {PG_USER}")
        print(f"Origen: {XLSX_PATH}")
        for row in per_sheet:
            print(
                f"- Hoja='{row['hoja']}' | header={row['header']} | match={row['score']} "
                f"| filas={row['filas_leidas']} | validas={row['filas_validas']}"
            )

        before = count_table(conn)
        copy_table(conn, final_df)
        after = count_table(conn)
        conn.commit()

        print("")
        print(f"Tabla destino: {SCHEMA}.{TABLE}")
        print(f"Filas cargadas desde Excel: {len(final_df)}")
        print(f"Filas en tabla antes: {before}")
        print(f"Filas en tabla despues: {after}")
        print(f"Duracion: {time.time() - t0:.1f}s")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
