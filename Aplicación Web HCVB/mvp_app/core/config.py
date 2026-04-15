import os
import re
from pathlib import Path
from typing import Dict, List, Tuple


def _load_env_file(env_path: Path) -> None:
    if not env_path.exists() or not env_path.is_file():
        return
    try:
        lines = env_path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return

    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)

APP_TITLE = "Lista de Espera"

ROOT = Path(__file__).resolve().parents[2]
_load_env_file(ROOT / ".env")
DB_DIR = ROOT / "BASES DE DATOS"
_default_tmp = ROOT / ".app_tmp"
TEMP_DIR = Path(str(os.getenv("APP_TEMP_DIR", str(_default_tmp))) or str(_default_tmp))
UPLOAD_DIR = TEMP_DIR / "uploads"
OUTPUT_DIR = TEMP_DIR / "outputs"

ALLOWED_EXTENSIONS = {".xlsx", ".csv", ".xlsb"}
#Max = 10 para permitir mas jobs simultaneos
#MAX_CONCURRENT_JOBS = max(1, int(os.getenv("MAX_CONCURRENT_JOBS", "10")))
MAX_CONCURRENT_JOBS = max(3, int(os.getenv("MAX_CONCURRENT_JOBS", "3")))
JOB_RETENTION_SECONDS = max(21600, int(os.getenv("JOB_RETENTION_SECONDS", "1200")))
MAX_STORED_JOBS = max(10, int(os.getenv("MAX_STORED_JOBS", "1000")))
FILE_RETENTION_SECONDS = max(1800, int(os.getenv("FILE_RETENTION_SECONDS", "10800")))
JOB_RUNNING_TTL_SECONDS = max(1800, int(os.getenv("JOB_RUNNING_TTL_SECONDS", "172800")))
try:
    PRELOAD_MAX_CPU_PERCENT = max(0.0, min(100.0, float(os.getenv("PRELOAD_MAX_CPU_PERCENT", "12"))))
except Exception:
    PRELOAD_MAX_CPU_PERCENT = 12.0
try:
    # CPU cap for interactive jobs (file processing/statistics). Default 0 = no throttling.
    PROCESS_MAX_CPU_PERCENT = max(0.0, min(100.0, float(os.getenv("PROCESS_MAX_CPU_PERCENT", "0"))))
except Exception:
    PROCESS_MAX_CPU_PERCENT = 0.0
LOW_MEMORY_DB_REFRESH = str(os.getenv("LOW_MEMORY_DB_REFRESH", "1") or "1").strip().lower() in {
    "1", "true", "yes", "on"
}
try:
    MEMORY_TRIM_ROUNDS = max(1, min(10, int(os.getenv("MEMORY_TRIM_ROUNDS", "4"))))
except Exception:
    MEMORY_TRIM_ROUNDS = 4
try:
    MEMORY_TRIM_SLEEP_MS = max(0, min(500, int(os.getenv("MEMORY_TRIM_SLEEP_MS", "35"))))
except Exception:
    MEMORY_TRIM_SLEEP_MS = 35
try:
    CLEANUP_INTERVAL_SECONDS = max(30, int(os.getenv("CLEANUP_INTERVAL_SECONDS", "300")))
except Exception:
    CLEANUP_INTERVAL_SECONDS = 300
BACKGROUND_CLEANUP_ENABLED = str(os.getenv("BACKGROUND_CLEANUP_ENABLED", "1") or "1").strip().lower() in {
    "1", "true", "yes", "on"
}
try:
    BACKGROUND_CLEANUP_STARTUP_DELAY_SECONDS = max(
        0, min(300, int(os.getenv("BACKGROUND_CLEANUP_STARTUP_DELAY_SECONDS", "30")))
    )
except Exception:
    BACKGROUND_CLEANUP_STARTUP_DELAY_SECONDS = 30
APP_SECRET_KEY = os.getenv("APP_SECRET_KEY", "change-me")
SESSION_COOKIE_SECURE = str(os.getenv("SESSION_COOKIE_SECURE", "0") or "0").strip().lower() in {
    "1", "true", "yes", "on"
}
SESSION_COOKIE_SAMESITE = str(os.getenv("SESSION_COOKIE_SAMESITE", "Lax") or "Lax").strip()
SESSION_COOKIE_HTTPONLY = True
REDIS_URL = str(os.getenv("REDIS_URL", "") or "").strip()
REQUESTED_SESSION_BACKEND = str(os.getenv("SESSION_BACKEND", "filesystem") or "filesystem").strip().lower()
REQUESTED_JOBS_BACKEND = str(os.getenv("JOBS_BACKEND", "memory") or "memory").strip().lower()
SESSION_BACKEND = REQUESTED_SESSION_BACKEND
JOBS_BACKEND = REQUESTED_JOBS_BACKEND
if SESSION_BACKEND == "redis" and not REDIS_URL:
    SESSION_BACKEND = "filesystem"
if JOBS_BACKEND == "redis" and not REDIS_URL:
    JOBS_BACKEND = "memory"
STRICT_REDIS_BACKEND = str(os.getenv("STRICT_REDIS_BACKEND", "0") or "0").strip().lower() in {
    "1", "true", "yes", "on"
}
ROW_LEVEL_SQL_LOOKUPS = str(os.getenv("ROW_LEVEL_SQL_LOOKUPS", "1") or "1").strip().lower() in {
    "1", "true", "yes", "on"
}
BULK_SQL_CROSSES = str(os.getenv("BULK_SQL_CROSSES", "1") or "1").strip().lower() in {
    "1", "true", "yes", "on"
}
COMPACT_DB_INDEX = str(os.getenv("COMPACT_DB_INDEX", "1") or "1").strip().lower() in {
    "1", "true", "yes", "on"
}
NOMINA_VERIFY_LAZY_LOAD = str(os.getenv("NOMINA_VERIFY_LAZY_LOAD", "1") or "1").strip().lower() in {
    "1", "true", "yes", "on"
}
try:
    SQL_STREAM_BATCH_SIZE = max(500, min(50000, int(os.getenv("SQL_STREAM_BATCH_SIZE", "5000"))))
except Exception:
    SQL_STREAM_BATCH_SIZE = 5000
JOB_STORE_PREFIX = str(os.getenv("JOB_STORE_PREFIX", "le_noges") or "le_noges").strip()
APP_OUTPUT_PREFIXES = (
    "LE_NOGES_result_",
    "LE_NOGES_mediana_",
    "LE_NOGES_estadisticas_",
    "LE_NOGES_admin_update_",
    "LE_NOGES_upload_",
)
ALLOWED_C_SALIDA_VALUES = [
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
    "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
    "20", "99"
]
C_SALIDA_LABELS = {
    "0": "GES",
    "1": "Atención Realizada",
    "2": "Procedimiento Informado",
    "3": "Indicación médica u odontologica para reevaluación",
    "4": "Atención Otorgada en el Extra sistema",
    "5": "Cambio de Asegurador",
    "6": "Renuncia o rechazo voluntario",
    "7": "Recuperación espontánea",
    "8": "Inasistencia",
    "9": "Fallecimiento",
    "10": "Solicitud de Indicación Duplicada",
    "11": "Contacto no corresponde",
    "12": "No corresponde realizar cirugía",
    "13": "Traslado coordinado",
    "14": "No pertinencia",
    "15": "Error de digitación",
    "16": "Atención por Resolutividad",
    "17": "Atención por Telemedicina",
    "18": "Modificación de la condición clínico-diagnóstica del caso",
    "19": "Atención por Hospital Digital",
    "20": "Postergaciones",
    "99": "Técnico Administrativo Nivel Central"
}

UI_BG_FILES = {"img 1.png"}

#=========================================================================#
#====================Vinculacion Base PostgreSQL==========================#
PG_DSN = str(os.getenv("POSTGRES_DSN", "") or "").strip()
PG_HOST = str(os.getenv("POSTGRES_HOST", "127.0.0.1") or "127.0.0.1").strip()
PG_PORT = int(str(os.getenv("POSTGRES_PORT", "5433") or "5432").strip())
PG_DATABASE = str(os.getenv("POSTGRES_DB", "postgres") or "postgres").strip()
PG_USER = str(os.getenv("POSTGRES_USER", "postgres") or "postgres").strip()
PG_PASSWORD = str(os.getenv("POSTGRES_PASSWORD", "") or "")
PG_SCHEMA = str(os.getenv("POSTGRES_SCHEMA", "raw") or "raw").strip()
# Authentication/security schema and policy
AUTH_SCHEMA = str(os.getenv("AUTH_SCHEMA", "app") or "app").strip()
AUTH_LOGIN_WINDOW_SECONDS = max(60, int(os.getenv("AUTH_LOGIN_WINDOW_SECONDS", "900")))
AUTH_LOGIN_MAX_ATTEMPTS = max(3, int(os.getenv("AUTH_LOGIN_MAX_ATTEMPTS", "5")))
AUTH_LOCK_BASE_SECONDS = max(60, int(os.getenv("AUTH_LOCK_BASE_SECONDS", "900")))
AUTH_LOCK_MAX_SECONDS = max(AUTH_LOCK_BASE_SECONDS, int(os.getenv("AUTH_LOCK_MAX_SECONDS", "86400")))
AUTH_PBKDF2_ITERATIONS = max(120000, int(os.getenv("AUTH_PBKDF2_ITERATIONS", "240000")))
AUTH_PASSWORD_MIN_LENGTH = max(8, int(os.getenv("AUTH_PASSWORD_MIN_LENGTH", "10")))
AUTH_RECOVERY_CODE_TTL_SECONDS = max(300, int(os.getenv("AUTH_RECOVERY_CODE_TTL_SECONDS", "1800")))
AUTH_RECOVERY_ALERT_THRESHOLD = max(3, int(os.getenv("AUTH_RECOVERY_ALERT_THRESHOLD", "5")))
#=========================================================================#
#=========================================================================#


PG_BASE_TABLES = {
    "historico": "ss06_cerradas_historicas",
    "cgr": "cgr",
    "defunciones": "defunciones",
    "establecimientos": "establecimientos",
    "comges_especiales": "comges_especiales",
}
PG_NOMINA_TABLES: Dict[Tuple[str, str], str] = {
    ("cne", "abierto"): "nomina_ic_abiertas",
    ("cne", "cerrado"): "nomina_ic_cerradas",
    ("iq", "abierto"): "nomina_iq_abiertas",
    ("iq", "cerrado"): "nomina_iq_cerradas",
    ("proc", "abierto"): "nomina_proc_abiertas",
    ("proc", "cerrado"): "nomina_proc_cerradas",
}

ADMIN_RAW_TABLES: List[Tuple[str, str]] = [
    ("cgr", "CGR"),
    ("comges_especiales", "Comges Especiales"),
    ("defunciones", "Defunciones"),
    ("establecimientos", "Establecimientos"),
    ("nomina_ic_abiertas", "Nomina IC Abiertas"),
    ("nomina_ic_cerradas", "Nomina IC Cerradas"),
    ("nomina_iq_abiertas", "Nomina IQ Abiertas"),
    ("nomina_iq_cerradas", "Nomina IQ Cerradas"),
    ("nomina_proc_abiertas", "Nomina PROC Abiertas"),
    ("nomina_proc_cerradas", "Nomina PROC Cerradas"),
    ("ss06_cerradas_historicas", "SS06 Cerradas Historicas"),
]
ADMIN_CORES_TRIGGER_TABLES = {
    "nomina_ic_abiertas",
    "nomina_ic_cerradas",
    "nomina_iq_abiertas",
    "nomina_iq_cerradas",
    "nomina_proc_abiertas",
    "nomina_proc_cerradas",
    "ss06_cerradas_historicas",
}
ADMIN_CORES_SQL_PATH = ROOT / "CODIGOS MIGRACION" / "CORES Y INDICES.sql"
VERIFY_FIELDS: Dict[str, List[str]] = {
    "SERV_SALUD": ["SERV_SALUD", "serv_salud"],
    "RUN": ["RUN", "run"],
    "DV": ["DV", "dv"],
    "NOMBRES": ["NOMBRES", "nombres"],
    "PRIMER_APELLIDO": ["PRIMER_APELLIDO", "primer_apellido"],
    "SEGUNDO_APELLIDO": ["SEGUNDO_APELLIDO", "apellido_materno"],
    "FECHA_NAC": ["FECHA_NAC", "fecha_nac"],
    "SEXO": ["SEXO", "sexo"],
    "TIPO_PREST": ["TIPO_PREST", "tipo_prest"],
    "PRESTA_MIN": ["PRESTA_MIN", "presta_min"],
    "PLANO": ["PLANO", "plano"],
    "EXTREMIDAD": ["EXTREMIDAD", "extremidad"],
    "PRESTA_EST": ["PRESTA_EST", "presta_est"],
    "F_ENTRADA": ["F_ENTRADA", "f_entrada"],
    "ESTAB_ORIG": ["ESTAB_ORIG", "estab_orig"],
    "ESTAB_DEST": ["ESTAB_DEST", "estab_dest"],
    "F_SALIDA": ["F_SALIDA", "f_salida"],
    "C_SALIDA": ["C_SALIDA", "c_salida"],
    "E_OTOR_AT": ["E_OTOR_AT", "e_otor_at"],
    "PRESTA_MIN_SALIDA": ["PRESTA_MIN_SALIDA", "presta_min_salida"],
    "PRAIS": ["PRAIS", "prais"],
    "RUN_PROF_SOL": ["RUN_PROF_SOL", "run_prof_sol"],
    "DV_PROF_SOL": ["DV_PROF_SOL", "dv_prof_sol"],
    "RUN_PROF_RESOL": ["RUN_PROF_RESOL", "run_prof_resol"],
    "DV_PROF_RESOL": ["DV_PROF_RESOL", "dv_prof_resol"],
}
NOMINA_STATS_FIELDS: Dict[str, List[str]] = {
    "RUN": ["RUN", "run"],
    "DV": ["DV", "dv"],
    "SEXO": ["SEXO", "sexo"],
    "FECHA_NAC": ["FECHA_NAC", "fecha_nac"],
    "TIPO_PREST": ["TIPO_PREST", "tipo_prest"],
    "PRESTA_MIN": ["PRESTA_MIN", "presta_min"],
    "PRESTA_EST": ["PRESTA_EST", "presta_est"],
    "F_ENTRADA": ["F_ENTRADA", "f_entrada"],
    "F_SALIDA": ["F_SALIDA", "f_salida"],
    "C_SALIDA": ["C_SALIDA", "c_salida"],
    "EXTREMIDAD": ["EXTREMIDAD", "extremidad"],
    "ESTAB_DEST": ["ESTAB_DEST", "estab_dest"],
    "ESTAB_ORIG": ["ESTAB_ORIG", "estab_orig"],
    "ID_LOCAL": ["ID_LOCAL", "id_local"],
    "SIGTE_ID": ["SIGTE_ID", "sigte_id"],
}
NOMINA_SHEETS = ["abierto", "cerrado"]
NOMINA_TYPE_ALIASES = {
    "cne": "cne",
    "ic": "cne",
    "iq": "iq",
    "proc": "proc",
}
NOMINA_STATE_ALIASES = {
    "abierto": "abierto",
    "abierta": "abierto",
    "abiertos": "abierto",
    "abiertas": "abierto",
    "cerrado": "cerrado",
    "cerrada": "cerrado",
    "cerrados": "cerrado",
    "cerradas": "cerrado",
}
NOMINA_NAME_RE = re.compile(
    r"^nomina(?P<tipo>cne|ic|iq|proc).*?idsigte"
    r"(?P<estado>abierto|abierta|abiertos|abiertas|cerrado|cerrada|cerrados|cerradas)"
)

ESTAB_DEST_FILTER = "106100"
TABLE_PREVIEW_LIMIT = 500
