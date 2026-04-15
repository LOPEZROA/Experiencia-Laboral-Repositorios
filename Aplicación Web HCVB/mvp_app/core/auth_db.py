import base64
import hashlib
import hmac
import os
import re
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    import psycopg2
except Exception:
    psycopg2 = None

from .config import (
    AUTH_LOCK_BASE_SECONDS,
    AUTH_LOCK_MAX_SECONDS,
    AUTH_LOGIN_MAX_ATTEMPTS,
    AUTH_LOGIN_WINDOW_SECONDS,
    AUTH_PBKDF2_ITERATIONS,
    AUTH_RECOVERY_ALERT_THRESHOLD,
    AUTH_RECOVERY_CODE_TTL_SECONDS,
    AUTH_SCHEMA,
    PG_DATABASE,
    PG_DSN,
    PG_HOST,
    PG_PASSWORD,
    PG_PORT,
    PG_USER,
)


_SAFE_LOGIN_RE = re.compile(r"^[A-Za-z0-9.\-_@]{3,40}$")
_LOGIN_ID_RE = re.compile(r"^[0-9]{1,8}-[0-9K]$")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _quote_ident(value: str) -> str:
    return '"' + str(value or "").replace('"', '""') + '"'


def _qualified(table_name: str) -> str:
    schema = str(AUTH_SCHEMA or "").strip()
    table = str(table_name or "").strip()
    if schema:
        return f"{_quote_ident(schema)}.{_quote_ident(table)}"
    return _quote_ident(table)


def _pg_connect():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 no disponible para autenticacion.")
    if PG_DSN:
        return psycopg2.connect(PG_DSN)
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        connect_timeout=8,
    )


def _fetchone_dict(cur: Any) -> Optional[Dict[str, Any]]:
    row = cur.fetchone()
    if not row:
        return None
    cols = [str(c[0]) for c in (cur.description or [])]
    return {cols[i]: row[i] for i in range(min(len(cols), len(row)))}


def _fetchall_dict(cur: Any) -> List[Dict[str, Any]]:
    rows = cur.fetchall() or []
    cols = [str(c[0]) for c in (cur.description or [])]
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append({cols[i]: row[i] for i in range(min(len(cols), len(row)))})
    return out


def normalize_login_id(value: Any) -> str:
    s = str(value or "").strip().upper()
    s = s.replace(" ", "")
    return s


def normalize_email(value: Any) -> str:
    return str(value or "").strip().lower()


def looks_like_rut(value: str) -> bool:
    return bool(_LOGIN_ID_RE.match(str(value or "").strip().upper()))


def hash_password(password: str) -> str:
    raw = str(password or "")
    if not raw:
        raise RuntimeError("Contrasena vacia.")
    salt = base64.urlsafe_b64encode(os.urandom(16)).decode("ascii").rstrip("=")
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        raw.encode("utf-8"),
        salt.encode("utf-8"),
        int(AUTH_PBKDF2_ITERATIONS),
    )
    expected_hash = base64.b64encode(digest).decode("ascii")
    return f"pbkdf2_sha256${int(AUTH_PBKDF2_ITERATIONS)}${salt}${expected_hash}"


def verify_password_hash(provided_password: str, stored_password_hash: str) -> bool:
    expected = str(stored_password_hash or "")
    provided = str(provided_password or "")
    if not expected:
        return False
    if expected.startswith("pbkdf2_sha256$"):
        parts = expected.split("$", 3)
        if len(parts) != 4:
            return False
        _algo, iters_raw, salt, expected_hash = parts
        try:
            iters = max(1, int(iters_raw))
        except Exception:
            return False
        try:
            digest = hashlib.pbkdf2_hmac(
                "sha256",
                provided.encode("utf-8"),
                salt.encode("utf-8"),
                iters,
            )
            calc_hash = base64.b64encode(digest).decode("ascii")
            return hmac.compare_digest(calc_hash, expected_hash)
        except Exception:
            return False
    return hmac.compare_digest(provided, expected)


def validate_password_strength(password: str, login_id: str = "") -> Optional[str]:
    text = str(password or "").strip()
    if not text:
        return "La contrasena no puede estar vacia."
    return None


def _insert_event(
    conn: Any,
    event_type: str,
    login_id: str = "",
    actor_login: str = "",
    severity: str = "info",
    ip_address: str = "",
    detail: str = "",
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {_qualified("auth_security_events")}
                (event_type, login_id, actor_login, severity, ip_address, detail, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW());
            """,
            (
                str(event_type or "").strip(),
                normalize_login_id(login_id),
                normalize_login_id(actor_login),
                str(severity or "info").strip().lower(),
                str(ip_address or "").strip(),
                str(detail or "").strip(),
            ),
        )


def ensure_auth_storage() -> None:
    with _pg_connect() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            schema = str(AUTH_SCHEMA or "").strip()
            if schema:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(schema)};")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {_qualified("auth_users")} (
                    login_id VARCHAR(40) PRIMARY KEY,
                    password_hash TEXT NOT NULL,
                    email TEXT,
                    phone TEXT,
                    is_admin BOOLEAN NOT NULL DEFAULT FALSE,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    must_change_password BOOLEAN NOT NULL DEFAULT FALSE,
                    password_changed_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    created_by VARCHAR(40),
                    updated_by VARCHAR(40)
                );
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {_qualified("auth_login_state")} (
                    login_id VARCHAR(40) PRIMARY KEY,
                    failed_count INTEGER NOT NULL DEFAULT 0,
                    window_started_at TIMESTAMPTZ,
                    locked_until TIMESTAMPTZ,
                    lock_level INTEGER NOT NULL DEFAULT 0,
                    last_failed_ip TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {_qualified("auth_password_recovery_requests")} (
                    request_id UUID PRIMARY KEY,
                    login_id VARCHAR(40) NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'pending',
                    request_email TEXT,
                    request_note TEXT,
                    request_ip TEXT,
                    verification_hash TEXT,
                    verification_expires_at TIMESTAMPTZ,
                    recovery_attempts INTEGER NOT NULL DEFAULT 0,
                    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    reviewed_at TIMESTAMPTZ,
                    reviewed_by VARCHAR(40),
                    completed_at TIMESTAMPTZ,
                    review_note TEXT
                );
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {_qualified("auth_security_events")} (
                    event_id BIGSERIAL PRIMARY KEY,
                    event_type VARCHAR(60) NOT NULL,
                    login_id VARCHAR(40),
                    actor_login VARCHAR(40),
                    severity VARCHAR(12) NOT NULL DEFAULT 'info',
                    ip_address TEXT,
                    detail TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_auth_users_admin ON {_qualified('auth_users')} (is_admin, is_active);"
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_auth_login_state_locked ON {_qualified('auth_login_state')} (locked_until);"
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_auth_recovery_status ON {_qualified('auth_password_recovery_requests')} (status, requested_at DESC);"
            )
            cur.execute(
                f"ALTER TABLE {_qualified('auth_password_recovery_requests')} ADD COLUMN IF NOT EXISTS request_email TEXT;"
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_auth_events_created ON {_qualified('auth_security_events')} (created_at DESC);"
            )
        conn.commit()


def init_auth_runtime() -> Dict[str, int]:
    ensure_auth_storage()
    return {"users": 0, "admins": 0}


def _parse_ts(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    return None


def _get_user_for_update(cur: Any, login_id: str) -> Optional[Dict[str, Any]]:
    cur.execute(
        f"""
        SELECT login_id, password_hash, email, phone, is_admin, is_active, must_change_password
        FROM {_qualified("auth_users")}
        WHERE login_id = %s
        LIMIT 1;
        """,
        (normalize_login_id(login_id),),
    )
    return _fetchone_dict(cur)


def _get_lock_seconds(cur: Any, login_id: str) -> int:
    cur.execute(
        f"""
        SELECT locked_until
        FROM {_qualified("auth_login_state")}
        WHERE login_id = %s
        LIMIT 1;
        """,
        (normalize_login_id(login_id),),
    )
    row = _fetchone_dict(cur)
    if not row:
        return 0
    locked_until = _parse_ts(row.get("locked_until"))
    if not locked_until:
        return 0
    now = _utcnow()
    if locked_until <= now:
        return 0
    return max(1, int((locked_until - now).total_seconds()))


def _set_login_state(
    cur: Any,
    login_id: str,
    failed_count: int,
    window_started_at: Optional[datetime],
    locked_until: Optional[datetime],
    lock_level: int,
    last_failed_ip: str = "",
) -> None:
    cur.execute(
        f"""
        INSERT INTO {_qualified("auth_login_state")}
            (login_id, failed_count, window_started_at, locked_until, lock_level, last_failed_ip, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (login_id) DO UPDATE
        SET failed_count=EXCLUDED.failed_count,
            window_started_at=EXCLUDED.window_started_at,
            locked_until=EXCLUDED.locked_until,
            lock_level=EXCLUDED.lock_level,
            last_failed_ip=EXCLUDED.last_failed_ip,
            updated_at=NOW();
        """,
        (
            normalize_login_id(login_id),
            int(max(0, failed_count)),
            window_started_at,
            locked_until,
            int(max(0, lock_level)),
            str(last_failed_ip or "").strip(),
        ),
    )


def _register_failed_login(cur: Any, login_id: str, ip_address: str) -> Tuple[bool, int]:
    lid = normalize_login_id(login_id)
    now = _utcnow()
    cur.execute(
        f"""
        SELECT failed_count, window_started_at, locked_until, lock_level
        FROM {_qualified("auth_login_state")}
        WHERE login_id = %s
        LIMIT 1;
        """,
        (lid,),
    )
    row = _fetchone_dict(cur) or {}
    failed_count = int(row.get("failed_count", 0) or 0)
    lock_level = int(row.get("lock_level", 0) or 0)
    window_started = _parse_ts(row.get("window_started_at"))
    if (window_started is None) or ((now - window_started).total_seconds() > int(AUTH_LOGIN_WINDOW_SECONDS)):
        failed_count = 0
        window_started = now
    failed_count += 1
    locked_until: Optional[datetime] = None
    lock_triggered = False
    lock_seconds = 0
    if failed_count >= int(AUTH_LOGIN_MAX_ATTEMPTS):
        lock_level = min(lock_level + 1, 16)
        lock_seconds = min(int(AUTH_LOCK_MAX_SECONDS), int(AUTH_LOCK_BASE_SECONDS) * (2 ** (lock_level - 1)))
        locked_until = now + timedelta(seconds=lock_seconds)
        failed_count = 0
        window_started = now
        lock_triggered = True
    _set_login_state(
        cur,
        login_id=lid,
        failed_count=failed_count,
        window_started_at=window_started,
        locked_until=locked_until,
        lock_level=lock_level,
        last_failed_ip=ip_address,
    )
    return lock_triggered, lock_seconds


def _reset_login_state(cur: Any, login_id: str) -> None:
    _set_login_state(
        cur,
        login_id=normalize_login_id(login_id),
        failed_count=0,
        window_started_at=None,
        locked_until=None,
        lock_level=0,
        last_failed_ip="",
    )


def authenticate_login(login_id: str, password: str, ip_address: str = "") -> Dict[str, Any]:
    lid = normalize_login_id(login_id)
    if not lid:
        return {"ok": False, "error": "Debes ingresar un RUT valido."}
    if not _SAFE_LOGIN_RE.match(lid):
        return {"ok": False, "error": "Formato de RUT invalido."}
    with _pg_connect() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            lock_seconds = _get_lock_seconds(cur, lid)
            if lock_seconds > 0:
                _insert_event(
                    conn,
                    event_type="login_blocked",
                    login_id=lid,
                    severity="warn",
                    ip_address=ip_address,
                    detail=f"blocked_for={lock_seconds}s",
                )
                conn.commit()
                return {"ok": False, "error": "Cuenta temporalmente bloqueada.", "blocked_seconds": lock_seconds}

            user = _get_user_for_update(cur, lid)
            if not user or (not bool(user.get("is_active", False))):
                lock_triggered, lock_for = _register_failed_login(cur, lid, ip_address)
                _insert_event(
                    conn,
                    event_type="login_failed",
                    login_id=lid,
                    severity="warn",
                    ip_address=ip_address,
                    detail="usuario_no_encontrado_o_inactivo",
                )
                if lock_triggered:
                    _insert_event(
                        conn,
                        event_type="login_lockout",
                        login_id=lid,
                        severity="warn",
                        ip_address=ip_address,
                        detail=f"lock_seconds={lock_for}",
                    )
                conn.commit()
                return {"ok": False, "error": "usuario o contrasena incorrecta."}

            if not verify_password_hash(password, str(user.get("password_hash") or "")):
                lock_triggered, lock_for = _register_failed_login(cur, lid, ip_address)
                _insert_event(
                    conn,
                    event_type="login_failed",
                    login_id=lid,
                    severity="warn",
                    ip_address=ip_address,
                    detail="contrasena_incorrecta",
                )
                if lock_triggered:
                    _insert_event(
                        conn,
                        event_type="login_lockout",
                        login_id=lid,
                        severity="warn",
                        ip_address=ip_address,
                        detail=f"lock_seconds={lock_for}",
                    )
                conn.commit()
                return {"ok": False, "error": "usuario o contrasena incorrecta."}

            _reset_login_state(cur, lid)
            _insert_event(
                conn,
                event_type="login_success",
                login_id=lid,
                severity="info",
                ip_address=ip_address,
            )
            conn.commit()
            return {
                "ok": True,
                "login_id": lid,
                "is_admin": bool(user.get("is_admin", False)),
                "must_change_password": bool(user.get("must_change_password", False)),
            }


def is_admin_user(login_id: Optional[str]) -> bool:
    lid = normalize_login_id(login_id)
    if not lid:
        return False
    try:
        with _pg_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT is_admin, is_active
                    FROM {_qualified("auth_users")}
                    WHERE login_id = %s
                    LIMIT 1;
                    """,
                    (lid,),
                )
                row = _fetchone_dict(cur)
        return bool(row and row.get("is_admin") and row.get("is_active"))
    except Exception:
        return False


def change_password(login_id: str, current_password: str, new_password: str, ip_address: str = "") -> Tuple[bool, str]:
    lid = normalize_login_id(login_id)
    if not lid:
        return False, "Usuario invalido."
    policy_error = validate_password_strength(new_password, login_id=lid)
    if policy_error:
        return False, policy_error
    with _pg_connect() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            user = _get_user_for_update(cur, lid)
            if not user or (not bool(user.get("is_active", False))):
                conn.rollback()
                return False, "Usuario no encontrado o inactivo."
            current_hash = str(user.get("password_hash") or "")
            if not verify_password_hash(current_password, current_hash):
                _insert_event(
                    conn,
                    event_type="password_change_failed",
                    login_id=lid,
                    actor_login=lid,
                    severity="warn",
                    ip_address=ip_address,
                    detail="contrasena_actual_incorrecta",
                )
                conn.commit()
                return False, "La contrasena actual no coincide."
            if verify_password_hash(new_password, current_hash):
                conn.rollback()
                return False, "La nueva contrasena debe ser distinta a la actual."
            new_hash = hash_password(new_password)
            cur.execute(
                f"""
                UPDATE {_qualified("auth_users")}
                SET password_hash=%s,
                    must_change_password=FALSE,
                    password_changed_at=NOW(),
                    updated_at=NOW(),
                    updated_by=%s
                WHERE login_id=%s;
                """,
                (new_hash, lid, lid),
            )
            _insert_event(
                conn,
                event_type="password_changed",
                login_id=lid,
                actor_login=lid,
                severity="info",
                ip_address=ip_address,
            )
            conn.commit()
    return True, "Contrasena actualizada correctamente."


def create_password_recovery_request(
    login_id: str,
    request_note: str = "",
    request_email: str = "",
    ip_address: str = "",
) -> Tuple[bool, str, str]:
    lid = normalize_login_id(login_id)
    if not lid:
        return False, "Debes ingresar un RUT valido.", ""
    with _pg_connect() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            user = _get_user_for_update(cur, lid)
            if not user or (not bool(user.get("is_active", False))):
                _insert_event(
                    conn,
                    event_type="recovery_request_rejected",
                    login_id=lid,
                    severity="warn",
                    ip_address=ip_address,
                    detail="usuario_no_encontrado_o_inactivo",
                )
                conn.commit()
                return False, "No fue posible crear la solicitud para ese usuario.", ""
            provided_email = normalize_email(request_email)
            cur.execute(
                f"""
                SELECT request_id
                FROM {_qualified("auth_password_recovery_requests")}
                WHERE login_id=%s AND status IN ('pending', 'approved')
                ORDER BY requested_at DESC
                LIMIT 1;
                """,
                (lid,),
            )
            active_request = _fetchone_dict(cur)
            if active_request:
                req_id = str(active_request.get("request_id") or "")
                conn.commit()
                return (
                    True,
                    "Ya existe una solicitud activa. Esta pendiente de autorizacion ADMIN o ya fue aprobada.",
                    req_id,
                )
            request_id = str(uuid.uuid4())
            cur.execute(
                f"""
                INSERT INTO {_qualified("auth_password_recovery_requests")}
                    (request_id, login_id, status, request_email, request_note, request_ip, requested_at)
                VALUES (%s::uuid, %s, 'pending', %s, %s, %s, NOW());
                """,
                (
                    request_id,
                    lid,
                    provided_email,
                    str(request_note or "").strip(),
                    str(ip_address or "").strip(),
                ),
            )
            _insert_event(
                conn,
                event_type="recovery_requested",
                login_id=lid,
                severity="warn",
                ip_address=ip_address,
                detail=f"request_id={request_id}",
            )
            conn.commit()
            return True, "Solicitud enviada al perfil ADMIN. Queda pendiente de autorizacion.", request_id


def list_password_recovery_requests(status: str = "", limit: int = 200) -> List[Dict[str, Any]]:
    lim = max(1, min(500, int(limit or 200)))
    status_norm = str(status or "").strip().lower()
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            if status_norm:
                cur.execute(
                    f"""
                    SELECT request_id, login_id, status, request_email, request_note, request_ip, requested_at,
                           reviewed_at, reviewed_by, completed_at, review_note, verification_expires_at
                    FROM {_qualified("auth_password_recovery_requests")}
                    WHERE status = %s
                    ORDER BY requested_at DESC
                    LIMIT %s;
                    """,
                    (status_norm, lim),
                )
            else:
                cur.execute(
                    f"""
                    SELECT request_id, login_id, status, request_email, request_note, request_ip, requested_at,
                           reviewed_at, reviewed_by, completed_at, review_note, verification_expires_at
                    FROM {_qualified("auth_password_recovery_requests")}
                    ORDER BY requested_at DESC
                    LIMIT %s;
                    """,
                    (lim,),
                )
            return _fetchall_dict(cur)


def review_password_recovery_request(
    request_id: str,
    action: str,
    actor_login: str,
    review_note: str = "",
    ip_address: str = "",
) -> Tuple[bool, str, str]:
    rid = str(request_id or "").strip()
    actor = normalize_login_id(actor_login)
    action_norm = str(action or "").strip().lower()
    if action_norm not in {"approve", "reject"}:
        return False, "Accion invalida.", ""
    if not rid:
        return False, "Solicitud invalida.", ""
    with _pg_connect() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT request_id, login_id, status
                FROM {_qualified("auth_password_recovery_requests")}
                WHERE request_id=%s::uuid
                LIMIT 1;
                """,
                (rid,),
            )
            row = _fetchone_dict(cur)
            if not row:
                conn.rollback()
                return False, "Solicitud no encontrada.", ""
            status = str(row.get("status") or "").strip().lower()
            if status != "pending":
                conn.rollback()
                return False, f"La solicitud ya fue revisada (estado: {status}).", ""
            lid = normalize_login_id(row.get("login_id"))
            if action_norm == "reject":
                cur.execute(
                    f"""
                    UPDATE {_qualified("auth_password_recovery_requests")}
                    SET status='rejected', reviewed_at=NOW(), reviewed_by=%s, review_note=%s
                    WHERE request_id=%s::uuid;
                    """,
                    (actor, str(review_note or "").strip(), rid),
                )
                _insert_event(
                    conn,
                    event_type="recovery_rejected",
                    login_id=lid,
                    actor_login=actor,
                    severity="warn",
                    ip_address=ip_address,
                    detail=f"request_id={rid}",
                )
                conn.commit()
                return True, "Solicitud rechazada.", ""

            verification_code = f"{secrets.randbelow(10**8):08d}"
            verification_hash = hash_password(verification_code)
            expires_at = _utcnow() + timedelta(seconds=int(AUTH_RECOVERY_CODE_TTL_SECONDS))
            cur.execute(
                f"""
                UPDATE {_qualified("auth_password_recovery_requests")}
                SET status='approved',
                    reviewed_at=NOW(),
                    reviewed_by=%s,
                    review_note=%s,
                    verification_hash=%s,
                    verification_expires_at=%s,
                    recovery_attempts=0
                WHERE request_id=%s::uuid;
                """,
                (actor, str(review_note or "").strip(), verification_hash, expires_at, rid),
            )
            _insert_event(
                conn,
                event_type="recovery_approved",
                login_id=lid,
                actor_login=actor,
                severity="warn",
                ip_address=ip_address,
                detail=f"request_id={rid}",
            )
            conn.commit()
            return True, "Solicitud aprobada.", verification_code


def complete_password_recovery(
    request_id: str,
    login_id: str,
    verification_code: str,
    new_password: str,
    ip_address: str = "",
) -> Tuple[bool, str]:
    rid = str(request_id or "").strip()
    lid = normalize_login_id(login_id)
    code = str(verification_code or "").strip()
    if not rid or not lid or not code:
        return False, "Debes completar todos los datos de recuperacion."
    policy_error = validate_password_strength(new_password, login_id=lid)
    if policy_error:
        return False, policy_error
    with _pg_connect() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT request_id, login_id, status, verification_hash, verification_expires_at, recovery_attempts
                FROM {_qualified("auth_password_recovery_requests")}
                WHERE request_id=%s::uuid
                LIMIT 1;
                """,
                (rid,),
            )
            req = _fetchone_dict(cur)
            if not req:
                conn.rollback()
                return False, "Solicitud no encontrada."
            req_login = normalize_login_id(req.get("login_id"))
            if req_login != lid:
                conn.rollback()
                return False, "La solicitud no corresponde al usuario indicado."
            status = str(req.get("status") or "").strip().lower()
            if status != "approved":
                conn.rollback()
                return False, "La solicitud no esta aprobada por administrador."
            exp = _parse_ts(req.get("verification_expires_at"))
            if (not exp) or (exp <= _utcnow()):
                cur.execute(
                    f"""
                    UPDATE {_qualified("auth_password_recovery_requests")}
                    SET status='expired', review_note=COALESCE(review_note, '') || ' | codigo_expirado'
                    WHERE request_id=%s::uuid;
                    """,
                    (rid,),
                )
                _insert_event(
                    conn,
                    event_type="recovery_expired",
                    login_id=lid,
                    severity="warn",
                    ip_address=ip_address,
                    detail=f"request_id={rid}",
                )
                conn.commit()
                return False, "El codigo de recuperacion expiro. Solicita uno nuevo."
            stored_hash = str(req.get("verification_hash") or "")
            if not verify_password_hash(code, stored_hash):
                attempts = int(req.get("recovery_attempts", 0) or 0) + 1
                cur.execute(
                    f"""
                    UPDATE {_qualified("auth_password_recovery_requests")}
                    SET recovery_attempts=%s
                    WHERE request_id=%s::uuid;
                    """,
                    (attempts, rid),
                )
                _insert_event(
                    conn,
                    event_type="recovery_code_failed",
                    login_id=lid,
                    severity="warn",
                    ip_address=ip_address,
                    detail=f"request_id={rid} attempts={attempts}",
                )
                conn.commit()
                return False, "Codigo de recuperacion invalido."
            new_hash = hash_password(new_password)
            cur.execute(
                f"""
                UPDATE {_qualified("auth_users")}
                SET password_hash=%s,
                    must_change_password=FALSE,
                    password_changed_at=NOW(),
                    updated_at=NOW(),
                    updated_by=%s
                WHERE login_id=%s;
                """,
                (new_hash, lid, lid),
            )
            _reset_login_state(cur, lid)
            cur.execute(
                f"""
                UPDATE {_qualified("auth_password_recovery_requests")}
                SET status='completed',
                    completed_at=NOW(),
                    verification_hash=NULL,
                    verification_expires_at=NULL
                WHERE request_id=%s::uuid;
                """,
                (rid,),
            )
            _insert_event(
                conn,
                event_type="password_reset_completed",
                login_id=lid,
                actor_login=lid,
                severity="info",
                ip_address=ip_address,
                detail=f"request_id={rid}",
            )
            conn.commit()
    return True, "Contrasena restablecida correctamente."


def get_security_events(limit: int = 200) -> List[Dict[str, Any]]:
    lim = max(1, min(500, int(limit or 200)))
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT event_id, event_type, login_id, actor_login, severity, ip_address, detail, created_at
                FROM {_qualified("auth_security_events")}
                ORDER BY created_at DESC
                LIMIT %s;
                """,
                (lim,),
            )
            return _fetchall_dict(cur)


def get_security_health() -> Dict[str, Any]:
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*) AS n
                FROM {_qualified("auth_password_recovery_requests")}
                WHERE status='pending';
                """
            )
            pending_requests = int((cur.fetchone() or [0])[0] or 0)
            cur.execute(
                f"""
                SELECT COUNT(*) AS n
                FROM {_qualified("auth_login_state")}
                WHERE locked_until IS NOT NULL AND locked_until > NOW();
                """
            )
            active_lockouts = int((cur.fetchone() or [0])[0] or 0)
            cur.execute(
                f"""
                SELECT COUNT(*) AS n
                FROM {_qualified("auth_security_events")}
                WHERE event_type IN ('login_failed', 'login_lockout')
                  AND created_at >= NOW() - INTERVAL '24 hours';
                """
            )
            failed_last_24h = int((cur.fetchone() or [0])[0] or 0)
            cur.execute(
                f"""
                SELECT COUNT(*) AS n
                FROM {_qualified("auth_security_events")}
                WHERE event_type='recovery_requested'
                  AND created_at >= NOW() - INTERVAL '24 hours';
                """
            )
            recovery_last_24h = int((cur.fetchone() or [0])[0] or 0)
            cur.execute(
                f"""
                SELECT COUNT(*) AS n
                FROM {_qualified("auth_users")}
                WHERE is_active=TRUE;
                """
            )
            active_accounts = int((cur.fetchone() or [0])[0] or 0)
    alerts: List[str] = []
    if pending_requests >= int(AUTH_RECOVERY_ALERT_THRESHOLD):
        alerts.append(f"Hay {pending_requests} solicitudes de recuperacion pendientes.")
    if active_lockouts > 0:
        alerts.append(f"Hay {active_lockouts} cuentas bloqueadas por intentos fallidos.")
    if failed_last_24h >= (int(AUTH_LOGIN_MAX_ATTEMPTS) * 2):
        alerts.append(f"Se registraron {failed_last_24h} eventos de fallo/bloqueo en 24h.")
    return {
        "active_accounts": active_accounts,
        "pending_recovery_requests": pending_requests,
        "active_lockouts": active_lockouts,
        "failed_events_24h": failed_last_24h,
        "recovery_requests_24h": recovery_last_24h,
        "alerts": alerts,
    }
