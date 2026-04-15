import io
import hmac
import re
import secrets
import threading
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from flask import Flask, abort, flash, jsonify, redirect, render_template, request, send_file, session, url_for
from werkzeug.utils import secure_filename

from ..core import auth_db
from ..core.catalog import CATEGORIES
from ..core.config import (
    ADMIN_RAW_TABLES,
    APP_TITLE,
    JOBS_BACKEND,
    MAX_CONCURRENT_JOBS,
    OUTPUT_DIR,
    ROOT,
    SESSION_BACKEND,
    TABLE_PREVIEW_LIMIT,
    UI_BG_FILES,
    UPLOAD_DIR,
)
from ..core.services import (
    _admin_accept_attr_for_table,
    _admin_allowed_ext_text_for_table,
    _admin_is_allowed_file_for_table,
    _admin_resolve_load_mode,
    _admin_table_allows_append,
    _can_start_new_job,
    _current_user_can_download,
    _current_user_is_admin,
    _current_user_rut,
    _get_owned_or_admin_job,
    _get_owned_job,
    _get_owned_job_fresh,
    _init_job,
    _is_db_updating,
    _is_admin_user,
    _jobs_snapshot,
    _json_safe_value,
    _request_cancel_job,
    _run_admin_update_job,
    _run_cross_stats_job,
    _run_job,
    _run_mediana_job,
    _run_stats_job,
    _safe_unlink,
    _search_cases_full_rows,
    _slice_preview_rows,
    _stats_to_excel,
    _to_int,
    _update_job,
    allowed_file,
    apply_short_date_format_to_workbook,
    format_dates_for_export,
    normalize_id,
    normalize_rut_concat,
    parse_excel_date,
)

_CSRF_TOKEN_KEY = "_csrf_token"


def _ensure_csrf_token() -> str:
    token = str(session.get(_CSRF_TOKEN_KEY, "") or "").strip()
    if token:
        return token
    token = secrets.token_urlsafe(32)
    session[_CSRF_TOKEN_KEY] = token
    return token


def _is_csrf_valid() -> bool:
    expected = str(session.get(_CSRF_TOKEN_KEY, "") or "").strip()
    if not expected:
        return False
    provided = (
        request.form.get("_csrf_token", "")
        or request.headers.get("X-CSRF-Token", "")
        or request.headers.get("X-CSRFToken", "")
    )
    provided_text = str(provided or "").strip()
    if not provided_text:
        return False
    return hmac.compare_digest(expected, provided_text)


def register_web_routes(app: Flask) -> None:
    @app.before_request
    def require_login():
        endpoint = request.endpoint or ""
        if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
            if endpoint not in {"static", "ui_image"}:
                if not _is_csrf_valid():
                    if endpoint == "cancel_job" or request.path.startswith("/cancel/"):
                        return jsonify({"status": "error", "error": "CSRF token invalido."}), 400
                    flash("Sesion invalida. Recarga la pagina e intenta nuevamente.")
                    if endpoint == "login":
                        return redirect(url_for("login"))
                    return redirect(request.referrer or url_for("home"))

        if request.endpoint in (
            None,
            "login",
            "password_recovery_request",
            "password_recovery_reset",
            "static",
            "ui_image",
        ):
            return
        if request.endpoint == "logout":
            return
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        session.permanent = True
        now = datetime.utcnow()
        last = session.get("last_activity")
        if last:
            try:
                last_dt = datetime.fromisoformat(last)
            except Exception:
                last_dt = None
            if last_dt and (now - last_dt) > timedelta(minutes=30):
                session.clear()
                flash("Sesión expirada por inactividad.")
                return redirect(url_for("login"))
        session["last_activity"] = now.isoformat()
    @app.context_processor
    def inject_template_flags() -> Dict[str, Any]:
        return {
            "is_admin_user": _is_admin_user(str(session.get("rut", "") or "").strip().upper()),
            "csrf_token": _ensure_csrf_token(),
        }

    @app.get("/")
    def home():
        return render_template("home.html", title=APP_TITLE, categories=CATEGORIES)


    @app.get("/manuales-de-uso")
    def manuales_de_uso():
        manuals = [
            {
                "key": "usuario",
                "label": "Manual usuario",
                "filename": "Guia uso app.pdf",
                "description": "Guia general de uso para usuarios operativos de la plataforma.",
            },
            {
                "key": "admin",
                "label": "Manual ADMIN",
                "filename": "Guia ADMIN.pdf",
                "description": "Guia orientada a administradores con funciones avanzadas de la app.",
            },
        ]
        return render_template("manuales_uso.html", title=APP_TITLE, manuals=manuals)


    @app.get("/manuales-de-uso/download/<manual_key>")
    def manuales_uso_download(manual_key: str):
        manuals = {
            "usuario": "Guia uso app.pdf",
            "admin": "Guia ADMIN.pdf",
        }
        filename = manuals.get(str(manual_key or "").strip().lower())
        if not filename:
            abort(404)
        path = (ROOT / filename).resolve()
        try:
            root_path = ROOT.resolve()
        except Exception:
            root_path = ROOT
        if root_path not in path.parents and path != root_path:
            abort(404)
        if not path.exists() or not path.is_file():
            flash(f"No se encontro el archivo solicitado: {filename}")
            return redirect(url_for("manuales_de_uso"))
        return send_file(path, as_attachment=True, download_name=path.name, mimetype="application/pdf")


    @app.get("/ui-bg/<path:filename>")
    def ui_image(filename: str):
        if filename not in UI_BG_FILES:
            abort(404)
        path = ROOT / filename
        if not path.exists() or not path.is_file():
            abort(404)
        return send_file(path)


    @app.route("/login", methods=["GET", "POST"])
    def login():
        if request.method == "POST":
            session.clear()
            rut_raw = (request.form.get("rut", "") or "").strip().upper()
            client_ip = str(request.headers.get("X-Forwarded-For", request.remote_addr or "")).split(",")[0].strip()
            if not rut_raw:
                flash("Debes ingresar tu RUT/usuario.")
                return redirect(request.url)
            password = (request.form.get("password", "") or "").strip()
            auth_result = auth_db.authenticate_login(rut_raw, password, ip_address=client_ip)
            if not bool(auth_result.get("ok")):
                blocked_seconds = int(auth_result.get("blocked_seconds", 0) or 0)
                if blocked_seconds > 0:
                    flash(f"Cuenta bloqueada temporalmente por intentos fallidos. Espera {blocked_seconds} segundos.")
                else:
                    flash(str(auth_result.get("error", "RUT/usuario o contrasena incorrecta.")))
                return redirect(request.url)

            session["logged_in"] = True
            session["rut"] = str(auth_result.get("login_id", rut_raw) or rut_raw).strip().upper()
            session.permanent = True
            session["last_activity"] = datetime.utcnow().isoformat()
            session["must_change_password"] = bool(auth_result.get("must_change_password", False))
            return redirect(url_for("home"))

        return render_template("login.html", title=APP_TITLE)


    @app.route("/recuperar-contrasena", methods=["GET", "POST"])
    def password_recovery_request():
        last_request_id = ""
        if request.method == "POST":
            login_id = (request.form.get("rut", "") or "").strip().upper()
            note = (request.form.get("nota", "") or "").strip()
            client_ip = str(request.headers.get("X-Forwarded-For", request.remote_addr or "")).split(",")[0].strip()
            ok, msg, request_id = auth_db.create_password_recovery_request(
                login_id=login_id,
                request_note=note,
                ip_address=client_ip,
            )
            if ok:
                last_request_id = request_id
                flash(msg)
                if request_id:
                    flash(f"ID de solicitud: {request_id}")
            else:
                flash(msg)
        return render_template(
            "password_recovery_request.html",
            title=APP_TITLE,
            request_id=last_request_id,
        )


    @app.route("/recuperar-contrasena/finalizar", methods=["GET", "POST"])
    def password_recovery_reset():
        if request.method == "POST":
            login_id = (request.form.get("rut", "") or "").strip().upper()
            request_id = (request.form.get("request_id", "") or "").strip()
            verification_code = (request.form.get("verification_code", "") or "").strip()
            new_password = (request.form.get("new_password", "") or "")
            confirm_password = (request.form.get("confirm_password", "") or "")
            if new_password != confirm_password:
                flash("La confirmacion de contrasena no coincide.")
                return redirect(request.url)
            client_ip = str(request.headers.get("X-Forwarded-For", request.remote_addr or "")).split(",")[0].strip()
            ok, msg = auth_db.complete_password_recovery(
                request_id=request_id,
                login_id=login_id,
                verification_code=verification_code,
                new_password=new_password,
                ip_address=client_ip,
            )
            flash(msg)
            if ok:
                return redirect(url_for("login"))
        return render_template("password_recovery_reset.html", title=APP_TITLE)


    @app.route("/cambiar-contrasena", methods=["GET", "POST"])
    def change_password():
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        current_user = str(session.get("rut", "") or "").strip().upper()
        if request.method == "POST":
            current_password = (request.form.get("current_password", "") or "")
            new_password = (request.form.get("new_password", "") or "")
            confirm_password = (request.form.get("confirm_password", "") or "")
            if new_password != confirm_password:
                flash("La confirmacion de contrasena no coincide.")
                return redirect(request.url)
            client_ip = str(request.headers.get("X-Forwarded-For", request.remote_addr or "")).split(",")[0].strip()
            ok, msg = auth_db.change_password(
                login_id=current_user,
                current_password=current_password,
                new_password=new_password,
                ip_address=client_ip,
            )
            flash(msg)
            if ok:
                session["must_change_password"] = False
                return redirect(url_for("home"))
        return render_template("change_password.html", title=APP_TITLE, current_user=current_user)


    @app.get("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login"))


    def _safe_init_job(
        job_id: str,
        owner_rut: str = "",
        input_file: str = "",
        repeat_url: str = "",
        cleanup_paths: Optional[List[Path]] = None,
    ) -> Optional[str]:
        try:
            _init_job(
                job_id,
                owner_rut=owner_rut,
                input_file=input_file,
                repeat_url=repeat_url,
            )
            return None
        except RuntimeError as e:
            for path in (cleanup_paths or []):
                _safe_unlink(path)
            return str(e)

    def _fmt_job_ts(value: Any) -> str:
        try:
            ts = float(value or 0)
        except Exception:
            return "-"
        if ts <= 0:
            return "-"
        try:
            return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return "-"

    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return float(default)

    def _to_int_or(value: Any, default: int = 0) -> int:
        parsed = _to_int(value)
        if parsed is None:
            return int(default)
        return int(parsed)

    def _job_process_label(job: Dict[str, Any]) -> str:
        repeat_url = str(job.get("repeat_url", "") or "").strip().lower()
        if job.get("admin_selected") is not None or "admin-db-update" in repeat_url:
            return "Actualizacion RAW"
        if job.get("mediana_stats") is not None or "vacio" in repeat_url:
            return "Calculo mediana"
        if job.get("cross_stats") is not None or "estadisticas-cruces" in repeat_url:
            return "Estadisticas cruces"
        if job.get("stats") is not None or "estadisticas" in repeat_url:
            return "Estadisticas generales"
        if "busqueda-sudais" in repeat_url:
            return "Busqueda sudais"
        if str(job.get("input_file", "") or "").strip():
            return "Cruces de archivo"
        return "Proceso general"

    def _status_label(status: str) -> str:
        status_norm = str(status or "").strip().lower()
        labels = {
            "running": "En ejecucion",
            "done": "Finalizado",
            "error": "Error",
            "canceled": "Cancelado",
        }
        return labels.get(status_norm, "Desconocido")

    def _status_class(status: str) -> str:
        status_norm = str(status or "").strip().lower()
        if status_norm == "done":
            return "ok"
        if status_norm == "running":
            return "warn"
        if status_norm == "error":
            return "error"
        if status_norm == "canceled":
            return "muted"
        return "muted"

    def _recovery_status_label(status: str) -> str:
        status_norm = str(status or "").strip().lower()
        labels = {
            "pending": "Pendiente autorizacion ADMIN",
            "approved": "Aprobada por ADMIN",
            "rejected": "Rechazada por ADMIN",
            "completed": "Completada por usuario",
            "expired": "Codigo expirado",
        }
        return labels.get(status_norm, "Desconocido")

    def _pending_wait_label(oldest_pending_at: Optional[datetime]) -> str:
        if not isinstance(oldest_pending_at, datetime):
            return "-"
        now = datetime.utcnow()
        try:
            oldest_naive = oldest_pending_at.replace(tzinfo=None)
        except Exception:
            oldest_naive = oldest_pending_at
        delta = now - oldest_naive
        total_seconds = int(max(0, delta.total_seconds()))
        if total_seconds < 60:
            return f"{total_seconds}s"
        minutes = total_seconds // 60
        if minutes < 60:
            return f"{minutes}m"
        hours = minutes // 60
        rem_minutes = minutes % 60
        if hours < 24:
            return f"{hours}h {rem_minutes}m"
        days = hours // 24
        rem_hours = hours % 24
        return f"{days}d {rem_hours}h"

    def _build_admin_power_data() -> Dict[str, Any]:
        jobs = _jobs_snapshot(purge=True)
        running_jobs = 0
        done_jobs = 0
        error_jobs = 0
        canceled_jobs = 0
        active_users: set = set()
        history_rows: List[Dict[str, Any]] = []
        audit_rows: List[Dict[str, Any]] = []
        user_rows_map: Dict[str, Dict[str, Any]] = {}

        for job_id, job in jobs.items():
            status = str(job.get("status", "") or "").strip().lower()
            owner_rut = str(job.get("owner_rut", "") or "").strip().upper() or "SIN_USUARIO"
            process_label = _job_process_label(job)
            created_ts = _to_float(job.get("created_at", 0) or 0, default=0.0)
            updated_ts = _to_float(job.get("updated_at", 0) or 0, default=0.0)
            finished_ts = _to_float(job.get("finished_at", 0) or 0, default=0.0)

            if status == "running":
                running_jobs += 1
            elif status == "done":
                done_jobs += 1
            elif status == "error":
                error_jobs += 1
            elif status == "canceled":
                canceled_jobs += 1

            if owner_rut != "SIN_USUARIO":
                active_users.add(owner_rut)

            progress = _to_int_or(job.get("progress", 0), default=0)
            in_name = Path(str(job.get("input_file", "") or "")).name
            out_name = str(job.get("out_file", "") or "").strip() or "-"
            elapsed = str(job.get("elapsed_display", "") or "").strip() or "-"
            history_rows.append(
                {
                    "job_id": job_id,
                    "owner_rut": owner_rut,
                    "process_label": process_label,
                    "status": _status_label(status),
                    "status_class": _status_class(status),
                    "can_kill": status == "running",
                    "created_at": _fmt_job_ts(created_ts),
                    "updated_at": _fmt_job_ts(updated_ts),
                    "finished_at": _fmt_job_ts(finished_ts),
                    "elapsed_display": elapsed,
                    "progress": max(0, min(100, progress)),
                    "input_file": in_name or "-",
                    "out_file": out_name,
                    "_updated_ts": updated_ts,
                }
            )

            user_row = user_rows_map.get(owner_rut)
            if user_row is None:
                user_row = {
                    "owner_rut": owner_rut,
                    "total": 0,
                    "running": 0,
                    "done": 0,
                    "error": 0,
                    "canceled": 0,
                    "last_update": "-",
                    "last_process": "-",
                    "_last_ts": 0.0,
                }
                user_rows_map[owner_rut] = user_row
            user_row["total"] = _to_int_or(user_row.get("total", 0), default=0) + 1
            if status in {"running", "done", "error", "canceled"}:
                user_row[status] = _to_int_or(user_row.get(status, 0), default=0) + 1
            if updated_ts >= _to_float(user_row.get("_last_ts", 0) or 0, default=0.0):
                user_row["_last_ts"] = updated_ts
                user_row["last_update"] = _fmt_job_ts(updated_ts)
                user_row["last_process"] = process_label

            admin_selected = job.get("admin_selected")
            is_admin_load = isinstance(admin_selected, list) or "admin-db-update" in str(job.get("repeat_url", "") or "")
            if not is_admin_load:
                continue
            selected_tables = [str(v).strip() for v in (admin_selected or []) if str(v).strip()]
            selected_text = ", ".join(selected_tables) if selected_tables else "-"
            summary_rows = job.get("admin_summary")
            files: List[str] = []
            rows_loaded = 0
            if isinstance(summary_rows, list):
                for row in summary_rows:
                    if not isinstance(row, dict):
                        continue
                    file_name = str(row.get("Archivo", "") or "").strip()
                    if file_name:
                        files.append(file_name)
                    rows_loaded += _to_int_or(row.get("Filas cargadas", 0), default=0)
            files_text = ", ".join(sorted(set(files))) if files else "-"
            audit_rows.append(
                {
                    "job_id": job_id,
                    "owner_rut": owner_rut,
                    "status": _status_label(status),
                    "status_class": _status_class(status),
                    "selected_tables": selected_text,
                    "files": files_text,
                    "rows_loaded": rows_loaded,
                    "run_cores": "SI" if bool(job.get("run_cores", False)) else "NO",
                    "cores_updated": "SI" if bool(job.get("cores_updated", False)) else "NO",
                    "elapsed_display": elapsed,
                    "updated_at": _fmt_job_ts(updated_ts),
                    "_updated_ts": updated_ts,
                }
            )

        history_rows.sort(key=lambda row: _to_float(row.get("_updated_ts", 0) or 0, default=0.0), reverse=True)
        audit_rows.sort(key=lambda row: _to_float(row.get("_updated_ts", 0) or 0, default=0.0), reverse=True)
        user_rows = list(user_rows_map.values())
        user_rows.sort(
            key=lambda row: (
                -_to_int_or(row.get("total", 0), default=0),
                -_to_float(row.get("_last_ts", 0) or 0, default=0.0),
            )
        )
        for row in history_rows:
            row.pop("_updated_ts", None)
        for row in audit_rows:
            row.pop("_updated_ts", None)
        for row in user_rows:
            row.pop("_last_ts", None)

        security_rows: List[Dict[str, Any]] = []
        recovery_rows: List[Dict[str, Any]] = []
        pending_recovery_rows: List[Dict[str, Any]] = []
        oldest_pending_at: Optional[datetime] = None
        security_health: Dict[str, Any] = {
            "active_accounts": 0,
            "pending_recovery_requests": 0,
            "active_lockouts": 0,
            "failed_events_24h": 0,
            "recovery_requests_24h": 0,
            "alerts": [],
        }
        try:
            security_health = auth_db.get_security_health()
            for event in auth_db.get_security_events(limit=250):
                if not isinstance(event, dict):
                    continue
                ev_ts = event.get("created_at")
                ev_ts_fmt = "-"
                if isinstance(ev_ts, datetime):
                    try:
                        ev_ts_fmt = ev_ts.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
                    except Exception:
                        ev_ts_fmt = str(ev_ts)
                security_rows.append(
                    {
                        "event_type": str(event.get("event_type", "") or "").strip(),
                        "login_id": str(event.get("login_id", "") or "").strip() or "-",
                        "actor_login": str(event.get("actor_login", "") or "").strip() or "-",
                        "severity": str(event.get("severity", "") or "").strip() or "info",
                        "ip_address": str(event.get("ip_address", "") or "").strip() or "-",
                        "detail": str(event.get("detail", "") or "").strip() or "-",
                        "created_at": ev_ts_fmt,
                    }
                )
            for req in auth_db.list_password_recovery_requests(status="", limit=250):
                if not isinstance(req, dict):
                    continue
                requested_at = req.get("requested_at")
                reviewed_at = req.get("reviewed_at")
                expires_at = req.get("verification_expires_at")
                requested_ts = 0.0
                if isinstance(requested_at, datetime):
                    try:
                        requested_ts = float(requested_at.replace(tzinfo=None).timestamp())
                    except Exception:
                        requested_ts = 0.0
                status_raw = str(req.get("status", "") or "").strip().lower()
                is_pending = status_raw == "pending"
                recovery_rows.append(
                    {
                        "request_id": str(req.get("request_id", "") or "").strip(),
                        "login_id": str(req.get("login_id", "") or "").strip() or "-",
                        "status": status_raw or "unknown",
                        "status_label": _recovery_status_label(status_raw),
                        "status_class": _status_class(
                            "running"
                            if is_pending
                            else (
                                "done"
                                if status_raw in {"approved", "completed"}
                                else "error"
                            )
                        ),
                        "request_note": str(req.get("request_note", "") or "").strip() or "-",
                        "request_email": str(req.get("request_email", "") or "").strip() or "-",
                        "request_ip": str(req.get("request_ip", "") or "").strip() or "-",
                        "requested_at": (
                            requested_at.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
                            if isinstance(requested_at, datetime)
                            else "-"
                        ),
                        "reviewed_at": (
                            reviewed_at.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
                            if isinstance(reviewed_at, datetime)
                            else "-"
                        ),
                        "reviewed_by": str(req.get("reviewed_by", "") or "").strip() or "-",
                        "expires_at": (
                            expires_at.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
                            if isinstance(expires_at, datetime)
                            else "-"
                        ),
                        "can_review": is_pending,
                        "_requested_ts": requested_ts,
                    }
                )
                if is_pending:
                    pending_recovery_rows.append(recovery_rows[-1])
                    if isinstance(requested_at, datetime):
                        if (oldest_pending_at is None) or (requested_at < oldest_pending_at):
                            oldest_pending_at = requested_at
        except Exception as e:
            security_health["alerts"] = [f"No fue posible leer auditoria de seguridad: {e}"]

        recovery_rows.sort(key=lambda row: _to_float(row.get("_requested_ts", 0) or 0, default=0.0), reverse=True)
        pending_recovery_rows.sort(key=lambda row: _to_float(row.get("_requested_ts", 0) or 0, default=0.0), reverse=True)
        for row in recovery_rows:
            row.pop("_requested_ts", None)
        for row in pending_recovery_rows:
            row.pop("_requested_ts", None)

        health = {
            "jobs_backend": str(JOBS_BACKEND or "").upper() or "DESCONOCIDO",
            "session_backend": str(SESSION_BACKEND or "").upper() or "DESCONOCIDO",
            "db_updating": _is_db_updating(),
            "max_concurrent_jobs": MAX_CONCURRENT_JOBS,
            "total_jobs": len(jobs),
            "running_jobs": running_jobs,
            "done_jobs": done_jobs,
            "error_jobs": error_jobs,
            "canceled_jobs": canceled_jobs,
            "active_users": len(active_users),
            "audit_jobs": len(audit_rows),
            "auth_active_accounts": int(security_health.get("active_accounts", 0) or 0),
            "auth_pending_recovery": int(security_health.get("pending_recovery_requests", 0) or 0),
            "auth_active_lockouts": int(security_health.get("active_lockouts", 0) or 0),
            "auth_failed_24h": int(security_health.get("failed_events_24h", 0) or 0),
            "auth_recovery_24h": int(security_health.get("recovery_requests_24h", 0) or 0),
            "auth_alerts": list(security_health.get("alerts", []) or []),
            "auth_pending_oldest_wait": _pending_wait_label(oldest_pending_at),
            "auth_pending_requires_action": len(pending_recovery_rows) > 0,
        }
        return {
            "health": health,
            "user_rows": user_rows[:200],
            "history_rows": history_rows[:300],
            "audit_rows": audit_rows[:200],
            "security_rows": security_rows[:250],
            "recovery_rows": recovery_rows[:250],
            "pending_recovery_rows": pending_recovery_rows[:100],
        }


    def calculo_mediana():
        class_order = ["IC", "Dental", "IQ", "PROC"]
        class_suffix = {"IC": "ic", "Dental": "dental", "IQ": "iq", "PROC": "proc"}
        if request.method == "POST":
            stats = None
            out_file = ""
            elapsed_display = ""
            apply_input = {cls: (request.form.get(f"apply_{class_suffix[cls]}") == "on") for cls in class_order}
            active_classes = [cls for cls in class_order if apply_input.get(cls)]
            fecha_corte_input = {cls: request.form.get(f"fecha_corte_{class_suffix[cls]}", "") for cls in class_order}
            fecha_Percentil_input = {cls: request.form.get(f"fecha_Percentil_{class_suffix[cls]}", "") for cls in class_order}
            ideales_input = {cls: request.form.get(f"ideal_{class_suffix[cls]}", "") for cls in class_order}
            if not active_classes:
                flash("Debes seleccionar al menos una clasificacion para aplicar el calculo.")
                return render_template(
                    "calculo_mediana.html",
                    title=APP_TITLE,
                    stats=stats,
                    out_file=out_file,
                    elapsed_display=elapsed_display,
                    apply_input=apply_input,
                    fecha_corte_input=fecha_corte_input,
                    fecha_Percentil_input=fecha_Percentil_input,
                    ideales_input=ideales_input,
                )
            f = request.files.get("workfile")
            if not f or f.filename == "":
                flash("Debes seleccionar un archivo (.xlsx, .xlsb o .csv).")
                return render_template(
                    "calculo_mediana.html",
                    title=APP_TITLE,
                    stats=stats,
                    out_file=out_file,
                    elapsed_display=elapsed_display,
                    apply_input=apply_input,
                    fecha_corte_input=fecha_corte_input,
                    fecha_Percentil_input=fecha_Percentil_input,
                    ideales_input=ideales_input,
                )
            if not allowed_file(f.filename):
                flash("Formato invalido. Solo se permite .xlsx, .xlsb o .csv")
                return render_template(
                    "calculo_mediana.html",
                    title=APP_TITLE,
                    stats=stats,
                    out_file=out_file,
                    elapsed_display=elapsed_display,
                    apply_input=apply_input,
                    fecha_corte_input=fecha_corte_input,
                    fecha_Percentil_input=fecha_Percentil_input,
                    ideales_input=ideales_input,
                )

            fechas_corte: Dict[str, datetime] = {}
            fechas_Percentil: Dict[str, datetime] = {}
            for cls in active_classes:
                fecha_corte_dt = parse_excel_date(fecha_corte_input.get(cls))
                if not fecha_corte_dt:
                    flash(f"Debes ingresar una fecha de corte valida para {cls}.")
                    return render_template(
                        "calculo_mediana.html",
                        title=APP_TITLE,
                        stats=stats,
                        out_file=out_file,
                        elapsed_display=elapsed_display,
                        apply_input=apply_input,
                        fecha_corte_input=fecha_corte_input,
                        fecha_Percentil_input=fecha_Percentil_input,
                        ideales_input=ideales_input,
                    )
                fecha_Percentil_dt = parse_excel_date(fecha_Percentil_input.get(cls))
                if not fecha_Percentil_dt:
                    flash(f"Debes ingresar una fecha Percentil valida para {cls}.")
                    return render_template(
                        "calculo_mediana.html",
                        title=APP_TITLE,
                        stats=stats,
                        out_file=out_file,
                        elapsed_display=elapsed_display,
                        apply_input=apply_input,
                        fecha_corte_input=fecha_corte_input,
                        fecha_Percentil_input=fecha_Percentil_input,
                        ideales_input=ideales_input,
                    )
                fechas_corte[cls] = fecha_corte_dt
                fechas_Percentil[cls] = fecha_Percentil_dt

            ideales: Dict[str, int] = {}
            for cls in active_classes:
                raw = ideales_input.get(cls, "")
                val = _to_int(raw)
                if val is None:
                    flash(f"El ideal de {cls} debe ser un numero entero.")
                    return render_template(
                        "calculo_mediana.html",
                        title=APP_TITLE,
                        stats=stats,
                        out_file=out_file,
                        elapsed_display=elapsed_display,
                        apply_input=apply_input,
                        fecha_corte_input=fecha_corte_input,
                        fecha_Percentil_input=fecha_Percentil_input,
                        ideales_input=ideales_input,
                    )
                ideales[cls] = int(val)

            if not _can_start_new_job():
                flash(f"Hay {MAX_CONCURRENT_JOBS} procesos en ejecución. Espera a que termine uno para iniciar otro.")
                return render_template(
                    "calculo_mediana.html",
                    title=APP_TITLE,
                    stats=stats,
                    out_file=out_file,
                    elapsed_display=elapsed_display,
                    apply_input=apply_input,
                    fecha_corte_input=fecha_corte_input,
                    fecha_Percentil_input=fecha_Percentil_input,
                    ideales_input=ideales_input,
                )

            filename = secure_filename(f.filename)
            saved = UPLOAD_DIR / f"LE_NOGES_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex}_{filename}"
            f.save(saved)

            job_id = uuid.uuid4().hex
            init_error = _safe_init_job(
                job_id,
                owner_rut=_current_user_rut(),
                input_file=str(saved),
                repeat_url=url_for("categoria", slug="vacio"),
                cleanup_paths=[saved],
            )
            if init_error:
                flash(init_error)
                return render_template(
                    "calculo_mediana.html",
                    title=APP_TITLE,
                    stats=stats,
                    out_file=out_file,
                    elapsed_display=elapsed_display,
                    apply_input=apply_input,
                    fecha_corte_input=fecha_corte_input,
                    fecha_Percentil_input=fecha_Percentil_input,
                    ideales_input=ideales_input,
                )
            _update_job(
                job_id,
                apply_input=apply_input,
                fecha_corte_input=fecha_corte_input,
                fecha_Percentil_input=fecha_Percentil_input,
                ideales_input=ideales_input,
            )
            thread = threading.Thread(
                target=_run_mediana_job,
                args=(job_id, saved, fechas_corte, fechas_Percentil, ideales, active_classes),
                daemon=True
            )
            thread.start()
            return render_template(
                "processing.html",
                title=APP_TITLE,
                job_id=job_id,
                redirect_url=url_for("calculo_mediana_result", job_id=job_id)
            )

        return render_template(
            "calculo_mediana.html",
            title=APP_TITLE,
            stats=None,
            out_file="",
            elapsed_display="",
            apply_input={cls: True for cls in class_order},
            fecha_corte_input={cls: "" for cls in class_order},
            fecha_Percentil_input={cls: "" for cls in class_order},
            ideales_input={cls: "" for cls in class_order},
        )

    def estadisticas():
        stats = None
        source = "archivo"

        if request.method == "POST":
            source = "archivo"
            if not _can_start_new_job():
                flash(f"Hay {MAX_CONCURRENT_JOBS} procesos en ejecución. Espera a que termine uno para iniciar otro.")
                return render_template("estadisticas.html", title=APP_TITLE, source=source, stats=None)
            f = request.files.get("workfile")
            if not f or f.filename == "":
                flash("Debes seleccionar un archivo (.xlsx, .xlsb o .csv) para estadí­sticas.")
                return render_template("estadisticas.html", title=APP_TITLE, source=source, stats=None)
            if not allowed_file(f.filename):
                flash("Formato inválido. Solo se permite .xlsx, .xlsb o .csv")
                return render_template("estadisticas.html", title=APP_TITLE, source=source, stats=None)
            filename = secure_filename(f.filename)
            saved = UPLOAD_DIR / f"LE_NOGES_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex}_{filename}"
            f.save(saved)

            job_id = uuid.uuid4().hex
            init_error = _safe_init_job(
                job_id,
                owner_rut=_current_user_rut(),
                input_file=str(saved) if saved else "",
                repeat_url=url_for("categoria", slug="estadisticas"),
                cleanup_paths=[saved],
            )
            if init_error:
                flash(init_error)
                return render_template("estadisticas.html", title=APP_TITLE, source=source, stats=None)
            thread = threading.Thread(
                target=_run_stats_job,
                args=(job_id, source, saved),
                daemon=True
            )
            thread.start()

            return render_template(
                "processing.html",
                title=APP_TITLE,
                job_id=job_id,
                redirect_url=url_for("estadisticas_result", job_id=job_id)
            )

        return render_template("estadisticas.html", title=APP_TITLE, source=source, stats=stats)


    def estadisticas_cruces():
        stats = None
        if request.method == "POST":
            if not _can_start_new_job():
                flash(f"Hay {MAX_CONCURRENT_JOBS} procesos en ejecución. Espera a que termine uno para iniciar otro.")
                return render_template("estadisticas_cruces.html", title=APP_TITLE, stats=None)
            if "workfile" not in request.files:
                flash("No se recibió archivo.")
                return render_template("estadisticas_cruces.html", title=APP_TITLE, stats=None)

            f = request.files["workfile"]
            if f.filename == "":
                flash("Debes seleccionar un archivo (.xlsx, .xlsb o .csv)")
                return render_template("estadisticas_cruces.html", title=APP_TITLE, stats=None)

            if not allowed_file(f.filename):
                flash("Formato invalido. Solo se permite .xlsx, .xlsb o .csv")
                return render_template("estadisticas_cruces.html", title=APP_TITLE, stats=None)

            filename = secure_filename(f.filename)
            saved = UPLOAD_DIR / f"LE_NOGES_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex}_{filename}"
            f.save(saved)

            job_id = uuid.uuid4().hex
            init_error = _safe_init_job(
                job_id,
                owner_rut=_current_user_rut(),
                input_file=str(saved),
                repeat_url=url_for("categoria", slug="estadisticas-cruces"),
                cleanup_paths=[saved],
            )
            if init_error:
                flash(init_error)
                return render_template("estadisticas_cruces.html", title=APP_TITLE, stats=None)
            thread = threading.Thread(
                target=_run_cross_stats_job,
                args=(job_id, saved),
                daemon=True
            )
            thread.start()

            return render_template(
                "processing.html",
                title=APP_TITLE,
                job_id=job_id,
                redirect_url=url_for("estadisticas_cruces_result", job_id=job_id)
            )

        return render_template("estadisticas_cruces.html", title=APP_TITLE, stats=stats)


    def busqueda_sudais():
        rut_input = ""
        id_local_input = ""
        searched = False
        results_rows: List[Dict[str, str]] = []
        rows_total = 0
        summary = {
            "historico": 0,
            "nominas": 0,
            "total": 0,
        }

        if request.method == "POST":
            searched = True
            rut_input = (request.form.get("rut", "") or "").strip()
            id_local_input = (request.form.get("id_local", "") or "").strip()

            if not rut_input and not id_local_input:
                flash("Debes ingresar RUT o ID_LOCAL para buscar.")
                return render_template(
                    "busqueda_sudais.html",
                    title=APP_TITLE,
                    rut_input=rut_input,
                    id_local_input=id_local_input,
                    searched=searched,
                    results_rows=results_rows,
                    rows_total=rows_total,
                    summary=summary,
                )

            rut_query = normalize_rut_concat(rut_input) if rut_input else ""
            id_query = normalize_id(id_local_input) if id_local_input else ""

            if rut_input and not rut_query:
                flash("El RUT ingresado no es valido.")
                return render_template(
                    "busqueda_sudais.html",
                    title=APP_TITLE,
                    rut_input=rut_input,
                    id_local_input=id_local_input,
                    searched=searched,
                    results_rows=results_rows,
                    rows_total=rows_total,
                    summary=summary,
                )
            if id_local_input and not id_query:
                flash("El ID_LOCAL ingresado no es valido.")
                return render_template(
                    "busqueda_sudais.html",
                    title=APP_TITLE,
                    rut_input=rut_input,
                    id_local_input=id_local_input,
                    searched=searched,
                    results_rows=results_rows,
                    rows_total=rows_total,
                    summary=summary,
                )

            try:
                all_rows, _export_df, counts = _search_cases_full_rows(
                    rut_filter=rut_query,
                    id_filter=id_query,
                    build_preview=True,
                )
            except Exception as e:
                flash(f"Error tecnico al buscar casos: {e}")
                return render_template(
                    "busqueda_sudais.html",
                    title=APP_TITLE,
                    rut_input=rut_input,
                    id_local_input=id_local_input,
                    searched=searched,
                    results_rows=results_rows,
                    rows_total=rows_total,
                    summary=summary,
                )

            results_rows, rows_total = _slice_preview_rows(all_rows, TABLE_PREVIEW_LIMIT)
            summary = {
                "historico": int(counts.get("historico", 0)),
                "nominas": int(counts.get("nominas", 0)),
                "total": int(counts.get("total", 0)),
            }

            if summary["total"] == 0:
                if rut_query and id_query:
                    flash("No hay coincidencias para ese RUT + ID_LOCAL en historico o nominas.")
                else:
                    flash("No se encontraron casos para el criterio ingresado.")

        return render_template(
            "busqueda_sudais.html",
            title=APP_TITLE,
            rut_input=rut_input,
            id_local_input=id_local_input,
            searched=searched,
            results_rows=results_rows,
            rows_total=rows_total,
            summary=summary,
        )


    @app.get("/busqueda-sudais/export")
    def busqueda_sudais_export():
        rut_input = (request.args.get("rut", "") or "").strip()
        id_local_input = (request.args.get("id_local", "") or "").strip()

        if not rut_input and not id_local_input:
            flash("Debes ingresar RUT o ID_LOCAL para exportar.")
            return redirect(url_for("categoria", slug="busqueda-sudais"))

        rut_query = normalize_rut_concat(rut_input) if rut_input else ""
        id_query = normalize_id(id_local_input) if id_local_input else ""

        if rut_input and not rut_query:
            flash("El RUT ingresado no es valido para exportar.")
            return redirect(url_for("categoria", slug="busqueda-sudais"))
        if id_local_input and not id_query:
            flash("El ID_LOCAL ingresado no es valido para exportar.")
            return redirect(url_for("categoria", slug="busqueda-sudais"))

        try:
            _rows, export_df, counts = _search_cases_full_rows(
                rut_filter=rut_query,
                id_filter=id_query,
                build_preview=False,
            )
        except Exception as e:
            flash(f"Error tecnico al exportar busqueda: {e}")
            return redirect(url_for("categoria", slug="busqueda-sudais"))

        if export_df.empty or int(counts.get("total", 0)) == 0:
            flash("No hay casos encontrados para exportar.")
            return redirect(url_for("categoria", slug="busqueda-sudais"))

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"LE_NOGES_busqueda_sudais_{stamp}.xlsx"
        output = io.BytesIO()
        with pd.ExcelWriter(
            output,
            engine="openpyxl",
            date_format="DD-MM-YYYY",
            datetime_format="DD-MM-YYYY",
        ) as writer:
            format_dates_for_export(export_df).to_excel(writer, index=False, sheet_name="Busqueda Sudais")
            apply_short_date_format_to_workbook(writer.book)
        output.seek(0)
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


    @app.route("/admin-db-update", methods=["GET", "POST"])
    def admin_db_update():
        if not _current_user_is_admin():
            flash("No tienes permisos para acceder a este modulo.")
            return redirect(url_for("home"))

        selected_input = {k: False for k, _ in ADMIN_RAW_TABLES}
        mode_input = {k: "replace" for k, _ in ADMIN_RAW_TABLES}
        run_cores = True
        tables_ui = [
            {
                "key": k,
                "label": label,
                "accept": _admin_accept_attr_for_table(k),
                "allowed_text": _admin_allowed_ext_text_for_table(k),
                "supports_append": _admin_table_allows_append(k),
            }
            for k, label in ADMIN_RAW_TABLES
        ]

        if request.method == "POST":
            if not _can_start_new_job():
                flash(f"Hay {MAX_CONCURRENT_JOBS} procesos en ejecucion. Espera a que termine uno para iniciar otro.")
                return render_template(
                    "admin_db_update.html",
                    title=APP_TITLE,
                    admin_tables=tables_ui,
                    selected=selected_input,
                    load_modes=mode_input,
                )

            selected_tables: List[str] = []
            upload_paths: Dict[str, Path] = {}
            selected_modes: Dict[str, str] = {}

            for table_name, _label in ADMIN_RAW_TABLES:
                checked = request.form.get(f"use_{table_name}") == "on"
                selected_input[table_name] = checked
                requested_mode = request.form.get(f"mode_{table_name}", "replace")
                resolved_mode = _admin_resolve_load_mode(table_name, requested_mode)
                mode_input[table_name] = resolved_mode
                if not checked:
                    continue

                selected_tables.append(table_name)
                selected_modes[table_name] = resolved_mode
                f = request.files.get(f"file_{table_name}")
                if not f or f.filename == "":
                    for path in upload_paths.values():
                        _safe_unlink(path)
                    allowed = _admin_allowed_ext_text_for_table(table_name)
                    flash(f"Debes adjuntar un archivo para {table_name} ({allowed}).")
                    return render_template(
                        "admin_db_update.html",
                        title=APP_TITLE,
                        admin_tables=tables_ui,
                        selected=selected_input,
                        load_modes=mode_input,
                    )
                if not _admin_is_allowed_file_for_table(table_name, f.filename):
                    for path in upload_paths.values():
                        _safe_unlink(path)
                    allowed = _admin_allowed_ext_text_for_table(table_name)
                    flash(f"Formato invalido para {table_name}. Permitidos: {allowed}")
                    return render_template(
                        "admin_db_update.html",
                        title=APP_TITLE,
                        admin_tables=tables_ui,
                        selected=selected_input,
                        load_modes=mode_input,
                    )

                safe_name = secure_filename(f.filename)
                saved = UPLOAD_DIR / (
                    f"LE_NOGES_upload_admin_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex}_{table_name}_{safe_name}"
                )
                f.save(saved)
                upload_paths[table_name] = saved

            if not selected_tables:
                flash("Debes seleccionar al menos una tabla para actualizar.")
                return render_template(
                    "admin_db_update.html",
                    title=APP_TITLE,
                    admin_tables=tables_ui,
                    selected=selected_input,
                    load_modes=mode_input,
                )

            job_id = uuid.uuid4().hex
            init_error = _safe_init_job(
                job_id,
                owner_rut=_current_user_rut(),
                input_file="",
                repeat_url=url_for("admin_db_update"),
                cleanup_paths=list(upload_paths.values()),
            )
            if init_error:
                flash(init_error)
                return render_template(
                    "admin_db_update.html",
                    title=APP_TITLE,
                    admin_tables=tables_ui,
                    selected=selected_input,
                    load_modes=mode_input,
                )
            _update_job(
                job_id,
                admin_selected=selected_tables,
                run_cores=run_cores,
                admin_load_modes=selected_modes,
            )
            thread = threading.Thread(
                target=_run_admin_update_job,
                args=(job_id, _current_user_rut(), upload_paths, selected_tables, run_cores, selected_modes),
                daemon=True,
            )
            thread.start()
            return render_template(
                "processing.html",
                title=APP_TITLE,
                job_id=job_id,
                redirect_url=url_for("admin_db_update_result", job_id=job_id),
            )

        return render_template(
            "admin_db_update.html",
            title=APP_TITLE,
            admin_tables=tables_ui,
            selected=selected_input,
            load_modes=mode_input,
        )

    @app.get("/admin-power")
    def admin_power():
        if not _current_user_is_admin():
            flash("No tienes permisos para acceder a este modulo.")
            return redirect(url_for("home"))
        dashboard = _build_admin_power_data()
        return render_template(
            "admin_power.html",
            title=APP_TITLE,
            health=dashboard["health"],
            user_rows=dashboard["user_rows"],
            history_rows=dashboard["history_rows"],
            audit_rows=dashboard["audit_rows"],
            security_rows=dashboard["security_rows"],
            recovery_rows=dashboard["recovery_rows"],
            pending_recovery_rows=dashboard["pending_recovery_rows"],
        )


    @app.post("/admin-power/kill/<job_id>")
    def admin_kill_job(job_id: str):
        if not _current_user_is_admin():
            flash("No tienes permisos para acceder a este modulo.")
            return redirect(url_for("home"))
        job = _get_owned_or_admin_job(job_id)
        if not job:
            flash("Job no encontrado.")
            return redirect(url_for("admin_power"))
        status = str(job.get("status", "")).strip().lower()
        if status in {"done", "error", "canceled"}:
            flash("No se puede cancelar este proceso.")
            return redirect(url_for("admin_power"))
        if _request_cancel_job(job_id):
            flash(f"Cancelacion solicitada para job {job_id}.")
        else:
            flash("No fue posible solicitar la cancelacion.")
        return redirect(url_for("admin_power"))


    @app.post("/admin-power/recovery/<request_id>/<action>")
    def admin_review_recovery(request_id: str, action: str):
        if not _current_user_is_admin():
            flash("No tienes permisos para acceder a este modulo.")
            return redirect(url_for("home"))
        note = (request.form.get("review_note", "") or "").strip()
        client_ip = str(request.headers.get("X-Forwarded-For", request.remote_addr or "")).split(",")[0].strip()
        ok, msg, code = auth_db.review_password_recovery_request(
            request_id=request_id,
            action=action,
            actor_login=_current_user_rut(),
            review_note=note,
            ip_address=client_ip,
        )
        flash(msg)
        if ok and str(action or "").strip().lower() == "approve" and code:
            flash(f"Codigo de verificacion temporal para la solicitud {request_id}: {code}")
            flash("Comparte el codigo por canal seguro. Vence en el tiempo configurado por politica.")
        return redirect(url_for("admin_power"))


    @app.route("/categoria/<slug>", methods=["GET", "POST"])
    def categoria(slug: str):
        if slug not in CATEGORIES:
            return redirect(url_for("home"))

        if slug == "estadisticas":
            return estadisticas()
        if slug == "estadisticas-cruces":
            return estadisticas_cruces()
        if slug == "busqueda-sudais":
            return busqueda_sudais()
        if slug == "vacio":
            return calculo_mediana()
        if slug == "manuales-de-uso":
            return manuales_de_uso()

        cat = CATEGORIES[slug]

        if request.method == "POST":
            if not _can_start_new_job():
                flash(f"Hay {MAX_CONCURRENT_JOBS} procesos en ejecución. Espera a que termine uno para iniciar otro.")
                return redirect(request.url)
            if "workfile" not in request.files:
                flash("No se recibió archivo.")
                return redirect(request.url)

            f = request.files["workfile"]
            if f.filename == "":
                flash("Debes seleccionar un archivo (.xlsx, .xlsb o .csv)")
                return redirect(request.url)

            if not allowed_file(f.filename):
                flash("Formato inválido. Solo se permite .xlsx, .xlsb o .csv")
                return redirect(request.url)

            filename = secure_filename(f.filename)
            saved = UPLOAD_DIR / f"LE_NOGES_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex}_{filename}"
            f.save(saved)
            selected = {k: False for k, _ in cat["options"]}
            for k, _label in cat["options"]:
                selected[k] = (request.form.get(k) == "on")

            job_id = uuid.uuid4().hex
            init_error = _safe_init_job(
                job_id,
                owner_rut=_current_user_rut(),
                input_file=str(saved),
                repeat_url=url_for("categoria", slug=slug),
                cleanup_paths=[saved],
            )
            if init_error:
                flash(init_error)
                return redirect(request.url)
            thread = threading.Thread(
                target=_run_job,
                args=(job_id, saved, selected),
                daemon=True
            )
            thread.start()

            return render_template(
                "processing.html",
                title=APP_TITLE,
                job_id=job_id
            )

        return render_template("category.html", title=APP_TITLE, cat=cat, slug=slug)

    @app.route("/calculo_mediana_result/<job_id>", methods=["GET", "POST"])
    def calculo_mediana_result(job_id: str):
        def _as_mediana_inputs(raw: Any) -> Dict[str, str]:
            base = {"IC": "", "Dental": "", "IQ": "", "PROC": ""}
            if not isinstance(raw, dict):
                return base
            out = dict(base)
            for k in base.keys():
                v = raw.get(k, "")
                out[k] = "" if v is None else str(v)
            return out

        def _as_apply_inputs(raw: Any) -> Dict[str, bool]:
            base = {"IC": True, "Dental": True, "IQ": True, "PROC": True}
            if not isinstance(raw, dict):
                return base
            out = dict(base)
            for k in base.keys():
                out[k] = bool(raw.get(k))
            return out

        job = _get_owned_job(job_id)
        if not job:
            flash("Proceso no encontrado o sin permisos.")
            return redirect(url_for("home"))
        if job.get("status") == "error":
            flash(f"Error tecnico: {job.get('error', 'Desconocido')}")
            return render_template(
                "calculo_mediana.html",
                title=APP_TITLE,
                stats=None,
                out_file="",
                elapsed_display="",
                apply_input=_as_apply_inputs(job.get("apply_input")),
                fecha_corte_input=_as_mediana_inputs(job.get("fecha_corte_input")),
                fecha_Percentil_input=_as_mediana_inputs(job.get("fecha_Percentil_input")),
                ideales_input=_as_mediana_inputs(job.get("ideales_input")),
            )
        if job.get("status") == "canceled":
            flash("Proceso cancelado por usuario.")
            return render_template(
                "calculo_mediana.html",
                title=APP_TITLE,
                stats=None,
                out_file="",
                elapsed_display="",
                apply_input=_as_apply_inputs(job.get("apply_input")),
                fecha_corte_input=_as_mediana_inputs(job.get("fecha_corte_input")),
                fecha_Percentil_input=_as_mediana_inputs(job.get("fecha_Percentil_input")),
                ideales_input=_as_mediana_inputs(job.get("ideales_input")),
            )
        if job.get("status") != "done":
            return render_template(
                "processing.html",
                title=APP_TITLE,
                job_id=job_id,
                redirect_url=url_for("calculo_mediana_result", job_id=job_id)
            )
        return render_template(
            "calculo_mediana.html",
            title=APP_TITLE,
            stats=job.get("mediana_stats"),
            out_file=job.get("out_file", ""),
            elapsed_display=job.get("elapsed_display", ""),
            apply_input=_as_apply_inputs(job.get("apply_input")),
            fecha_corte_input=_as_mediana_inputs(job.get("fecha_corte_input")),
            fecha_Percentil_input=_as_mediana_inputs(job.get("fecha_Percentil_input")),
            ideales_input=_as_mediana_inputs(job.get("ideales_input")),
            job_id=job_id,
        )


    @app.route("/estadisticas_result/<job_id>", methods=["GET", "POST"])
    def estadisticas_result(job_id: str):
        job = _get_owned_job(job_id)
        if not job:
            flash("Proceso no encontrado o sin permisos.")
            return redirect(url_for("home"))
        if job.get("status") == "error":
            flash(f"Error técnico: {job.get('error', 'Desconocido')}")
            return redirect(url_for("home"))
        if job.get("status") == "canceled":
            flash("Proceso cancelado por usuario.")
            return redirect(url_for("categoria", slug="estadisticas"))
        if job.get("status") != "done":
            return render_template(
                "processing.html",
                title=APP_TITLE,
                job_id=job_id,
                redirect_url=url_for("estadisticas_result", job_id=job_id)
            )
        stats = job.get("stats")
        source = job.get("source", "archivo")
        if stats and isinstance(stats, dict) and stats.get("error"):
            flash(stats["error"])
        return render_template("estadisticas.html", title=APP_TITLE, source=source, stats=stats, job_id=job_id)


    @app.route("/estadisticas_cruces_result/<job_id>", methods=["GET", "POST"])
    def estadisticas_cruces_result(job_id: str):
        job = _get_owned_job(job_id)
        if not job:
            flash("Proceso no encontrado o sin permisos.")
            return redirect(url_for("home"))
        if job.get("status") == "error":
            flash(f"Error tecnico: {job.get('error', 'Desconocido')}")
            return redirect(url_for("home"))
        if job.get("status") == "canceled":
            flash("Proceso cancelado por usuario.")
            return redirect(url_for("categoria", slug="estadisticas-cruces"))
        if job.get("status") != "done":
            return render_template(
                "processing.html",
                title=APP_TITLE,
                job_id=job_id,
                redirect_url=url_for("estadisticas_cruces_result", job_id=job_id)
            )
        stats = job.get("cross_stats")
        if stats and isinstance(stats, dict) and stats.get("error"):
            flash(stats["error"])
        return render_template("estadisticas_cruces.html", title=APP_TITLE, stats=stats, job_id=job_id)


    @app.get("/admin-db-update-result/<job_id>")
    def admin_db_update_result(job_id: str):
        if not _current_user_is_admin():
            flash("No tienes permisos para acceder a este modulo.")
            return redirect(url_for("home"))
        job = _get_owned_job(job_id)
        if not job:
            flash("Proceso no encontrado o sin permisos.")
            return redirect(url_for("admin_db_update"))
        if job.get("status") == "error":
            flash(f"Error tecnico: {job.get('error', 'Desconocido')}")
            return redirect(url_for("admin_db_update"))
        if job.get("status") == "canceled":
            flash("Proceso cancelado por usuario.")
            return redirect(url_for("admin_db_update"))
        if job.get("status") != "done":
            return render_template(
                "processing.html",
                title=APP_TITLE,
                job_id=job_id,
                redirect_url=url_for("admin_db_update_result", job_id=job_id),
            )
        return render_template(
            "result.html",
            title=APP_TITLE,
            out_file=job.get("out_file", ""),
            elapsed_display=job.get("elapsed_display", ""),
            repeat_url=url_for("admin_db_update"),
        )


    @app.get("/estadisticas_export/<job_id>")
    def estadisticas_export(job_id: str):
        job = _get_owned_job(job_id)
        if not job:
            flash("Proceso no encontrado o sin permisos.")
            return redirect(url_for("home"))
        if job.get("status") != "done":
            flash("Las estadí­sticas aun estan en proceso.")
            return redirect(url_for("estadisticas_result", job_id=job_id))
        stats = job.get("stats")
        if not stats or (isinstance(stats, dict) and stats.get("error")):
            flash("No hay estadisticas disponibles para descargar.")
            return redirect(url_for("estadisticas_result", job_id=job_id))
        filename = f"LE_NOGES_estadisticas_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{job_id}.xlsx"
        out_path = OUTPUT_DIR / filename
        _stats_to_excel(stats, out_path)
        return send_file(out_path, as_attachment=True)


    @app.get("/progress/<job_id>")
    def progress(job_id: str):
        job = _get_owned_job_fresh(job_id)
        if not job:
            response = jsonify({"status": "error", "error": "Job no encontrado"})
            response.status_code = 404
        else:
            response = jsonify(_json_safe_value(job))
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0, private"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response


    @app.post("/cancel/<job_id>")
    def cancel_job(job_id: str):
        job = _get_owned_or_admin_job(job_id)
        if not job:
            return jsonify({"status": "error", "error": "Job no encontrado"}), 404
        status = str(job.get("status", ""))
        if status in {"done", "error", "canceled"}:
            return jsonify({"status": status, "message": "No se puede cancelar este proceso."})
        if _request_cancel_job(job_id):
            return jsonify({"status": "cancel_requested"})
        refreshed = _get_owned_or_admin_job(job_id)
        return jsonify({"status": str(refreshed.get("status", "unknown"))})


    @app.get("/result/<job_id>")
    def result(job_id: str):
        job = _get_owned_job(job_id)
        if not job:
            flash("Proceso no encontrado o sin permisos.")
            return redirect(url_for("home"))
        if job.get("status") == "error":
            flash(f"Error tÃ©cnico: {job.get('error', 'Desconocido')}")
            return redirect(url_for("home"))
        if job.get("status") == "canceled":
            flash("Proceso cancelado por usuario.")
            return redirect(job.get("repeat_url", "") or url_for("home"))
        if job.get("status") != "done":
            return render_template("processing.html", title=APP_TITLE, job_id=job_id)
        return render_template(
            "result.html",
            title=APP_TITLE,
            out_file=job.get("out_file", ""),
            elapsed_display=job.get("elapsed_display", ""),
            repeat_url=job.get("repeat_url", "") or url_for("home"),
        )

    @app.get("/download/<filename>")
    def download(filename: str):
        if not _current_user_can_download(filename):
            flash("No tienes permisos para descargar ese archivo.")
            return redirect(url_for("home"))
        path = (OUTPUT_DIR / filename).resolve()
        try:
            output_root = OUTPUT_DIR.resolve()
        except Exception:
            output_root = OUTPUT_DIR
        if output_root not in path.parents and path != output_root:
            flash("Ruta de descarga invalida.")
            return redirect(url_for("home"))
        if not path.exists():
            flash("Archivo no encontrado.")
            return redirect(url_for("home"))
        return send_file(path, as_attachment=True)

