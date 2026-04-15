from datetime import timedelta

from flask import Flask

from .apis.web import register_web_routes
from .core import auth_db
from .core import services as core
from .core.config import (
    APP_SECRET_KEY,
    ROOT,
    SESSION_BACKEND,
    SESSION_COOKIE_HTTPONLY,
    SESSION_COOKIE_SAMESITE,
    SESSION_COOKIE_SECURE,
)


def create_app() -> Flask:
    core.validate_runtime_backends()
    core.ensure_dirs()
    secret_key = str(APP_SECRET_KEY or "").strip()
    if (not secret_key) or (secret_key == "change-me"):
        raise RuntimeError("APP_SECRET_KEY invalida. Configura APP_SECRET_KEY en .env con un valor propio.")
    app = Flask(
        __name__,
        static_folder=str(ROOT / "static"),
        template_folder=str(ROOT / "templates"),
    )
    app.secret_key = secret_key
    app.config["SECRET_KEY"] = app.secret_key
    app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(minutes=30)
    app.config["SESSION_REFRESH_EACH_REQUEST"] = True
    app.config["SESSION_PERMANENT"] = True
    app.config["SESSION_USE_SIGNER"] = True
    app.config["SESSION_COOKIE_HTTPONLY"] = bool(SESSION_COOKIE_HTTPONLY)
    app.config["SESSION_COOKIE_SECURE"] = bool(SESSION_COOKIE_SECURE)
    app.config["SESSION_COOKIE_SAMESITE"] = str(SESSION_COOKIE_SAMESITE or "Lax")

    if SESSION_BACKEND == "redis" and core._REDIS_CLIENT is not None and core.Session is not None:
        app.config["SESSION_TYPE"] = "redis"
        app.config["SESSION_REDIS"] = core._REDIS_CLIENT
    else:
        app.config["SESSION_TYPE"] = "filesystem"
        session_dir = ROOT / ".flask_session"
        session_dir.mkdir(parents=True, exist_ok=True)
        app.config["SESSION_FILE_DIR"] = str(session_dir)
        app.config["SESSION_FILE_THRESHOLD"] = 2000

    if core.Session is not None:
        core.Session(app)

    auth_db.init_auth_runtime()
    core.start_background_maintenance()
    register_web_routes(app)
    return app
