import os
import time

from mvp_app import create_app
from mvp_app.core.services import get_db

try:
    from waitress import serve as waitress_serve
except Exception:
    waitress_serve = None

APP_HOST = str(os.getenv("APP_HOST", "127.0.0.1") or "127.0.0.1").strip()
APP_PORT = int(str(os.getenv("APP_PORT", "5000") or "5000").strip())
APP_THREADS = max(1, int(str(os.getenv("APP_THREADS", "2") or "2").strip()))
PRELOAD_ON_STARTUP = str(os.getenv("PRELOAD_ON_STARTUP", "1") or "1").strip().lower() in {
    "1", "true", "yes", "on"
}

app = create_app()


if __name__ == "__main__":
    if PRELOAD_ON_STARTUP:
        t0 = time.perf_counter()
        print("Precargando bases al iniciar...")
        try:
            get_db()
            elapsed = round(time.perf_counter() - t0, 2)
            print(f"Bases precargadas correctamente en {elapsed}s.")
        except Exception as e:
            print(f"Advertencia: no se pudo precargar bases: {e}")
    else:
        print("Precarga de bases desactivada (PRELOAD_ON_STARTUP=0).")

    if waitress_serve is not None:
        print(f"Iniciando servidor Waitress en http://{APP_HOST}:{APP_PORT} (threads={APP_THREADS})...")
        waitress_serve(app, host=APP_HOST, port=APP_PORT, threads=APP_THREADS)
    else:
        print("Advertencia: Waitress no disponible, se usara servidor de desarrollo Flask.")
        print(f"Servidor Flask iniciado en http://{APP_HOST}:{APP_PORT}")
        app.run(host=APP_HOST, port=APP_PORT, debug=False)
