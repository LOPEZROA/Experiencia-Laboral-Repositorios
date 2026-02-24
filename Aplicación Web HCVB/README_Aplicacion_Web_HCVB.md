# Aplicación Web HCVB

Aplicación web desarrollada en **Flask** para procesamiento de datos en contexto operativo: carga de archivos, generación de salidas Excel y módulos de **estadísticas/cálculo de mediana** y **cruces** con bases de referencia.

![Flask](https://img.shields.io/badge/Flask-Web%20App-black?style=for-the-badge&logo=flask&logoColor=white)
![Excel](https://img.shields.io/badge/Excel-openpyxl%20%2B%20pandas-1D6F42?style=for-the-badge&logo=microsoft-excel&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-psycopg2-316192?style=for-the-badge&logo=postgresql&logoColor=white)

---

## 🧩 Qué encontrarás aquí

Esta carpeta contiene **dos implementaciones**:

1. **`App WEB (Python + Excel)/`**  
   Ejecuta el flujo utilizando **archivos Excel** como bases de referencia (pandas/openpyxl).
2. **`App WEB (Python + PostgreSQL)/`**  
   Variante que consulta bases en **PostgreSQL** (y puede usar Redis para sesiones/estado).

Ambas versiones comparten un patrón de app “productiva”: UI con plantillas HTML, CSS, ejecución de procesos con progreso/resultado y descarga de archivos.

---

## ✅ Funcionalidades (alto nivel)

Dependiendo de la versión/módulo:

- **Carga y validación** de archivos de trabajo (`.xlsx` / `.xlsb`).
- **Cálculo de mediana / P75** sobre registros (con reglas de inclusión/exclusión).
- **Estadísticas** y exportación de resultados a Excel.
- **Cruces / validaciones** contra bases de referencia (p.ej., histórico, defunciones, etc.).
- Módulos UI (según templates):
  - Home / Login
  - Categorías de procesos
  - Pantalla de procesamiento con progreso (`processing.html`)
  - Resultados con descarga (`result.html`)
  - Estadísticas (`estadisticas.html`) y estadísticas de cruces (`estadisticas_cruces.html`)
  - En versión Excel: búsqueda y cruce adicional (templates `busqueda_sudais.html`, `cruce_sistema_local.html`).

---

## 🗂️ Estructura

```
Aplicación Web HCVB/
├─ App WEB (Python + Excel)/
│  ├─ app1.py
│  ├─ requirements.txt
│  ├─ static/styles.css
│  └─ templates/*.html
└─ App WEB (Python + PostgreSQL)/
   ├─ app1.py
   ├─ requirements.txt
   ├─ static/styles.css
   ├─ templates/*.html
   └─ CODIGOS MIGRACION A POSTGRESQL DESDE EXCEL/
      ├─ RAW.sql
      ├─ CORES Y INDICES.sql
      └─ Migracion Excel.py
```

---

## ⚙️ Requisitos

### Versión Python + Excel
Dependencias (ver `App WEB (Python + Excel)/requirements.txt`): Flask, pandas, openpyxl, numpy, dateutil, pyxlsb.

### Versión Python + PostgreSQL
Dependencias (ver `App WEB (Python + PostgreSQL)/requirements.txt`): Flask, Flask-Session, psycopg2, redis (opcional), pandas/openpyxl, etc.

---

## ▶️ Ejecución local

> Recomendación: crear entornos virtuales separados para cada variante (por diferencias de versiones).

### Python + Excel

```bash
cd "Aplicación Web HCVB/App WEB (Python + Excel)"
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/Mac:
source .venv/bin/activate

pip install -r requirements.txt
python app1.py
```

### Python + PostgreSQL

```bash
cd "Aplicación Web HCVB/App WEB (Python + PostgreSQL)"
python -m venv .venv
# activar venv...

pip install -r requirements.txt
python app1.py
```

---

## 🔧 Configuración PostgreSQL (sugerida)

La variante PostgreSQL requiere un servidor accesible y credenciales. Una práctica común:

- Definir variables de entorno (ejemplos):
  - `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`
  - `POSTGRES_USER`, `POSTGRES_PASSWORD`
  - (opcional) `REDIS_URL` si se usa Redis para sesión/estado

> Si vas a publicar la app: no hardcodees credenciales en el código; usa `.env`/variables de entorno y secretos del entorno.

---

## 🧪 Migración Excel → PostgreSQL

En `CODIGOS MIGRACION A POSTGRESQL DESDE EXCEL/` hay scripts SQL y Python para:
- crear estructura “raw”,
- definir índices/cores,
- migrar datos desde Excel hacia PostgreSQL.

---

## 🔒 Seguridad y datos

Este tipo de app suele trabajar con información sensible. Buenas prácticas:

- sanitizar/anonimizar archivos antes de versionar,
- controlar accesos (login/roles),
- limitar rutas de descarga a carpetas seguras,
- registrar auditoría de procesos (logs) sin exponer datos personales.

---

## Autor

**Nicolás Esteban López Roa** — GitHub: [@LOPEZROA](https://github.com/LOPEZROA)
