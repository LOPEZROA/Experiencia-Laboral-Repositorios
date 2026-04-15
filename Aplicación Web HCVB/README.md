# Lista de Espera NOGES - App MVP SQL

Aplicacion web desarrollada en Flask para analizar, cruzar y enriquecer archivos de listas de espera No GES usando fuentes almacenadas en PostgreSQL.

El proyecto permite operar flujos de trabajo de uso diario sobre nominas y bases historicas, incluyendo cruces de consistencia, estadisticas, calculo de mediana, busqueda puntual de casos y funciones administrativas para carga de datos y monitoreo de la plataforma.

## Objetivo

Centralizar en una sola aplicacion los procesos operativos mas frecuentes sobre listas de espera:

- Cruce de archivos contra historico, nominas, defunciones, CGR y establecimientos.
- Generacion de estadisticas generales y estadisticas de cruces.
- Calculo de medianas por clasificacion.
- Busqueda de casos por `RUT` e `ID_LOCAL`.
- Administracion de cargas a PostgreSQL.
- Gestion de autenticacion, recuperacion de contrasena y monitoreo operativo.

## Funcionalidades principales

### 1. Todos los cruces

Procesa archivos `.xlsx`, `.xlsb` o `.csv` y permite activar cruces como:

- Historico.
- Nominas.
- Verificacion de datos.
- Traslape.
- Duplicidad.
- CGR.
- Defunciones.
- Macrored.

El resultado se exporta como Excel enriquecido con nuevas columnas y alertas para analisis operativo.

### 2. Estadisticas generales

Genera tableros y exportables a partir de un archivo cargado, incluyendo:

- Total de registros.
- Abiertos vs cerrados.
- Distribuciones por sexo, edad y tipo de prestacion.
- Resumen por causal de salida.
- Indicadores semaforo de tiempo de espera y defunciones.

### 3. Estadisticas de cruces

Ejecuta internamente los cruces del modulo principal y construye un resumen consolidado con KPIs, tablas y vistas previas para analisis.

### 4. Busqueda sudais

Permite consultar casos por:

- `RUT`
- `ID_LOCAL`
- ambos campos a la vez

La consulta busca en historico y nominas, con opcion de exportar resultados.

### 5. Calculo de mediana

Procesa un archivo de trabajo para calcular medianas por clase:

- `IC`
- `Dental`
- `IQ`
- `PROC`

Genera un Excel con hojas de detalle, excluidos, resumen y una vista web con indicadores.

### 6. Modulo ADMIN

Incluye herramientas para:

- Actualizar tablas RAW desde archivos.
- Regenerar cores e indices cuando corresponde.
- Revisar salud de la aplicacion.
- Auditar eventos de seguridad.
- Aprobar o rechazar solicitudes de recuperacion de contrasena.
- Cancelar jobs en ejecucion.

## Arquitectura de la aplicacion

La app esta organizada en capas simples y claras:

- `app1.py`
  Punto de entrada. Crea la aplicacion, precarga bases si esta habilitado y levanta Waitress.

- `mvp_app/app_factory.py`
  Fabrica Flask. Configura sesiones, cookies, autenticacion, mantenimiento en background y registro de rutas.

- `mvp_app/apis/web.py`
  Capa HTTP. Maneja login, formularios, vistas, descargas, permisos, progreso de jobs y modulo ADMIN.

- `mvp_app/core/`
  Nucleo funcional de la app. Aqui vive la configuracion, autenticacion en PostgreSQL, motor de cruces, estadisticas, mediana, runtime de jobs y carga de datos.

- `templates/`
  Plantillas HTML renderizadas con Jinja2.

- `static/`
  Archivos estaticos, principalmente estilos CSS.

## Flujo general de trabajo

1. El usuario inicia sesion.
2. Carga un archivo o ejecuta una consulta.
3. La capa web valida permisos, CSRF y parametros.
4. Si el proceso es pesado, se crea un `job_id` y se ejecuta en background.
5. El core procesa el archivo y consulta PostgreSQL para enriquecer o validar datos.
6. La UI consulta el progreso periodicamente.
7. El usuario visualiza el resultado y descarga el archivo final si corresponde.

## Stack tecnologico

- Backend: Flask
- Procesamiento: pandas, numpy, openpyxl, pyxlsb
- Base de datos: PostgreSQL
- Sesiones y jobs: filesystem/memoria o Redis
- Servidor WSGI: Waitress
- Frontend: Jinja2 + HTML + CSS + JavaScript vanilla

## Estructura del repositorio

```text
.
|-- app1.py
|-- requirements.txt
|-- Dockerfile
|-- web.config
|-- README.md
|-- mvp_app/
|   |-- __init__.py
|   |-- app_factory.py
|   |-- apis/
|   |   |-- __init__.py
|   |   `-- web.py
|   `-- core/
|       |-- __init__.py
|       |-- auth.py
|       |-- auth_db.py
|       |-- catalog.py
|       |-- config.py
|       `-- services.py
|-- templates/
|-- static/
|-- documentacion/
|   |-- README.md
|   |-- README_APIS.md
|   |-- README_CORE.md
|   |-- README_FRONTEND.md
|   `-- README_POSTGRESQL.md
`-- CODIGOS MIGRACION/
    |-- RAW.sql
    |-- RAW_COMGES_ESPECIALES.sql
    |-- CORES Y INDICES.sql
    |-- SECURITY_AUTH.sql
    `-- SEED_AUTH_USERS.sql
```

## Estructura de datos

La aplicacion trabaja principalmente con PostgreSQL y separa responsabilidades por esquema:

- `raw`
  Recibe las fuentes originales o casi originales.

- `core`
  Contiene tablas normalizadas e indices optimizados para cruces y busquedas.

- `app`
  Maneja autenticacion, recuperacion de contrasena y auditoria.

Ademas, si Redis no esta habilitado, la app puede persistir runtime de jobs en PostgreSQL.

## Requisitos

- Python 3.8
- PostgreSQL disponible
- Redis opcional

Dependencias Python principales:

- Flask
- Flask-Session
- pandas
- numpy
- openpyxl
- pyxlsb
- psycopg2-binary
- redis
- waitress

## Instalacion local

### 1. Crear entorno virtual

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Configurar variables de entorno

El proyecto carga automaticamente variables desde un archivo `.env` en la raiz.

Ejemplo minimo:

```dotenv
APP_SECRET_KEY=tu_clave_secreta
APP_HOST=127.0.0.1
APP_PORT=5000
APP_THREADS=2
PRELOAD_ON_STARTUP=1

POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5432
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=tu_password
POSTGRES_SCHEMA=raw
AUTH_SCHEMA=app

SESSION_BACKEND=filesystem
JOBS_BACKEND=memory
# REDIS_URL=redis://127.0.0.1:6379/0
```

Variables especialmente importantes:

- `APP_SECRET_KEY`: obligatoria, no debe quedar en `change-me`.
- `POSTGRES_*`: conexion a la base principal.
- `POSTGRES_PORT`: define explicitamente el puerto real de tu instancia PostgreSQL.
- `SESSION_BACKEND`: `filesystem` o `redis`.
- `JOBS_BACKEND`: `memory` o `redis`.
- `PRELOAD_ON_STARTUP`: precarga indices de base al iniciar la app.

## Inicializacion de base de datos

Los scripts SQL del proyecto estan en [`CODIGOS MIGRACION/`](./CODIGOS%20MIGRACION).

Orden sugerido:

1. `CODIGOS MIGRACION/RAW.sql`
2. `CODIGOS MIGRACION/RAW_COMGES_ESPECIALES.sql`
3. `CODIGOS MIGRACION/SECURITY_AUTH.sql`
4. `CODIGOS MIGRACION/SEED_AUTH_USERS.sql`
5. `CODIGOS MIGRACION/CORES Y INDICES.sql`

Tablas base esperadas por la app:

- `ss06_cerradas_historicas`
- `cgr`
- `defunciones`
- `establecimientos`
- `comges_especiales`
- `nomina_ic_abiertas`
- `nomina_ic_cerradas`
- `nomina_iq_abiertas`
- `nomina_iq_cerradas`
- `nomina_proc_abiertas`
- `nomina_proc_cerradas`

## Ejecucion

```powershell
python app1.py
```

Luego abrir en navegador:

```text
http://127.0.0.1:5000
```

La app intenta usar Waitress. Si no esta disponible, cae al servidor de desarrollo de Flask.

## Despliegue

### Docker

El repositorio incluye `Dockerfile`.

```bash
docker build -t le-noges-app .
docker run --rm -p 5000:5000 --env-file .env le-noges-app
```

### IIS / reverse proxy

El archivo `web.config` permite reenviar trafico hacia una instancia local de la app, tipicamente expuesta por Waitress en `http://127.0.0.1:5000`.

## Seguridad y operacion

La aplicacion incorpora varias medidas operativas y de seguridad:

- Login obligatorio para casi toda la plataforma.
- Sesiones con expiracion por inactividad.
- Proteccion CSRF en operaciones mutantes.
- Hash de contrasenas con PBKDF2-SHA256.
- Bloqueo progresivo por intentos fallidos.
- Recuperacion de contrasena con aprobacion ADMIN.
- Descargas restringidas al propietario del job o a administradores.
- Limpieza automatica de temporales y jobs vencidos.
