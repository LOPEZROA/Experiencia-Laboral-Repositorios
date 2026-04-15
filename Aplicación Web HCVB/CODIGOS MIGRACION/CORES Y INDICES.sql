CREATE SCHEMA IF NOT EXISTS util;
CREATE SCHEMA IF NOT EXISTS core;

-- Creacion de Cores

CREATE OR REPLACE FUNCTION util.norm_run(x text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE
    WHEN x IS NULL OR trim(x) = '' THEN NULL
    ELSE COALESCE(
      NULLIF(
        regexp_replace(
          regexp_replace(trim(x), '[^0-9]', '', 'g'),
          '^0+',
          ''
        ),
        ''
      ),
      '0'
    )
  END;
$$;

CREATE OR REPLACE FUNCTION util.norm_dv(x text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT NULLIF(upper(regexp_replace(trim(coalesce(x, '')), '[^0-9Kk]', '', 'g')), '');
$$;

CREATE OR REPLACE FUNCTION util.norm_text(x text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT NULLIF(upper(regexp_replace(trim(coalesce(x,'')), '[\s\.\-]+', '', 'g')), '');
$$;

CREATE OR REPLACE FUNCTION util.norm_id(x text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT NULLIF(regexp_replace(trim(coalesce(x,'')), '\.0+$', ''), '');
$$;

CREATE OR REPLACE FUNCTION util.parse_fecha(x text)
RETURNS date LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE
    WHEN x IS NULL OR trim(x) = '' THEN NULL::date
    WHEN replace(trim(x), ',', '.') ~ '^\d+(?:\.\d+)?$'
      THEN (DATE '1899-12-30' + floor((replace(trim(x), ',', '.'))::numeric)::int)::date
    WHEN split_part(trim(x), ' ', 1) ~ '^\d{2}/\d{2}/\d{4}$'
      THEN to_date(split_part(trim(x), ' ', 1), 'DD/MM/YYYY')
    WHEN split_part(trim(x), ' ', 1) ~ '^\d{4}-\d{2}-\d{2}$'
      THEN to_date(split_part(trim(x), ' ', 1), 'YYYY-MM-DD')
    WHEN split_part(trim(x), ' ', 1) ~ '^\d{4}/\d{2}/\d{2}$'
      THEN to_date(split_part(trim(x), ' ', 1), 'YYYY/MM/DD')
    ELSE NULL
  END;
$$;

CREATE OR REPLACE FUNCTION util.excel_serial(x text)
RETURNS integer LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE
    WHEN x IS NULL OR trim(x) = '' THEN NULL
    WHEN trim(x) ~ '^\d+$' THEN trim(x)::int
    ELSE (util.parse_fecha(x) - DATE '1899-12-30')::int
  END;
$$;

CREATE OR REPLACE FUNCTION util.unico_traslape(run text, dv text, presta_min text, estab_dest text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT concat_ws('|',
    COALESCE(util.norm_run(run), ''),
    COALESCE(util.norm_dv(dv), ''),
    COALESCE(util.norm_text(presta_min), ''),
    COALESCE(util.norm_id(estab_dest), '')
  );
$$;

CREATE OR REPLACE FUNCTION util.unico_historico(
  run text, dv text, tipo_prest text, presta_min text, plano text, extremidad text, f_entrada text, estab_dest text
)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT concat_ws('|',
    COALESCE(util.norm_run(run), ''),
    COALESCE(util.norm_dv(dv), ''),
    COALESCE(util.norm_text(tipo_prest), ''),
    COALESCE(util.norm_text(presta_min), ''),
    COALESCE(util.norm_text(plano), ''),
    COALESCE(util.norm_text(extremidad), ''),
    COALESCE(util.excel_serial(f_entrada)::text,''),
    COALESCE(util.norm_id(estab_dest), '')
  );
$$;

CREATE TABLE IF NOT EXISTS core.nomina_ic (
  fuente text,
  run text,
  dv text,
  tipo_prest text,
  presta_min text,
  plano text,
  extremidad text,
  presta_norm text,
  ext_norm text,
  f_entrada date,
  f_salida date,
  estab_dest text,
  id_local text,
  sigte_id text,
  id_local_norm text,
  sigte_id_norm text,
  unico_traslape text,
  unico_historico text
);
ALTER TABLE core.nomina_ic
  ADD COLUMN IF NOT EXISTS fuente text,
  ADD COLUMN IF NOT EXISTS run text,
  ADD COLUMN IF NOT EXISTS dv text,
  ADD COLUMN IF NOT EXISTS tipo_prest text,
  ADD COLUMN IF NOT EXISTS presta_min text,
  ADD COLUMN IF NOT EXISTS plano text,
  ADD COLUMN IF NOT EXISTS extremidad text,
  ADD COLUMN IF NOT EXISTS presta_norm text,
  ADD COLUMN IF NOT EXISTS ext_norm text,
  ADD COLUMN IF NOT EXISTS f_entrada date,
  ADD COLUMN IF NOT EXISTS f_salida date,
  ADD COLUMN IF NOT EXISTS estab_dest text,
  ADD COLUMN IF NOT EXISTS id_local text,
  ADD COLUMN IF NOT EXISTS sigte_id text,
  ADD COLUMN IF NOT EXISTS id_local_norm text,
  ADD COLUMN IF NOT EXISTS sigte_id_norm text,
  ADD COLUMN IF NOT EXISTS unico_traslape text,
  ADD COLUMN IF NOT EXISTS unico_historico text;
TRUNCATE TABLE core.nomina_ic;
INSERT INTO core.nomina_ic (
  fuente, run, dv, tipo_prest, presta_min, plano, extremidad, presta_norm, ext_norm,
  f_entrada, f_salida, estab_dest, id_local, sigte_id, id_local_norm, sigte_id_norm, unico_traslape, unico_historico
)
SELECT
  'abiertas' AS fuente,
  util.norm_run(run) AS run,
  util.norm_dv(dv)   AS dv,
  tipo_prest, presta_min, plano, extremidad,
  util.norm_text(presta_min) AS presta_norm,
  util.norm_text(extremidad) AS ext_norm,
  util.parse_fecha(f_entrada) AS f_entrada,
  util.parse_fecha(f_salida)  AS f_salida,
  estab_dest,
  id_local,
  sigte_id,
  util.norm_id(id_local) AS id_local_norm,
  util.norm_id(sigte_id) AS sigte_id_norm,
  util.unico_traslape(run,dv,presta_min,estab_dest) AS unico_traslape,
  util.unico_historico(run,dv,tipo_prest,presta_min,plano,extremidad,f_entrada,estab_dest) AS unico_historico
FROM raw.nomina_ic_abiertas
UNION ALL
SELECT
  'cerradas',
  util.norm_run(run),
  util.norm_dv(dv),
  tipo_prest, presta_min, plano, extremidad,
  util.norm_text(presta_min),
  util.norm_text(extremidad),
  util.parse_fecha(f_entrada),
  util.parse_fecha(f_salida),
  estab_dest,
  id_local,
  sigte_id,
  util.norm_id(id_local),
  util.norm_id(sigte_id),
  util.unico_traslape(run,dv,presta_min,estab_dest),
  util.unico_historico(run,dv,tipo_prest,presta_min,plano,extremidad,f_entrada,estab_dest)
FROM raw.nomina_ic_cerradas;

CREATE TABLE IF NOT EXISTS core.nomina_proc (
  fuente text,
  run text,
  dv text,
  tipo_prest text,
  presta_min text,
  plano text,
  extremidad text,
  presta_norm text,
  ext_norm text,
  f_entrada date,
  f_salida date,
  estab_dest text,
  id_local text,
  sigte_id text,
  id_local_norm text,
  sigte_id_norm text,
  unico_traslape text,
  unico_historico text
);
ALTER TABLE core.nomina_proc
  ADD COLUMN IF NOT EXISTS fuente text,
  ADD COLUMN IF NOT EXISTS run text,
  ADD COLUMN IF NOT EXISTS dv text,
  ADD COLUMN IF NOT EXISTS tipo_prest text,
  ADD COLUMN IF NOT EXISTS presta_min text,
  ADD COLUMN IF NOT EXISTS plano text,
  ADD COLUMN IF NOT EXISTS extremidad text,
  ADD COLUMN IF NOT EXISTS presta_norm text,
  ADD COLUMN IF NOT EXISTS ext_norm text,
  ADD COLUMN IF NOT EXISTS f_entrada date,
  ADD COLUMN IF NOT EXISTS f_salida date,
  ADD COLUMN IF NOT EXISTS estab_dest text,
  ADD COLUMN IF NOT EXISTS id_local text,
  ADD COLUMN IF NOT EXISTS sigte_id text,
  ADD COLUMN IF NOT EXISTS id_local_norm text,
  ADD COLUMN IF NOT EXISTS sigte_id_norm text,
  ADD COLUMN IF NOT EXISTS unico_traslape text,
  ADD COLUMN IF NOT EXISTS unico_historico text;
TRUNCATE TABLE core.nomina_proc;
INSERT INTO core.nomina_proc (
  fuente, run, dv, tipo_prest, presta_min, plano, extremidad, presta_norm, ext_norm,
  f_entrada, f_salida, estab_dest, id_local, sigte_id, id_local_norm, sigte_id_norm, unico_traslape, unico_historico
)
SELECT
  'abiertas' AS fuente,
  util.norm_run(run) AS run,
  util.norm_dv(dv)   AS dv,
  tipo_prest, presta_min, plano, extremidad,
  util.norm_text(presta_min) AS presta_norm,
  util.norm_text(extremidad) AS ext_norm,
  util.parse_fecha(f_entrada) AS f_entrada,
  util.parse_fecha(f_salida)  AS f_salida,
  estab_dest,
  id_local,
  sigte_id,
  util.norm_id(id_local) AS id_local_norm,
  util.norm_id(sigte_id) AS sigte_id_norm,
  util.unico_traslape(run,dv,presta_min,estab_dest) AS unico_traslape,
  util.unico_historico(run,dv,tipo_prest,presta_min,plano,extremidad,f_entrada,estab_dest) AS unico_historico
FROM raw.nomina_proc_abiertas
UNION ALL
SELECT
  'cerradas',
  util.norm_run(run),
  util.norm_dv(dv),
  tipo_prest, presta_min, plano, extremidad,
  util.norm_text(presta_min),
  util.norm_text(extremidad),
  util.parse_fecha(f_entrada),
  util.parse_fecha(f_salida),
  estab_dest,
  id_local,
  sigte_id,
  util.norm_id(id_local),
  util.norm_id(sigte_id),
  util.unico_traslape(run,dv,presta_min,estab_dest),
  util.unico_historico(run,dv,tipo_prest,presta_min,plano,extremidad,f_entrada,estab_dest)
FROM raw.nomina_proc_cerradas;

CREATE TABLE IF NOT EXISTS core.nomina_iq (
  fuente text,
  run text,
  dv text,
  tipo_prest text,
  presta_min text,
  plano text,
  extremidad text,
  presta_norm text,
  ext_norm text,
  f_entrada date,
  f_salida date,
  estab_dest text,
  id_local text,
  sigte_id text,
  id_local_norm text,
  sigte_id_norm text,
  unico_traslape text,
  unico_historico text
);
ALTER TABLE core.nomina_iq
  ADD COLUMN IF NOT EXISTS fuente text,
  ADD COLUMN IF NOT EXISTS run text,
  ADD COLUMN IF NOT EXISTS dv text,
  ADD COLUMN IF NOT EXISTS tipo_prest text,
  ADD COLUMN IF NOT EXISTS presta_min text,
  ADD COLUMN IF NOT EXISTS plano text,
  ADD COLUMN IF NOT EXISTS extremidad text,
  ADD COLUMN IF NOT EXISTS presta_norm text,
  ADD COLUMN IF NOT EXISTS ext_norm text,
  ADD COLUMN IF NOT EXISTS f_entrada date,
  ADD COLUMN IF NOT EXISTS f_salida date,
  ADD COLUMN IF NOT EXISTS estab_dest text,
  ADD COLUMN IF NOT EXISTS id_local text,
  ADD COLUMN IF NOT EXISTS sigte_id text,
  ADD COLUMN IF NOT EXISTS id_local_norm text,
  ADD COLUMN IF NOT EXISTS sigte_id_norm text,
  ADD COLUMN IF NOT EXISTS unico_traslape text,
  ADD COLUMN IF NOT EXISTS unico_historico text;
TRUNCATE TABLE core.nomina_iq;
INSERT INTO core.nomina_iq (
  fuente, run, dv, tipo_prest, presta_min, plano, extremidad, presta_norm, ext_norm,
  f_entrada, f_salida, estab_dest, id_local, sigte_id, id_local_norm, sigte_id_norm, unico_traslape, unico_historico
)
SELECT
  'abiertas' AS fuente,
  util.norm_run(run) AS run,
  util.norm_dv(dv)   AS dv,
  tipo_prest, presta_min, plano, extremidad,
  util.norm_text(presta_min) AS presta_norm,
  util.norm_text(extremidad) AS ext_norm,
  util.parse_fecha(f_entrada) AS f_entrada,
  util.parse_fecha(f_salida)  AS f_salida,
  estab_dest,
  id_local,
  sigte_id,
  util.norm_id(id_local) AS id_local_norm,
  util.norm_id(sigte_id) AS sigte_id_norm,
  util.unico_traslape(run,dv,presta_min,estab_dest) AS unico_traslape,
  util.unico_historico(run,dv,tipo_prest,presta_min,plano,extremidad,f_entrada,estab_dest) AS unico_historico
FROM raw.nomina_iq_abiertas
UNION ALL
SELECT
  'cerradas',
  util.norm_run(run),
  util.norm_dv(dv),
  tipo_prest, presta_min, plano, extremidad,
  util.norm_text(presta_min),
  util.norm_text(extremidad),
  util.parse_fecha(f_entrada),
  util.parse_fecha(f_salida),
  estab_dest,
  id_local,
  sigte_id,
  util.norm_id(id_local),
  util.norm_id(sigte_id),
  util.unico_traslape(run,dv,presta_min,estab_dest),
  util.unico_historico(run,dv,tipo_prest,presta_min,plano,extremidad,f_entrada,estab_dest)
FROM raw.nomina_iq_cerradas;

CREATE TABLE IF NOT EXISTS core.historico (
  run text,
  dv text,
  tipo_prest text,
  presta_min text,
  plano text,
  extremidad text,
  presta_norm text,
  ext_norm text,
  f_entrada date,
  f_salida date,
  estab_dest text,
  id_local text,
  sigte_id text,
  id_local_norm text,
  sigte_id_norm text,
  unico_historico text
);
ALTER TABLE core.historico
  ADD COLUMN IF NOT EXISTS run text,
  ADD COLUMN IF NOT EXISTS dv text,
  ADD COLUMN IF NOT EXISTS tipo_prest text,
  ADD COLUMN IF NOT EXISTS presta_min text,
  ADD COLUMN IF NOT EXISTS plano text,
  ADD COLUMN IF NOT EXISTS extremidad text,
  ADD COLUMN IF NOT EXISTS presta_norm text,
  ADD COLUMN IF NOT EXISTS ext_norm text,
  ADD COLUMN IF NOT EXISTS f_entrada date,
  ADD COLUMN IF NOT EXISTS f_salida date,
  ADD COLUMN IF NOT EXISTS estab_dest text,
  ADD COLUMN IF NOT EXISTS id_local text,
  ADD COLUMN IF NOT EXISTS sigte_id text,
  ADD COLUMN IF NOT EXISTS id_local_norm text,
  ADD COLUMN IF NOT EXISTS sigte_id_norm text,
  ADD COLUMN IF NOT EXISTS unico_historico text;
TRUNCATE TABLE core.historico;
INSERT INTO core.historico (
  run, dv, tipo_prest, presta_min, plano, extremidad, presta_norm, ext_norm,
  f_entrada, f_salida, estab_dest, id_local, sigte_id, id_local_norm, sigte_id_norm, unico_historico
)
SELECT
  util.norm_run(run) AS run,
  util.norm_dv(dv)   AS dv,
  tipo_prest, presta_min, plano, extremidad,
  util.norm_text(presta_min) AS presta_norm,
  util.norm_text(extremidad) AS ext_norm,
  util.parse_fecha(f_entrada) AS f_entrada,
  util.parse_fecha(f_salida)  AS f_salida,
  estab_dest,
  id_local,
  sigte_id,
  util.norm_id(id_local) AS id_local_norm,
  util.norm_id(sigte_id) AS sigte_id_norm,
  util.unico_historico(run,dv,tipo_prest,presta_min,plano,extremidad,f_entrada,estab_dest) AS unico_historico
FROM raw.ss06_cerradas_historicas;

-- Creacion de Indices
-- (Cruces veloces)

CREATE INDEX IF NOT EXISTS idx_ic_unico_traslape  ON core.nomina_ic   (unico_traslape);
CREATE INDEX IF NOT EXISTS idx_ic_unico_historico ON core.nomina_ic   (unico_historico);
CREATE INDEX IF NOT EXISTS idx_proc_unico_traslape  ON core.nomina_proc(unico_traslape);
CREATE INDEX IF NOT EXISTS idx_proc_unico_historico ON core.nomina_proc(unico_historico);
CREATE INDEX IF NOT EXISTS idx_iq_unico_traslape  ON core.nomina_iq   (unico_traslape);
CREATE INDEX IF NOT EXISTS idx_iq_unico_historico ON core.nomina_iq   (unico_historico);
CREATE INDEX IF NOT EXISTS idx_hist_unico_historico ON core.historico (unico_historico);

-- Limpieza de indices legacy redundantes (si existen)
DROP INDEX IF EXISTS core.idx_ic_run_dv_presta_estab;
DROP INDEX IF EXISTS core.idx_proc_run_dv_presta_estab;
DROP INDEX IF EXISTS core.idx_iq_run_dv_presta_estab;
DROP INDEX IF EXISTS core.idx_hist_run_dv_presta_estab;
DROP INDEX IF EXISTS core.idx_ic_sigte_id;
DROP INDEX IF EXISTS core.idx_proc_sigte_id;
DROP INDEX IF EXISTS core.idx_iq_sigte_id;
DROP INDEX IF EXISTS core.idx_hist_sigte_id;

-- Indices optimizados para cruces SQL en columnas normalizadas
CREATE INDEX IF NOT EXISTS idx_ic_run_dv_prestan_estab   ON core.nomina_ic   (run, dv, presta_norm, estab_dest);
CREATE INDEX IF NOT EXISTS idx_proc_run_dv_prestan_estab ON core.nomina_proc (run, dv, presta_norm, estab_dest);
CREATE INDEX IF NOT EXISTS idx_iq_run_dv_prestan_estab   ON core.nomina_iq   (run, dv, presta_norm, estab_dest);
CREATE INDEX IF NOT EXISTS idx_hist_run_dv_prestan_estab ON core.historico   (run, dv, presta_norm, estab_dest);

CREATE INDEX IF NOT EXISTS idx_ic_run_dv_prestan_fechas   ON core.nomina_ic   (run, dv, presta_norm, f_entrada, f_salida);
CREATE INDEX IF NOT EXISTS idx_proc_run_dv_prestan_fechas ON core.nomina_proc (run, dv, presta_norm, f_entrada, f_salida);
CREATE INDEX IF NOT EXISTS idx_iq_run_dv_prestan_fechas   ON core.nomina_iq   (run, dv, presta_norm, f_entrada, f_salida);
CREATE INDEX IF NOT EXISTS idx_hist_run_dv_prestan_fechas ON core.historico   (run, dv, presta_norm, f_entrada, f_salida);

CREATE INDEX IF NOT EXISTS idx_ic_id_local_norm   ON core.nomina_ic   (id_local_norm);
CREATE INDEX IF NOT EXISTS idx_proc_id_local_norm ON core.nomina_proc (id_local_norm);
CREATE INDEX IF NOT EXISTS idx_iq_id_local_norm   ON core.nomina_iq   (id_local_norm);
CREATE INDEX IF NOT EXISTS idx_hist_id_local_norm ON core.historico   (id_local_norm);

CREATE INDEX IF NOT EXISTS idx_ic_sigte_id_norm   ON core.nomina_ic   (sigte_id_norm);
CREATE INDEX IF NOT EXISTS idx_proc_sigte_id_norm ON core.nomina_proc (sigte_id_norm);
CREATE INDEX IF NOT EXISTS idx_iq_sigte_id_norm   ON core.nomina_iq   (sigte_id_norm);
CREATE INDEX IF NOT EXISTS idx_hist_sigte_id_norm ON core.historico   (sigte_id_norm);

CREATE INDEX IF NOT EXISTS idx_ic_run_dv_prestan_ext   ON core.nomina_ic   (run, dv, presta_norm, ext_norm);
CREATE INDEX IF NOT EXISTS idx_proc_run_dv_prestan_ext ON core.nomina_proc (run, dv, presta_norm, ext_norm);
CREATE INDEX IF NOT EXISTS idx_iq_run_dv_prestan_ext   ON core.nomina_iq   (run, dv, presta_norm, ext_norm);
CREATE INDEX IF NOT EXISTS idx_hist_run_dv_prestan_ext ON core.historico   (run, dv, presta_norm, ext_norm);

ANALYZE core.nomina_ic;
ANALYZE core.nomina_proc;
ANALYZE core.nomina_iq;
ANALYZE core.historico;
