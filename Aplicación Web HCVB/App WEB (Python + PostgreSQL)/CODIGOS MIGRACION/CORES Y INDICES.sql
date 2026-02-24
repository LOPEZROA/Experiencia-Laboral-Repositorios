CREATE SCHEMA IF NOT EXISTS util;
CREATE SCHEMA IF NOT EXISTS core;

-- Creacion de Cores

CREATE OR REPLACE FUNCTION util.norm_run(x text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT NULLIF(regexp_replace(coalesce(x,''), '[^0-9]', '', 'g'), '');
$$;

CREATE OR REPLACE FUNCTION util.norm_dv(x text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT NULLIF(upper(trim(coalesce(x,''))), '');
$$;

CREATE OR REPLACE FUNCTION util.parse_fecha(x text)
RETURNS date LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE
    WHEN x IS NULL OR trim(x) = '' THEN NULL
    WHEN trim(x) ~ '^\d+$' THEN (DATE '1899-12-30' + (trim(x))::int)::date
    WHEN trim(x) ~ '^\d{2}/\d{2}/\d{4}$' THEN to_date(trim(x), 'DD/MM/YYYY')
    WHEN trim(x) ~ '^\d{4}-\d{2}-\d{2}$' THEN (trim(x))::date
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
    util.norm_run(run),
    util.norm_dv(dv),
    NULLIF(trim(coalesce(presta_min,'')), ''),
    NULLIF(trim(coalesce(estab_dest,'')), '')
  );
$$;

CREATE OR REPLACE FUNCTION util.unico_historico(
  run text, dv text, tipo_prest text, presta_min text, plano text, extremidad text, f_entrada text, estab_dest text
)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT concat_ws('|',
    util.norm_run(run),
    util.norm_dv(dv),
    NULLIF(trim(coalesce(tipo_prest,'')), ''),
    NULLIF(trim(coalesce(presta_min,'')), ''),
    NULLIF(trim(coalesce(plano,'')), ''),
    NULLIF(trim(coalesce(extremidad,'')), ''),
    COALESCE(util.excel_serial(f_entrada)::text,''),
    NULLIF(trim(coalesce(estab_dest,'')), '')
  );
$$;

DROP TABLE IF EXISTS core.nomina_ic;
CREATE TABLE core.nomina_ic AS
SELECT
  'abiertas' AS fuente,
  util.norm_run(run) AS run,
  util.norm_dv(dv)   AS dv,
  tipo_prest, presta_min, plano, extremidad,
  util.parse_fecha(f_entrada) AS f_entrada,
  util.parse_fecha(f_salida)  AS f_salida,
  estab_dest,
  sigte_id,
  util.unico_traslape(run,dv,presta_min,estab_dest) AS unico_traslape,
  util.unico_historico(run,dv,tipo_prest,presta_min,plano,extremidad,f_entrada,estab_dest) AS unico_historico
FROM raw.nomina_ic_abiertas
UNION ALL
SELECT
  'cerradas',
  util.norm_run(run),
  util.norm_dv(dv),
  tipo_prest, presta_min, plano, extremidad,
  util.parse_fecha(f_entrada),
  util.parse_fecha(f_salida),
  estab_dest,
  sigte_id,
  util.unico_traslape(run,dv,presta_min,estab_dest),
  util.unico_historico(run,dv,tipo_prest,presta_min,plano,extremidad,f_entrada,estab_dest)
FROM raw.nomina_ic_cerradas;

DROP TABLE IF EXISTS core.nomina_proc;
CREATE TABLE core.nomina_proc AS
SELECT
  'abiertas' AS fuente,
  util.norm_run(run) AS run,
  util.norm_dv(dv)   AS dv,
  tipo_prest, presta_min, plano, extremidad,
  util.parse_fecha(f_entrada) AS f_entrada,
  util.parse_fecha(f_salida)  AS f_salida,
  estab_dest,
  sigte_id,
  util.unico_traslape(run,dv,presta_min,estab_dest) AS unico_traslape,
  util.unico_historico(run,dv,tipo_prest,presta_min,plano,extremidad,f_entrada,estab_dest) AS unico_historico
FROM raw.nomina_proc_abiertas
UNION ALL
SELECT
  'cerradas',
  util.norm_run(run),
  util.norm_dv(dv),
  tipo_prest, presta_min, plano, extremidad,
  util.parse_fecha(f_entrada),
  util.parse_fecha(f_salida),
  estab_dest,
  sigte_id,
  util.unico_traslape(run,dv,presta_min,estab_dest),
  util.unico_historico(run,dv,tipo_prest,presta_min,plano,extremidad,f_entrada,estab_dest)
FROM raw.nomina_proc_cerradas;

DROP TABLE IF EXISTS core.nomina_iq;
CREATE TABLE core.nomina_iq AS
SELECT
  'abiertas' AS fuente,
  util.norm_run(run) AS run,
  util.norm_dv(dv)   AS dv,
  tipo_prest, presta_min, plano, extremidad,
  util.parse_fecha(f_entrada) AS f_entrada,
  util.parse_fecha(f_salida)  AS f_salida,
  estab_dest,
  sigte_id,
  util.unico_traslape(run,dv,presta_min,estab_dest) AS unico_traslape,
  util.unico_historico(run,dv,tipo_prest,presta_min,plano,extremidad,f_entrada,estab_dest) AS unico_historico
FROM raw.nomina_iq_abiertas
UNION ALL
SELECT
  'cerradas',
  util.norm_run(run),
  util.norm_dv(dv),
  tipo_prest, presta_min, plano, extremidad,
  util.parse_fecha(f_entrada),
  util.parse_fecha(f_salida),
  estab_dest,
  sigte_id,
  util.unico_traslape(run,dv,presta_min,estab_dest),
  util.unico_historico(run,dv,tipo_prest,presta_min,plano,extremidad,f_entrada,estab_dest)
FROM raw.nomina_iq_cerradas;

DROP TABLE IF EXISTS core.historico;
CREATE TABLE core.historico AS
SELECT
  util.norm_run(run) AS run,
  util.norm_dv(dv)   AS dv,
  tipo_prest, presta_min, plano, extremidad,
  util.parse_fecha(f_entrada) AS f_entrada,
  util.parse_fecha(f_salida)  AS f_salida,
  estab_dest,
  sigte_id,
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

ANALYZE core.nomina_ic;
ANALYZE core.nomina_proc;
ANALYZE core.nomina_iq;
ANALYZE core.historico;