CREATE TABLE IF NOT EXISTS raw.comges_especiales (
  sigte_id   text,
  run        text,
  dv         text,
  sexo       text,
  estab_dest text,
  f_entrada  text,
  f_salida   text,
  c_salida   text
);

-- Indices en RAW para cargas/consultas directas
CREATE INDEX IF NOT EXISTS idx_raw_comges_especiales_sigte_id ON raw.comges_especiales (sigte_id);
CREATE INDEX IF NOT EXISTS idx_raw_comges_especiales_run_dv   ON raw.comges_especiales (run, dv);

-- Core normalizado para cruces por SIGTE_ID
CREATE TABLE IF NOT EXISTS core.comges_especiales (
  sigte_id_norm   text PRIMARY KEY,
  sigte_id        text,
  run_norm        text,
  run             text,
  dv_norm         text,
  dv              text,
  sexo            text,
  estab_dest_norm text,
  estab_dest      text,
  f_entrada       date,
  f_salida        date,
  c_salida        text
);

TRUNCATE TABLE core.comges_especiales;

WITH staged AS (
  SELECT
    NULLIF(regexp_replace(trim(COALESCE(sigte_id, '')), '\.0+$', ''), '') AS sigte_id_norm,
    trim(COALESCE(sigte_id, '')) AS sigte_id,
    COALESCE(
      NULLIF(
        regexp_replace(
          regexp_replace(trim(COALESCE(run, '')), '[^0-9]', '', 'g'),
          '^0+',
          ''
        ),
        ''
      ),
      '0'
    ) AS run_norm,
    trim(COALESCE(run, '')) AS run,
    NULLIF(upper(regexp_replace(trim(COALESCE(dv, '')), '[^0-9Kk]', '', 'g')), '') AS dv_norm,
    trim(COALESCE(dv, '')) AS dv,
    trim(COALESCE(sexo, '')) AS sexo,
    NULLIF(regexp_replace(trim(COALESCE(estab_dest, '')), '\.0+$', ''), '') AS estab_dest_norm,
    trim(COALESCE(estab_dest, '')) AS estab_dest,
    CASE
      WHEN trim(COALESCE(f_entrada, '')) = '' THEN NULL::date
      WHEN replace(trim(COALESCE(f_entrada, '')), ',', '.') ~ '^\d+(?:\.\d+)?$'
        THEN (DATE '1899-12-30' + floor((replace(trim(COALESCE(f_entrada, '')), ',', '.'))::numeric)::int)::date
      WHEN split_part(trim(COALESCE(f_entrada, '')), ' ', 1) ~ '^\d{2}/\d{2}/\d{4}$'
        THEN to_date(split_part(trim(COALESCE(f_entrada, '')), ' ', 1), 'DD/MM/YYYY')
      WHEN split_part(trim(COALESCE(f_entrada, '')), ' ', 1) ~ '^\d{4}-\d{2}-\d{2}$'
        THEN to_date(split_part(trim(COALESCE(f_entrada, '')), ' ', 1), 'YYYY-MM-DD')
      WHEN split_part(trim(COALESCE(f_entrada, '')), ' ', 1) ~ '^\d{4}/\d{2}/\d{2}$'
        THEN to_date(split_part(trim(COALESCE(f_entrada, '')), ' ', 1), 'YYYY/MM/DD')
      ELSE NULL::date
    END AS f_entrada,
    CASE
      WHEN trim(COALESCE(f_salida, '')) = '' THEN NULL::date
      WHEN replace(trim(COALESCE(f_salida, '')), ',', '.') ~ '^\d+(?:\.\d+)?$'
        THEN (DATE '1899-12-30' + floor((replace(trim(COALESCE(f_salida, '')), ',', '.'))::numeric)::int)::date
      WHEN split_part(trim(COALESCE(f_salida, '')), ' ', 1) ~ '^\d{2}/\d{2}/\d{4}$'
        THEN to_date(split_part(trim(COALESCE(f_salida, '')), ' ', 1), 'DD/MM/YYYY')
      WHEN split_part(trim(COALESCE(f_salida, '')), ' ', 1) ~ '^\d{4}-\d{2}-\d{2}$'
        THEN to_date(split_part(trim(COALESCE(f_salida, '')), ' ', 1), 'YYYY-MM-DD')
      WHEN split_part(trim(COALESCE(f_salida, '')), ' ', 1) ~ '^\d{4}/\d{2}/\d{2}$'
        THEN to_date(split_part(trim(COALESCE(f_salida, '')), ' ', 1), 'YYYY/MM/DD')
      ELSE NULL::date
    END AS f_salida,
    trim(COALESCE(c_salida, '')) AS c_salida
  FROM raw.comges_especiales
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY sigte_id_norm
      ORDER BY f_entrada DESC NULLS LAST, f_salida DESC NULLS LAST
    ) AS rn
  FROM staged
  WHERE sigte_id_norm IS NOT NULL
)
INSERT INTO core.comges_especiales (
  sigte_id_norm,
  sigte_id,
  run_norm,
  run,
  dv_norm,
  dv,
  sexo,
  estab_dest_norm,
  estab_dest,
  f_entrada,
  f_salida,
  c_salida
)
SELECT
  sigte_id_norm,
  sigte_id,
  run_norm,
  run,
  dv_norm,
  dv,
  sexo,
  estab_dest_norm,
  estab_dest,
  f_entrada,
  f_salida,
  c_salida
FROM ranked
WHERE rn = 1;

CREATE INDEX IF NOT EXISTS idx_core_comges_especiales_run_dv ON core.comges_especiales (run_norm, dv_norm);
CREATE INDEX IF NOT EXISTS idx_core_comges_especiales_estab  ON core.comges_especiales (estab_dest_norm);
