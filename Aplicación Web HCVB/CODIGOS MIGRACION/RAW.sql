-- =========================
-- 00_create_raw.sql
-- =========================
CREATE SCHEMA IF NOT EXISTS raw;

-- 1) cgr
CREATE TABLE IF NOT EXISTS raw.cgr (
  run text, dv text, tipo_prest text, f_entrada text, f_salida text, c_salida text, id_local text, origen text
);

-- 2) defunciones
CREATE TABLE IF NOT EXISTS raw.defunciones (
  rut_dv text, nombre_limpio text, dia_nac text, mes_nac text, ano1_nac text, ano2_nac text,
  dia_def text, mes_def text, ano_def text, fecha_def text
);

-- 3) establecimientos
CREATE TABLE IF NOT EXISTS raw.establecimientos (
  codigo text, codigo_antiguo text, codigo_vigente text, codigo_madre_antiguo text, codigo_madre_nuevo text,
  codigo_region text, nombre_region text, codigo_dependencia text, nombre_dependencia text, pertenece_snss text,
  tipo_establecimiento text, ambito_funcionamiento text, nombre_oficial text, certificacion text,
  dependencia_administrativa text, nivel_atencion text, codigo_comuna text, nombre_comuna text, via text,
  numero text, direccion text, telefono text, fecha_inicio_funcionamiento text, tiene_servicio_urgencia text,
  tipo_urgencia text, clasificacion_sapu text, latitud text, longitud text, tipo_prestador text,
  estado_funcionamiento text, nivel_complejidad text, tipo_atencion text, fecha_incorporacion text
);

-- 4) ss06_cerradas_historicas
CREATE TABLE IF NOT EXISTS raw.ss06_cerradas_historicas (
  serv_salud_orig text, serv_salud_dest text, run text, dv text, nombres text, primer_apellido text, segundo_apellido text,
  fecha_nac text, sexo text, cod_prevision text, tipo_prest text, presta_min text, plano text, extremidad text, presta_est text,
  f_entrada text, estab_orig text, estab_dest text, f_salida text, c_salida text, estab_otor text, presta_min_salida text, prais text,
  region_cod text, comuna_cod text, sospecha_diag text, confir_diag text, ciudad text, cond_ruralidad text, via_direccion_cod text,
  nom_calle text, num_direccion text, resto_direccion text, fono_fijo text, fono_movil text, email text, f_citacion text,
  run_prof_sol text, dv_prof_sol text, run_prof_resol text, dv_prof_resol text, id_local text, resultado text, sigte_id text, estado text
);

-- 5-6) nomina_ic_abiertas / nomina_ic_cerradas
CREATE TABLE IF NOT EXISTS raw.nomina_ic_abiertas (
  _id text, tipo_archivo text, archivo_id text, serv_salud text, run text, dv text, nombres text, primer_apellido text, segundo_apellido text,
  fecha_nac text, sexo text, prevision text, tipo_prest text, presta_min text, plano text, extremidad text, presta_est text, f_entrada text,
  estab_orig text, estab_dest text, f_salida text, c_salida text, e_otor_at text, presta_min_salida text, prais text, region text, comuna text,
  sospecha_diag text, confir_diag text, ciudad text, cond_ruralidad text, via_direccion text, nom_calle text, num_direccion text, resto_direccion text,
  fono_fijo text, fono_movil text, email text, f_citacion text, run_prof_sol text, dv_prof_sol text, run_prof_resol text, dv_prof_resol text,
  id_local text, resultado text, sigte_id text, estado_glosa text, f_defuncion text, cierre_2025_minsal text
);

CREATE TABLE IF NOT EXISTS raw.nomina_ic_cerradas (LIKE raw.nomina_ic_abiertas INCLUDING ALL);

-- 7-8) nomina_proc_abiertas / nomina_proc_cerradas
CREATE TABLE IF NOT EXISTS raw.nomina_proc_abiertas (LIKE raw.nomina_ic_abiertas INCLUDING ALL);
CREATE TABLE IF NOT EXISTS raw.nomina_proc_cerradas (LIKE raw.nomina_ic_abiertas INCLUDING ALL);

-- 9) nomina_iq_abiertas (sin _id ni archivo_id; y trae minsal_oct)
CREATE TABLE IF NOT EXISTS raw.nomina_iq_abiertas (
  tipo_archivo text, serv_salud text, run text, dv text, nombres text, primer_apellido text, segundo_apellido text, fecha_nac text, sexo text,
  prevision text, tipo_prest text, presta_min text, plano text, extremidad text, presta_est text, f_entrada text, estab_orig text, estab_dest text,
  f_salida text, c_salida text, e_otor_at text, presta_min_salida text, prais text, region text, comuna text, sospecha_diag text, confir_diag text,
  ciudad text, cond_ruralidad text, via_direccion text, nom_calle text, num_direccion text, resto_direccion text, fono_fijo text, fono_movil text,
  email text, f_citacion text, run_prof_sol text, dv_prof_sol text, run_prof_resol text, dv_prof_resol text, id_local text, resultado text,
  sigte_id text, estado_glosa text, f_defuncion text, minsal_oct text
);

-- 10) nomina_iq_cerradas (con _id/archivo_id y minsal)
CREATE TABLE IF NOT EXISTS raw.nomina_iq_cerradas (
  _id text, tipo_archivo text, archivo_id text, serv_salud text, run text, dv text, nombres text, primer_apellido text, segundo_apellido text,
  fecha_nac text, sexo text, prevision text, tipo_prest text, presta_min text, plano text, extremidad text, presta_est text, f_entrada text,
  estab_orig text, estab_dest text, f_salida text, c_salida text, e_otor_at text, presta_min_salida text, prais text, region text, comuna text,
  sospecha_diag text, confir_diag text, ciudad text, cond_ruralidad text, via_direccion text, nom_calle text, num_direccion text, resto_direccion text,
  fono_fijo text, fono_movil text, email text, f_citacion text, run_prof_sol text, dv_prof_sol text, run_prof_resol text, dv_prof_resol text,
  id_local text, resultado text, sigte_id text, estado_glosa text, f_defuncion text, minsal text
);