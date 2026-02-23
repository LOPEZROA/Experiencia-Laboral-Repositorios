# ---- Imports ----
import re, unicodedata, warnings
import numpy as np, pandas as pd, geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
from scipy.spatial.distance import pdist, squareform
warnings.filterwarnings("ignore")

# ================== TUNING K-MEANS (K óptimo) + GAP STATISTIC ==================

# ---- Config ----
FILE_PATH      = '/content/drive/MyDrive/HPM/Bases de trabajo/BASE FINAL.xlsx'
SHEET          = 0
PROVINCIAS_SHP = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Provincias/Provincias.shp'
COMUNAS_SHP    = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Comunas/comunas.shp'
TARGET_PROV    = 'LLANQUIHUE'
K_RANGE        = range(2, 11)
RANDOM_STATE   = 42
B_REF          = 20   # <-- nº de réplicas de referencia para el Gap Statistic

# ---- Helpers ----
EXPECTED_ALL = [
    "Sexo (Desc)", "lat", "lng", "tiempo (minutos)", "km", "Numero de registros",
    "Mayor", "Menor", "Moderada", "Ultima Edad Registrada", "Total_Amputaciones",
    'AMP_DEDO_MANO','AMP_PULGAR','AMP_DEDO_PIE','AMP_A_NIVEL_PIE','DESART_TOBILLO',
    'AMP_NIVEL_MALEOLO','AMP_DEBAJO_RODILLA','DESART_RODILLA','AMP_ENCIMA_RODILLA'
]
CLUSTER_VARS = [
    "Numero de registros", "Ultima Edad Registrada",
    "Total_Amputaciones"
    ,'AMP_DEDO_MANO','AMP_PULGAR','AMP_DEDO_PIE',
    'AMP_A_NIVEL_PIE','DESART_TOBILLO','AMP_NIVEL_MALEOLO',
    'AMP_DEBAJO_RODILLA','DESART_RODILLA','AMP_ENCIMA_RODILLA'
]

def canon(s):
    s = '' if s is None else str(s)
    s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c)!='Mn')
    s = re.sub(r'[^0-9a-zA-Z\s]', ' ', s).lower()
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def normalize_col(s):
    t = ''.join(c for c in unicodedata.normalize('NFD', str(s))
                if unicodedata.category(c)!='Mn').lower()
    return re.sub(r'[^0-9a-z]', '', t)

def map_expected_columns(df, expected_list):
    norm_actual = {normalize_col(c): c for c in df.columns}
    mapping    = {name: norm_actual.get(normalize_col(name)) for name in expected_list}
    miss       = [k for k,v in mapping.items() if v is None]
    if miss:
        print("Aviso: no se encontraron columnas ->", miss)
    return mapping

def detectar_col_provincia(columns):
    for c in ['PROVINCIA','Provincia','provincia','NOM_PROV','NOM_PROVIN',
              'NOM_PROVINCIA','PROV_NOM','PROVINC','PROV_NAME','NAME_2']:
        if c in columns:
            return c
    for c in columns:
        if 'prov' in c.lower():
            return c
    return None

def read_gdf_safe(path):
    """Intenta con pyogrio (rápido) y cae a backend por defecto si falla."""
    try:
        return gpd.read_file(path, engine="pyogrio")
    except Exception:
        return gpd.read_file(path)

# -------- NUEVO FILTRO ROBUSTO (incluye borde + fallback por cercanía) --------
def filtrar_llanquihue(df, target_prov=None, tol_m=250):
    """
    Retorna GeoDataFrame de puntos ubicados en la provincia objetivo.
    - Incluye puntos en el borde (predicate='intersects').
    - Fallback: asigna por cercanía con sjoin_nearest hasta tol_m metros
      (y respaldo con buffer si sjoin_nearest no está disponible).
    Mantiene misma firma que tu función original: retorna solo gdf_ll (EPSG:4326).
    """
    if target_prov is None:
        target_prov = TARGET_PROV

    # Validación y armado de puntos
    if 'lat' not in df.columns or 'lng' not in df.columns:
        raise ValueError("Faltan 'lat'/'lng'.")
    df = df.copy()
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lng'] = pd.to_numeric(df['lng'], errors='coerce')

    m_ok = df['lat'].notna() & df['lng'].notna()
    gdf_pts = gpd.GeoDataFrame(
        df.loc[m_ok].copy(),
        geometry=gpd.points_from_xy(df.loc[m_ok, 'lng'], df.loc[m_ok, 'lat']),
        crs='EPSG:4326'
    )

    # Provincias (reparar geometrías)
    prov = read_gdf_safe(PROVINCIAS_SHP)
    prov = prov.to_crs('EPSG:4326') if prov.crs else prov.set_crs('EPSG:4326')
    colp = detectar_col_provincia(prov.columns)
    if colp is None:
        raise ValueError("No se identificó columna de provincia en shapefile.")
    try:
        prov['geometry'] = prov.geometry.buffer(0)
    except Exception:
        pass

    # Join principal (intersects incluye borde)
    try:
        j1 = gpd.sjoin(gdf_pts, prov[[colp,'geometry']], how='left', predicate='intersects')
    except TypeError:
        j1 = gpd.sjoin(gdf_pts, prov[[colp,'geometry']], how='left', op='intersects')

    # Fallback por cercanía en metros para no asignados
    na_idx = j1[colp].isna()
    if na_idx.any():
        pts_m  = j1.to_crs(3857)
        prov_m = prov.to_crs(3857)
        try:
            jnear = gpd.sjoin_nearest(
                pts_m.loc[na_idx, ['geometry']],
                prov_m[[colp, 'geometry']],
                how='left',
                max_distance=tol_m,
                distance_col='_dist_m'
            )
            j1.loc[na_idx, colp] = jnear[colp].values
        except Exception:
            # Respaldo con buffer si sjoin_nearest no existe
            prov_buf = prov_m.copy()
            prov_buf['geometry'] = prov_buf.buffer(tol_m)
            try:
                j2 = gpd.sjoin(pts_m.loc[na_idx, ['geometry']], prov_buf[[colp, 'geometry']],
                               how='left', predicate='intersects')
            except TypeError:
                j2 = gpd.sjoin(pts_m.loc[na_idx, ['geometry']], prov_buf[[colp, 'geometry']],
                               how='left', op='intersects')
            j2 = j2.to_crs(4326)
            j1.loc[na_idx, colp] = j2[colp].values

    # Filtra provincia objetivo (normalizando)
    norm = lambda s: canon(s).upper()
    j1['__prov_norm__'] = j1[colp].map(norm)
    gdf_ll = j1[j1['__prov_norm__'] == norm(target_prov)].drop(columns=['__prov_norm__', 'index_right'], errors='ignore')

    if gdf_ll.empty:
        raise ValueError(f"No hay puntos en '{target_prov}' (tol={tol_m}m).")

    return gdf_ll.to_crs('EPSG:4326')
# -----------------------------------------------------------------------------

def dunn_index(X, labels):
    labs = np.asarray(labels)
    uniq = np.unique(labs)
    if len(uniq) < 2:
        return np.nan
    D = squareform(pdist(X, metric='euclidean'))
    diameters, idx_by_label = [], {}
    for l in uniq:
        idx = np.where(labs == l)[0]; idx_by_label[l] = idx
        diam = 0.0 if len(idx) <= 1 else D[np.ix_(idx, idx)].max()
        diameters.append(diam)
    max_diam = np.max(diameters)
    min_inter = np.inf
    for i, li in enumerate(uniq[:-1]):
        Ii = idx_by_label[li]
        for lj in uniq[i+1:]:
            Ij = idx_by_label[lj]
            if Ii.size==0 or Ij.size==0: continue
            cross = D[np.ix_(Ii, Ij)]
            if cross.size > 0: min_inter = min(min_inter, cross.min())
    if not np.isfinite(min_inter): return np.nan
    if max_diam == 0: return np.inf
    return float(min_inter / max_diam)

def eval_internal(X, labels):
    if len(np.unique(labels)) < 2:
        return np.nan, np.nan, np.nan, np.nan
    sil = silhouette_score(X, labels, metric='euclidean')
    dunn = dunn_index(X, labels)
    dbi = davies_bouldin_score(X, labels)
    ch  = calinski_harabasz_score(X, labels)
    return sil, dunn, dbi, ch

# ---------------- GAP STATISTIC ----------------
def _uniform_reference_like(X, n, rng):
    """Muestra uniforme por columna en [min, max] del X dado."""
    X = np.asarray(X)
    mins = X.min(axis=0)
    maxs = X.max(axis=0)
    spans = np.where(maxs > mins, maxs - mins, 1e-9)  # evitar span 0
    return mins + rng.random((n, X.shape[1])) * spans

def gap_statistic(X, k_range, B=20, random_state=None):
    """
    Implementa Tibshirani et al. (2001):
      Gap(k) = E_ref[log(W_k)] - log(W_k)
    donde W_k = inercia (SSE) de k-means.
    Retorna DataFrame con columnas: K, gap, elogW_ref, logW_data, s_k.
    """
    rng = np.random.default_rng(random_state)
    n, d = X.shape
    out_rows = []

    # Precalcular log(W_k) en los datos reales
    logW_data = {}
    for k in k_range:
        km = KMeans(n_clusters=k, n_init=10, random_state=random_state)
        km.fit(X)
        logW_data[k] = np.log(km.inertia_)

    # Réplicas de referencia
    for k in k_range:
        ref_logs = []
        for b in range(B):
            X_ref = _uniform_reference_like(X, n, rng)
            km = KMeans(n_clusters=k, n_init=10, random_state=random_state)
            km.fit(X_ref)
            ref_logs.append(np.log(km.inertia_))
        ref_logs = np.asarray(ref_logs)
        elog = ref_logs.mean()
        s    = ref_logs.std(ddof=1) * np.sqrt(1 + 1.0/B)  # s_k ajustado

        gap = elog - logW_data[k]
        out_rows.append({
            "K": k,
            "gap": gap,
            "elogW_ref": elog,
            "logW_data": logW_data[k],
            "s_k": s
        })

    gdf = pd.DataFrame(out_rows)

    # Criterio de una desviación (elige primer k que satisface la desigualdad)
    k_list = list(k_range)
    k_star = None
    for i in range(len(k_list) - 1):
        k     = k_list[i]
        knext = k_list[i+1]
        if gdf.loc[gdf.K==k, "gap"].values[0] >= gdf.loc[gdf.K==knext, "gap"].values[0] - gdf.loc[gdf.K==knext, "s_k"].values[0]:
            k_star = k
            break
    if k_star is None:
        k_star = gdf.loc[gdf["gap"].idxmax(), "K"]

    return gdf.sort_values("K").reset_index(drop=True), int(k_star)
# ------------------------------------------------

# ---- Load + filtro espacial ----
df_raw = pd.read_excel(FILE_PATH, sheet_name=SHEET)
colmap = map_expected_columns(df_raw, EXPECTED_ALL)
df_std = df_raw.rename(columns={colmap[k]: k for k in EXPECTED_ALL if colmap.get(k)})
gdf_ll = filtrar_llanquihue(df_std, target_prov=TARGET_PROV, tol_m=250)

# ---- Features y preprocesamiento ----
present = [v for v in CLUSTER_VARS if v in gdf_ll.columns]
if len(present) < 3:
    raise ValueError(f"Variables insuficientes para clusterizar: {present}")
df_feat = pd.DataFrame(gdf_ll.drop(columns='geometry'))[present].copy()

cont_cols = [c for c in ["Numero de registros","Ultima Edad Registrada"] if c in df_feat.columns]
bin_cols  = [c for c in ["Mayor","Menor","Moderada","Total_Amputaciones"] if c in df_feat.columns]

ct = ColumnTransformer([
    ('cont', Pipeline([
        ('imp', SimpleImputer(strategy='median')),
        ('sc',  StandardScaler())
    ]), cont_cols),
    ('bin',  Pipeline([
        ('imp', SimpleImputer(strategy='most_frequent'))
    ]), bin_cols),
], remainder='drop')

X = ct.fit_transform(df_feat)

# ---- Búsqueda de K con SSE y métricas internas ----
rows = []
for K in K_RANGE:
    km = KMeans(n_clusters=K, n_init=10, random_state=RANDOM_STATE)
    labels = km.fit_predict(X)
    sil, dunn, dbi, ch = eval_internal(X, labels)
    sse = km.inertia_
    rows.append({'K': K,'sse': sse,'silhouette': sil,'dunn': dunn,
                 'davies_bouldin': dbi,'calinski_harabasz': ch})
res = pd.DataFrame(rows).sort_values("K").reset_index(drop=True)

# ---- Gap Statistic ----
gap_df, k_gap = gap_statistic(X, K_RANGE, B=B_REF, random_state=RANDOM_STATE)
res = res.merge(gap_df[["K","gap","s_k","elogW_ref","logW_data"]], on="K", how="left")

print("\n=== TUNING K-MEANS ===")
print("Tabla de métricas por K (incluye Gap):")
with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    print(res.to_string(index=False, float_format=lambda x: f"{x:.4f}"))

# Selecciones recomendadas
best_sil = res.loc[res["silhouette"].idxmax(), "K"]
best_sse = res.loc[res["sse"].idxmin(), "K"]
print(f"\n>> K recomendado (criterio silhouette): {int(best_sil)}")
print(f">> K recomendado (criterio Gap, 1-s.k): {int(k_gap)}")
print(f">> K con mínima SSE (informativo): {int(best_sse)}")

# ---- Gráficos ----
plt.figure(figsize=(8, 4))
plt.plot(res['K'], res['sse'], marker='o', linestyle='-')
plt.xlabel('K'); plt.ylabel('SSE'); plt.title('K-Means: Curva del codo (SSE)'); plt.grid(True); plt.show()

plt.figure(figsize=(8, 4))
plt.plot(res['K'], res['silhouette'], marker='o', linestyle='-')
plt.xlabel('K'); plt.ylabel('Coeficiente de Silhouette'); plt.title('K-Means: Coeficiente de Silhouette'); plt.grid(True); plt.show()

plt.figure(figsize=(8, 4))
plt.errorbar(res['K'], res['gap'], yerr=res['s_k'], fmt='-o')
plt.xlabel('K'); plt.ylabel('Gap'); plt.title('K-Means: Estadística de brecha'); plt.grid(True); plt.show()
