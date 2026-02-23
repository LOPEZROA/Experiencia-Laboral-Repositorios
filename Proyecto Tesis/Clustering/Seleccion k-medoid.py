# ================== TUNING K-MEDOIDS (Bray–Curtis) ==================
# (opcional) instala versiones compatibles si hace falta
!pip install -q "numpy>=1.24,<2.0" "scikit-learn>=1.2,<1.5" "scikit-learn-extra>=0.3.0" geopandas shapely

# ---- Imports ----
import re, unicodedata, warnings
import numpy as np, pandas as pd, geopandas as gpd
from shapely.geometry import Point
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
from sklearn_extra.cluster import KMedoids
from scipy.spatial.distance import pdist, squareform
from sklearn.metrics import pairwise_distances
import matplotlib.pyplot as plt
warnings.filterwarnings("ignore")

# ---- Config ----
FILE_PATH      = '/content/drive/MyDrive/HPM/Bases de trabajo/BASE FINAL.xlsx'
SHEET          = 0
PROVINCIAS_SHP = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Provincias/Provincias.shp'
TARGET_PROV    = 'LLANQUIHUE'
K_RANGE        = list(range(2, 11))
RANDOM_STATE   = 42
N_REF          = 10     # réplicas para Gap Statistic
GAP_METRIC     = 'braycurtis'

# ---- Columnas esperadas ----
EXPECTED_ALL = [
    "Sexo (Desc)","lat","lng","tiempo (minutos)","km",
    "Numero de registros","Mayor","Menor","Moderada",
    "Ultima Edad Registrada","Total_Amputaciones"
]
CLUSTER_VARS = [
    "Numero de registros", "Ultima Edad Registrada",
    "Total_Amputaciones"
    ,'AMP_DEDO_MANO','AMP_PULGAR','AMP_DEDO_PIE',
    'AMP_A_NIVEL_PIE','DESART_TOBILLO','AMP_NIVEL_MALEOLO',
    'AMP_DEBAJO_RODILLA','DESART_RODILLA','AMP_ENCIMA_RODILLA'
]

# ---- Utils de nombres/columnas ----
def canon(s):
    s = '' if s is None else str(s)
    s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c)!='Mn')
    s = re.sub(r'[^0-9a-zA-Z\s]', ' ', s).lower()
    s = re.sub(r'\s+', ' ', s).strip()
    return s
def normalize_col(s):
    t = ''.join(ch for ch in unicodedata.normalize('NFD', str(s)) if unicodedata.category(ch)!='Mn').lower()
    return re.sub(r'[^0-9a-z]', '', t)
def map_expected_columns(df, expected_list):
    norm_actual = {normalize_col(c): c for c in df.columns}
    mapping = {name: norm_actual.get(normalize_col(name)) for name in expected_list}
    miss = [k for k,v in mapping.items() if v is None]
    if miss: print("Aviso: no se encontraron columnas ->", miss)
    return mapping

def detectar_col_provincia(columns):
    candidatos = ['PROVINCIA','Provincia','provincia','NOM_PROV','NOM_PROVIN',
                  'NOM_PROVINCIA','PROV_NOM','PROVINC','PROV_NAME','NAME_2']
    for c in candidatos:
        if c in columns: return c
    for c in columns:
        if 'prov' in c.lower(): return c
    return None

# -------- FILTRO ROBUSTO (bordes + fallback por cercanía en metros) --------
def filtrar_llanquihue(df, target_prov=None, tol_m=250):
    """
    Retorna GeoDataFrame de puntos ubicados en la provincia objetivo.
    - Incluye puntos en el borde (predicate='intersects').
    - Fallback: asigna por cercanía con sjoin_nearest hasta tol_m metros
      (y respaldo con buffer si sjoin_nearest no está disponible).
    Mantiene misma firma que la versión simple: retorna solo gdf_ll (EPSG:4326).
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
    prov = gpd.read_file(PROVINCIAS_SHP)
    prov = prov.to_crs('EPSG:4326') if prov.crs else prov.set_crs('EPSG:4326')
    colp = detectar_col_provincia(prov.columns)
    if colp is None:
        raise ValueError("No se identificó columna de provincia en shapefile.")
    try:
        prov['geometry'] = prov.geometry.buffer(0)  # repara geometrías inválidas
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
# ---------------------------------------------------------------------------

# ---- Dunn index (Bray–Curtis) ----
def dunn_index_from_condensed(D_condensed, labels):
    labels = np.asarray(labels)
    uniq = np.unique(labels)
    if uniq.size < 2:
        return np.nan
    D = squareform(D_condensed)
    idx_by_c = {c: np.where(labels == c)[0] for c in uniq}

    diameters = []
    for idx in idx_by_c.values():
        if idx.size <= 1: diameters.append(0.0)
        else:             diameters.append(np.max(D[np.ix_(idx, idx)]))
    max_diam = np.max(diameters) if len(diameters) else np.nan

    min_inter = np.inf
    u = list(uniq)
    for i in range(len(u)):
        for j in range(i+1, len(u)):
            a, b = idx_by_c[u[i]], idx_by_c[u[j]]
            if a.size == 0 or b.size == 0: continue
            m = D[np.ix_(a, b)].min()
            if m < min_inter: min_inter = m

    if not np.isfinite(min_inter): return np.nan
    if max_diam == 0:             return np.inf
    return float(min_inter / max_diam)

# ---- Gap Statistic (usando inertia_ de KMedoids como Wk) ----
def gap_statistic_kmedoids(X, ks, refs=10, metric='braycurtis', random_state=42):
    rng = np.random.RandomState(random_state)
    n, d = X.shape
    mins, maxs = X.min(axis=0), X.max(axis=0)
    gaps, sk = [], []

    for k in ks:
        # Wk del dataset real
        km = KMedoids(n_clusters=k, metric=metric, init='k-medoids++',
                      random_state=random_state)
        km.fit(X)
        wk = km.inertia_

        # Wk* de referencias
        ref_w = []
        for _ in range(refs):
            Xr = rng.uniform(mins, maxs, size=(n, d))
            kmr = KMedoids(n_clusters=k, metric=metric, init='k-medoids++',
                           random_state=rng)
            kmr.fit(Xr)
            ref_w.append(kmr.inertia_)
        log_ref = np.log(ref_w)

        gaps.append(log_ref.mean() - np.log(wk))
        sk.append(np.sqrt(((log_ref - log_ref.mean())**2).sum()/(refs-1)) * np.sqrt(1 + 1/refs))
    return np.array(gaps), np.array(sk)

# ================== Pipeline ==================
# 1) Carga y renombrado
df_raw = pd.read_excel(FILE_PATH, sheet_name=SHEET)
colmap = map_expected_columns(df_raw, EXPECTED_ALL)
df_std = df_raw.rename(columns={colmap[k]: k for k in EXPECTED_ALL if colmap.get(k)})

# 2) Filtro espacial (¡NUEVO!)
gdf_ll = filtrar_llanquihue(df_std, target_prov=TARGET_PROV, tol_m=250)

# 3) Variables y preprocesamiento
present = [v for v in CLUSTER_VARS if v in gdf_ll.columns]
if len(present) < 3:
    raise ValueError(f"Variables insuficientes: {present}")

df_feat = pd.DataFrame(gdf_ll.drop(columns='geometry', errors='ignore'))[present].copy()
cont_cols = [c for c in ["Numero de registros","Ultima Edad Registrada"] if c in df_feat.columns]
bin_cols  = [c for c in ["Mayor","Menor","Moderada","Total_Amputaciones"] if c in df_feat.columns]

ct = ColumnTransformer([
    ('cont', Pipeline([('imp', SimpleImputer(strategy='median')), ('sc', StandardScaler())]), cont_cols),
    ('bin',  Pipeline([('imp', SimpleImputer(strategy='most_frequent'))]), bin_cols),
], remainder='drop')

X = ct.fit_transform(df_feat)

# 4) Distancias precomputadas para Dunn
D_bray_condensed = pdist(X, metric='braycurtis')

# 5) Búsqueda por K
rows = []
for K in K_RANGE:
    km = KMedoids(n_clusters=K, metric='braycurtis', init='k-medoids++',
                  random_state=RANDOM_STATE)
    labels = km.fit_predict(X)

    # SSE / coste (suma distancias a su medoide)
    sse = float(km.inertia_)

    # Métricas (silhouette con Bray–Curtis; DBI/CH sobre X escalado)
    if len(np.unique(labels)) >= 2:
        sil = silhouette_score(X, labels, metric='braycurtis')
        dbi = davies_bouldin_score(X, labels)
        ch  = calinski_harabasz_score(X, labels)
        dunn = dunn_index_from_condensed(D_bray_condensed, labels)
    else:
        sil, dbi, ch, dunn = np.nan, np.nan, np.nan, np.nan

    rows.append({
        'K': K,
        'SSE': sse,
        'silhouette': sil,
        'davies_bouldin': dbi,
        'calinski_harabasz': ch,
        'dunn': np.inf if np.isinf(dunn) else dunn
    })

res = pd.DataFrame(rows).sort_values('K')

# 6) Gap Statistic
gap_vals, gap_std = gap_statistic_kmedoids(X, K_RANGE, refs=N_REF, metric=GAP_METRIC, random_state=RANDOM_STATE)
res['gap'] = gap_vals
res['gap_std'] = gap_std

# 7) Impresión tabla
print("\n=== TUNING K-MEDOIDS (Bray–Curtis) ===")
print("Tabla de métricas por K:")
print(res.to_string(index=False, float_format=lambda x: f"{x:.3f}"))

# 8) Gráficos
plt.figure(figsize=(7,4.2))
plt.plot(res['K'], res['SSE'], '-o')
plt.xlabel('K'); plt.ylabel('SSE')
plt.title('K-Medoids: Curva del codo (SSE)')
plt.grid(True, alpha=0.3); plt.xticks(res['K']); plt.show()

plt.figure(figsize=(7,4.2))
plt.plot(res['K'], res['silhouette'], '-o')
plt.xlabel('K'); plt.ylabel('Coeficiente de Silhouette')
plt.title('K-Medoids: Coeficiente de Silhouette')
plt.grid(True, alpha=0.3); plt.xticks(res['K']); plt.show()

plt.figure(figsize=(7,4.2))
plt.errorbar(res['K'], res['gap'], yerr=res['gap_std'], fmt='-o', capsize=4)
plt.xlabel('K'); plt.ylabel('Gap')
plt.title('K-Medoids: Estadistica de brecha')
plt.grid(True, alpha=0.3); plt.xticks(res['K']); plt.show()

# 9) Sugerencia de K (regla de Tibshirani et al.)
best_k = None
for i in range(len(K_RANGE)-1):
    k, kp1 = K_RANGE[i], K_RANGE[i+1]
    if gap_vals[i] >= gap_vals[i+1] - gap_std[i+1]:
        best_k = k
        break
if best_k is None:
    best_k = K_RANGE[np.argmax(gap_vals)]

best_row = res[res['K']==best_k].iloc[0]
print(f"\n>> K sugerido por Gap Statistic: {best_k} | Gap={best_row.gap:.3f} ± {best_row.gap_std:.3f}")
print(f"   Métricas en K={best_k}: SSE={best_row.SSE:.1f} | Sil={best_row.silhouette:.3f} "
      f"| CH={best_row.calinski_harabasz:.1f} | DBI={best_row.davies_bouldin:.3f} | Dunn={best_row.dunn:.3f}")
