# ================== TUNING DBSCAN (eps & min_samples) + DUNN ==================
# !pip -q install geopandas shapely pyproj fiona rtree openpyxl scikit-learn --upgrade

import re, unicodedata, warnings
import numpy as np, pandas as pd, geopandas as gpd
from shapely.geometry import Point
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.cluster import DBSCAN
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
from sklearn.neighbors import NearestNeighbors
from scipy.spatial.distance import pdist, squareform
import matplotlib.pyplot as plt
warnings.filterwarnings("ignore")

# ---- Config y Imports base ----
FILE_PATH = '/content/drive/MyDrive/HPM/Bases de trabajo/BASE FINAL.xlsx'
SHEET = 0
PROVINCIAS_SHP = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Provincias/Provincias.shp'
COMUNAS_SHP    = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Comunas/comunas.shp'
TARGET_PROV = 'LLANQUIHUE'
RANDOM_STATE = 42

EXPECTED_ALL = ["Sexo (Desc)","lat","lng","tiempo (minutos)","km","Numero de registros","Mayor","Menor","Moderada","Ultima Edad Registrada","Total_Amputaciones"]
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
    t = ''.join(ch for ch in unicodedata.normalize('NFD', str(s)) if unicodedata.category(ch)!='Mn').lower()
    return re.sub(r'[^0-9a-z]', '', t)

def map_expected_columns(df, expected_list):
    norm_actual = {normalize_col(c): c for c in df.columns}
    mapping = {name: norm_actual.get(normalize_col(name)) for name in expected_list}
    miss = [k for k,v in mapping.items() if v is None]
    if miss: print("Aviso: no se encontraron columnas ->", miss)
    return mapping

def detectar_col_provincia(columns):
    for c in ['PROVINCIA','Provincia','provincia','NOM_PROV','NOM_PROVIN','NOM_PROVINCIA','PROV_NOM','PROVINC','PROV_NAME','NAME_2']:
        if c in columns: return c
    for c in columns:
        if 'prov' in c.lower(): return c
    return None

# -------- FILTRO ROBUSTO (bordes + fallback por cercanía en metros) --------
def filtrar_llanquihue(df, target_prov=None, tol_m=250):
    """
    Retorna GeoDataFrame de puntos ubicados en la provincia objetivo (EPSG:4326).
    - Incluye puntos en el borde (predicate='intersects').
    - Repara geometrías con buffer(0).
    - Fallback: sjoin_nearest hasta tol_m metros (o buffer si no está disponible).
    """
    if target_prov is None:
        target_prov = TARGET_PROV

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

    prov = gpd.read_file(PROVINCIAS_SHP)
    prov = prov.to_crs('EPSG:4326') if prov.crs else prov.set_crs('EPSG:4326')
    colp = detectar_col_provincia(prov.columns)
    if colp is None:
        raise ValueError("No se identificó columna de provincia.")

    # Repara geometrías por seguridad
    try:
        prov['geometry'] = prov.geometry.buffer(0)
    except Exception:
        pass

    # Join principal (incluye borde)
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

    norm = lambda s: canon(s).upper()
    j1['__p__'] = j1[colp].map(norm)
    gdf_ll = j1[j1['__p__'] == norm(target_prov)].drop(columns=['__p__','index_right'], errors='ignore')

    if gdf_ll.empty:
        raise ValueError(f"No hay puntos en '{target_prov}' (tol={tol_m}m).")

    return gdf_ll.to_crs('EPSG:4326')
# ---------------------------------------------------------------------------

# ---- Dunn index (usando matriz de distancias cuadrada) ----
def dunn_index_from_square(D_square, labels):
    """
    Dunn = (mínima distancia inter-cluster) / (máximo diámetro intra-cluster).
    D_square: matriz NxN de distancias (Bray–Curtis aquí).
    labels: etiquetas de clusters (>=2 clusters requeridos).
    """
    labels = np.asarray(labels)
    uniq = np.unique(labels)
    if uniq.size < 2:
        return np.nan

    idx_by_c = {c: np.where(labels == c)[0] for c in uniq}

    # diámetros intra
    diameters = []
    for c, idx in idx_by_c.items():
        if idx.size <= 1:
            diameters.append(0.0)  # singleton
        else:
            sub = D_square[np.ix_(idx, idx)]
            diameters.append(np.max(sub))
    max_diam = np.max(diameters) if diameters else np.nan

    # mínima distancia inter
    min_inter = np.inf
    ulist = list(uniq)
    for i in range(len(ulist)):
        for j in range(i+1, len(ulist)):
            a = idx_by_c[ulist[i]]; b = idx_by_c[ulist[j]]
            if a.size == 0 or b.size == 0:
                continue
            m = D_square[np.ix_(a, b)].min()
            if m < min_inter:
                min_inter = m

    if not np.isfinite(min_inter):
        return np.nan
    if max_diam == 0:
        return np.inf
    return float(min_inter / max_diam)

# ---- Load + prep ----
df_raw = pd.read_excel(FILE_PATH, sheet_name=SHEET)
colmap = map_expected_columns(df_raw, EXPECTED_ALL)
df_std = df_raw.rename(columns={colmap[k]: k for k in EXPECTED_ALL if colmap.get(k)})
gdf_ll = filtrar_llanquihue(df_std, target_prov=TARGET_PROV, tol_m=250)

present = [v for v in CLUSTER_VARS if v in gdf_ll.columns]
if len(present) < 3:
    raise ValueError(f"Variables insuficientes: {present}")
df_feat = pd.DataFrame(gdf_ll.drop(columns='geometry'))[present].copy()

cont_cols = [c for c in ["Numero de registros","Ultima Edad Registrada"] if c in df_feat.columns]
bin_cols  = [c for c in ["Mayor","Menor","Moderada","Total_Amputaciones"] if c in df_feat.columns]

ct = ColumnTransformer([
    ('cont', Pipeline([('imp', SimpleImputer(strategy='median')), ('sc', StandardScaler())]), cont_cols),
    ('bin',  Pipeline([('imp', SimpleImputer(strategy='most_frequent'))]), bin_cols),
], remainder='drop')
X = ct.fit_transform(df_feat)

# ---- Distancias Bray–Curtis precomputadas (una sola vez) para Dunn ----
D_all = squareform(pdist(X, metric='braycurtis'))

# ---- Grid razonable de min_samples y eps por percentiles k-distance ----
d = X.shape[1]
min_samples_list = sorted(set([max(5, d), max(5, int(1.5*d)), max(5, 2*d)]))
rows=[]

for ms in min_samples_list:
    nn = NearestNeighbors(n_neighbors=ms, metric='braycurtis').fit(X)
    dists, _ = nn.kneighbors(X)
    kth = np.sort(dists[:,-1])

    # candidates eps por percentiles (ajusta si hace falta)
    eps_candidates = np.unique(np.round(np.percentile(kth, [80,85,90,92,95,97,98,99]), 3))
    for eps in eps_candidates:
        db = DBSCAN(eps=float(eps), min_samples=int(ms), metric='braycurtis')
        labels = db.fit_predict(X)

        valid = labels != -1
        n_clusters = len(np.unique(labels[valid])) if valid.any() else 0
        noise_ratio = 1 - (valid.sum()/len(labels))

        if n_clusters >= 2:
            Xv, Lv = X[valid], labels[valid]
            sil = silhouette_score(Xv, Lv, metric='braycurtis')
            dbi = davies_bouldin_score(Xv, Lv)
            ch  = calinski_harabasz_score(Xv, Lv)

            # ---- DUNN (Bray–Curtis, consistente con la métrica de DBSCAN) ----
            idx_valid = np.where(valid)[0]
            D_sub = D_all[np.ix_(idx_valid, idx_valid)]
            dunn = dunn_index_from_square(D_sub, Lv)
        else:
            sil, dbi, ch, dunn = np.nan, np.nan, np.nan, np.nan

        rows.append({
            'min_samples': ms,
            'eps': float(eps),
            'n_clusters': int(n_clusters),
            'noise_ratio': round(float(noise_ratio), 3),
            'silhouette': sil,
            'davies_bouldin': dbi,
            'calinski_harabasz': ch,
            'dunn': dunn
        })

res = pd.DataFrame(rows)

# Orden sugerida: Silhouette desc, Dunn desc, CH desc, DBI asc
res = res.sort_values(
    ['silhouette','dunn','calinski_harabasz','davies_bouldin'],
    ascending=[False, False, False, True]
)

print("\n=== TUNING DBSCAN ===")
print("Top 12 combinaciones (ordenadas por Silhouette, Dunn, CH, DBI):")
def _fmt(x):
    if isinstance(x, (float, np.floating)):
        if np.isinf(x): return "inf"
        if np.isnan(x): return "nan"
        return f"{x:.3f}"
    return str(x)
print(res.head(12).to_string(index=False, formatters={c:_fmt for c in res.columns}))

best = res.iloc[0]
print(f"\n>> Recomendado: eps={best.eps} | min_samples={int(best.min_samples)} "
      f"| clusters={int(best.n_clusters)} | ruido={best.noise_ratio:.2f} "
      f"| Silhouette={_fmt(best.silhouette)} | Dunn={_fmt(best.dunn)} "
      f"| CH={_fmt(best.calinski_harabasz)} | DBI={_fmt(best.davies_bouldin)}" )

# ================== GRÁFICAS SOLICITADAS ==================
res_valid = res.dropna(subset=['silhouette']).copy()

if res_valid.empty:
    print("\n[Aviso] No hay combinaciones válidas (>=2 clústeres) para graficar Silhouette.")
else:
    # --- Silhouette vs eps (máximo por eps)
    sil_by_eps = (res_valid
                  .groupby('eps', as_index=False)['silhouette']
                  .max()
                  .sort_values('eps'))

    plt.figure(figsize=(7,4.2))
    plt.plot(sil_by_eps['eps'], sil_by_eps['silhouette'], marker='o')
    plt.xlabel('eps'); plt.ylabel('Silhouette'); plt.title('DBSCAN: eps')
    plt.grid(True, linestyle='--', alpha=0.4); plt.tight_layout(); plt.show()

    # --- Silhouette vs min_samples (máximo por min_samples)
    sil_by_ms = (res_valid
                 .groupby('min_samples', as_index=False)['silhouette']
                 .max()
                 .sort_values('min_samples'))

    plt.figure(figsize=(7,4.2))
    plt.plot(sil_by_ms['min_samples'], sil_by_ms['silhouette'], marker='o')
    plt.xlabel('min_samples'); plt.ylabel('Silhouette'); plt.title('DBSCAN: min_samples')
    plt.grid(True, linestyle='--', alpha=0.4); plt.tight_layout(); plt.show()