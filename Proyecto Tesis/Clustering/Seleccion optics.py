# ================== TUNING OPTICS (xi, min_cluster_size, min_samples) + DUNN + DBCV + GRAFICOS (SIN xi EN GRAFICOS) ==================

import re, unicodedata, warnings
import numpy as np, pandas as pd, geopandas as gpd
from shapely.geometry import Point
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.cluster import OPTICS
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
from scipy.spatial.distance import pdist, squareform
import matplotlib.pyplot as plt
warnings.filterwarnings("ignore")

# ---- Config e imports ----
FILE_PATH = '/content/drive/MyDrive/HPM/Bases de trabajo/BASE FINAL.xlsx'
SHEET = 0
PROVINCIAS_SHP = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Provincias/Provincias.shp'
COMUNAS_SHP    = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Comunas/comunas.shp'
TARGET_PROV = 'LLANQUIHUE'

# ===== Helpers =====
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
    Retorna GeoDataFrame con los puntos en la provincia objetivo (EPSG:4326).
    - Incluye puntos en el borde (predicate='intersects').
    - Repara geometrías inválidas con buffer(0).
    - Fallback: asigna provincia por cercanía con sjoin_nearest hasta tol_m metros
      (y respaldo con buffer si sjoin_nearest no está disponible).
    Compatible con llamada existente: filtrar_llanquihue(df_std)
    """
    if target_prov is None:
        target_prov = TARGET_PROV

    # Validación y armado de puntos
    if 'lat' not in df.columns or 'lng' not in df.columns:
        raise ValueError("Faltan 'lat'/'lng'.")
    df = df.copy()
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lng'] = pd.to_numeric(df['lng'], errors='coerce')
    ok = df['lat'].notna() & df['lng'].notna()

    gdf_pts = gpd.GeoDataFrame(
        df.loc[ok].copy(),
        geometry=gpd.points_from_xy(df.loc[ok, 'lng'], df.loc[ok, 'lat']),
        crs='EPSG:4326'
    )

    # Provincias (EPSG:4326 + reparación de geometrías)
    prov = gpd.read_file(PROVINCIAS_SHP)
    prov = prov.to_crs('EPSG:4326') if prov.crs else prov.set_crs('EPSG:4326')
    colp = detectar_col_provincia(prov.columns)
    if colp is None:
        raise ValueError("No se identificó columna de provincia.")
    try:
        prov['geometry'] = prov.geometry.buffer(0)
    except Exception:
        pass

    # Join principal (intersects incluye borde)
    try:
        j1 = gpd.sjoin(gdf_pts, prov[[colp,'geometry']], how='left', predicate='intersects')
    except TypeError:
        j1 = gpd.sjoin(gdf_pts, prov[[colp,'geometry']], how='left', op='intersects')

    # Fallback por cercanía (metros) para puntos no asignados
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
            # Respaldo con buffer si sjoin_nearest no está disponible
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

    # Filtrado final por provincia objetivo (normalizado)
    norm = lambda s: canon(s).upper()
    j1['__p__'] = j1[colp].map(norm)
    gdf_ll = j1[j1['__p__'] == norm(target_prov)].drop(columns=['__p__','index_right'], errors='ignore')
    if gdf_ll.empty:
        raise ValueError(f"No hay puntos en '{target_prov}' (tol={tol_m}m).")

    return gdf_ll.to_crs('EPSG:4326')
# ---------------------------------------------------------------------------

def dunn_index_from_square(D_square, labels):
    labels = np.asarray(labels)
    uniq = np.array([u for u in np.unique(labels) if u != -1])  # ignora ruido
    if uniq.size < 2: return np.nan
    idx_by_c = {c: np.where(labels == c)[0] for c in uniq}
    diameters = []
    for idx in idx_by_c.values():
        if idx.size <= 1: diameters.append(0.0)
        else: diameters.append(np.max(D_square[np.ix_(idx, idx)]))
    if not diameters: return np.nan
    max_diam = np.max(diameters)
    min_inter = np.inf
    u = list(uniq)
    for i in range(len(u)):
        for j in range(i+1, len(u)):
            a, b = idx_by_c[u[i]], idx_by_c[u[j]]
            if a.size == 0 or b.size == 0: continue
            m = D_square[np.ix_(a, b)].min()
            if m < min_inter: min_inter = m
    if not np.isfinite(min_inter): return np.nan
    if max_diam == 0: return np.inf
    return float(min_inter / max_diam)

# ================== Carga + preprocesamiento ==================
df_raw = pd.read_excel(FILE_PATH, sheet_name=SHEET)
colmap = map_expected_columns(df_raw, EXPECTED_ALL)
df_std  = df_raw.rename(columns={colmap[k]: k for k in EXPECTED_ALL if colmap.get(k)})
gdf_ll  = filtrar_llanquihue(df_std)  # <- misma llamada que tenías

present = [v for v in CLUSTER_VARS if v in gdf_ll.columns]
if len(present) < 3:
    raise ValueError(f"Variables insuficientes: {present}")

df_feat = pd.DataFrame(gdf_ll.drop(columns='geometry'))[present].copy()

cat_cols  = ["Sexo (Desc)"] if "Sexo (Desc)" in df_feat.columns else []
cont_cols = [c for c in ["tiempo (minutos)", "km", "Numero de registros", "Ultima Edad Registrada"] if c in df_feat.columns]
bin_cols  = [c for c in ["Mayor","Menor","Moderada","Total_Amputaciones"] if c in df_feat.columns]

try:
    ohe = OneHotEncoder(handle_unknown='ignore', sparse_output=False)
except TypeError:
    ohe = OneHotEncoder(handle_unknown='ignore', sparse=False)

ct = ColumnTransformer(
    transformers=[
        ('cont', Pipeline([('imp', SimpleImputer(strategy='median')),
                           ('sc', StandardScaler())]), cont_cols),
        ('bin',  Pipeline([('imp', SimpleImputer(strategy='most_frequent'))]), bin_cols),
        ('cat',  Pipeline([('imp', SimpleImputer(strategy='most_frequent')),
                           ('oh', ohe)]), cat_cols)
    ],
    remainder='drop'
)

X = ct.fit_transform(df_feat)
n = X.shape[0]

# ================== Grid OPTICS ==================
METRIC = 'braycurtis'     # usa la misma métrica que en tu aplicación
XI_LIST  = [0.03, 0.05, 0.10, 0.20]
MCS_LIST = sorted(set([max(5, int(round(n*p))) for p in [0.02, 0.03, 0.05, 0.10, 0.15]]))
MIN_SAMPLES_LIST = [2, 5, 10, 20]  # si quieres uno solo, deja [2]

# DBCV (vía hdbscan.validity)
try:
    from hdbscan.validity import validity_index as _validity_fn
except Exception:
    _validity_fn = None

rows=[]
for xi in XI_LIST:
    for mcs in MCS_LIST:
        for ms in MIN_SAMPLES_LIST:
            opt = OPTICS(min_samples=ms, xi=xi, min_cluster_size=mcs, metric=METRIC)
            labels = opt.fit_predict(X)
            valid = labels != -1
            n_clusters = len(np.unique(labels[valid])) if valid.any() else 0
            noise_ratio = 1 - (valid.sum()/len(labels))

            if n_clusters >= 2:
                Xv, Lv = X[valid], labels[valid]
                sil = silhouette_score(Xv, Lv, metric=METRIC)
                dbi = davies_bouldin_score(Xv, Lv)
                ch  = calinski_harabasz_score(Xv, Lv)
                Dv  = squareform(pdist(Xv, metric=METRIC))
                dunn = dunn_index_from_square(Dv, Lv)
            else:
                sil, dbi, ch, dunn = np.nan, np.nan, np.nan, np.nan

            if n_clusters >= 2 and _validity_fn is not None:
                try:
                    dbcv = float(_validity_fn(X, labels, metric=METRIC))
                except Exception:
                    dbcv = np.nan
            else:
                dbcv = np.nan

            rows.append({
                'xi': xi,
                'min_cluster_size': mcs,
                'min_samples': ms,
                'n_clusters': n_clusters,
                'noise_ratio': round(noise_ratio, 3),
                'silhouette': sil,
                'dunn': dunn,
                'calinski_harabasz': ch,
                'davies_bouldin': dbi,
                'dbcv': dbcv
            })

# ================== ORDEN: Dunn primero, luego DBCV ==================
res = pd.DataFrame(rows).sort_values(
    ['dunn', 'dbcv', 'silhouette', 'calinski_harabasz', 'davies_bouldin'],
    ascending=[False, False, False, False, True]
)

def _fmt(x):
    if isinstance(x, (float, np.floating)):
        if np.isinf(x): return "inf"
        if np.isnan(x): return "nan"
        return f"{x:.3f}"
    return str(x)

print("\n=== TUNING OPTICS (alineado) ===")
print("Top 12 combinaciones:")
print(res.head(12).to_string(index=False, formatters={
    'silhouette': _fmt, 'dunn': _fmt, 'calinski_harabasz': _fmt,
    'davies_bouldin': _fmt, 'dbcv': _fmt
}))

best = res.iloc[0]
print(
    f"\n>> Recomendado: xi={best.xi} | min_cluster_size={int(best.min_cluster_size)} | min_samples={int(best.min_samples)} "
    f"| clusters={int(best.n_clusters)} | ruido={best.noise_ratio:.2f} "
    f"| DBCV={_fmt(best.dbcv)} | Silhouette={_fmt(best.silhouette)} | Dunn={_fmt(best.dunn)} "
    f"| CH={_fmt(best.calinski_harabasz)} | DBI={_fmt(best.davies_bouldin)}"
)

# ================== GRAFICOS (SIN xi) ==================
# Elegimos, para cada (min_samples, min_cluster_size), la combinación con MAYOR Dunn.
# Desempate: mayor DBCV, luego mayor Silhouette.
best_by_dunn = (
    res.sort_values(['dunn', 'dbcv', 'silhouette'], ascending=[False, False, False])
      .groupby(['min_samples', 'min_cluster_size'], as_index=False)
      .first()
)

# Usaremos DBCV y Silhouette del "mejor Dunn" por par
agg = best_by_dunn[['min_samples', 'min_cluster_size', 'dbcv', 'silhouette']].copy()

def _plot_lines(df, x_col, y_col, series_col, title):
    plt.figure(figsize=(7,4.5))
    for key, sub in df.groupby(series_col):
        sub = sub.dropna(subset=[x_col, y_col]).sort_values(x_col)
        if sub.empty:
            continue
        plt.plot(sub[x_col], sub[y_col], marker='o', linestyle='-', label=f"{series_col}={int(key)}")
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    plt.title(title)
    plt.grid(True, alpha=0.3)
    plt.legend(title=series_col, fontsize=8, ncol=2)
    plt.tight_layout()
    plt.show()

# 1) Min samples × DBCV (series por min_cluster_size)
_plot_lines(agg, x_col='min_samples', y_col='dbcv', series_col='min_cluster_size',
            title='min_samples')

# 2) Min cluster size × DBCV (series por min_samples)
_plot_lines(agg, x_col='min_cluster_size', y_col='dbcv', series_col='min_samples',
            title='min_cluster_size')

# ================== TABLAS FILTRADAS POR min_samples (2 y 5) ==================
cols_show = ['xi','min_cluster_size','min_samples','n_clusters','noise_ratio',
             'dunn','dbcv','silhouette','calinski_harabasz','davies_bouldin']

#def _print_table_for_ms(df, ms):
    #tbl = df[df['min_samples'] == ms][cols_show].copy()
    #if tbl.empty:
        #print(f"\n(No hay combinaciones con min_samples={ms})")
        #return
    #tbl['min_cluster_size'] = tbl['min_cluster_size'].astype(int)
    #tbl['min_samples']      = tbl['min_samples'].astype(int)
    #tbl['n_clusters']       = tbl['n_clusters'].astype(int)
    #fmt_map = {
        #'noise_ratio': lambda x: f"{x:.3f}",
        #'dunn': _fmt, 'dbcv': _fmt, 'silhouette': _fmt,
        #'calinski_harabasz': _fmt, 'davies_bouldin': _fmt
    #}
    #print(f"\n=== Tabla (min_samples={ms}) — orden: Dunn > DBCV > Silhouette > CH > DBI ===")
    #print(tbl.to_string(index=False, formatters=fmt_map))

#_print_table_for_ms(res, 2)
#_print_table_for_ms(res, 5)
#_print_table_for_ms(res, 10)
#_print_table_for_ms(res, 20)

def _print_table_for_ms2(df, ms):
    tbl = df[df['min_samples'] == ms][cols_show].copy()
    if tbl.empty:
        print(f"\n(No hay combinaciones con min_samples={ms})")
        return

    # casteos “bonitos”
    tbl['min_cluster_size'] = tbl['min_cluster_size'].astype(int)
    tbl['min_samples']      = tbl['min_samples'].astype(int)
    tbl['n_clusters']       = tbl['n_clusters'].astype(int)

    # --- ORDEN: primero por menor porcentaje de ruido ---
    tbl = tbl.sort_values(
        ['noise_ratio', 'dunn', 'dbcv', 'silhouette', 'calinski_harabasz', 'davies_bouldin'],
        ascending=[True,         False,  False,     False,        False,              True]
    )

    fmt_map = {
        'noise_ratio': lambda x: f"{x:.3f}",
        'dunn': _fmt, 'dbcv': _fmt, 'silhouette': _fmt,
        'calinski_harabasz': _fmt, 'davies_bouldin': _fmt
    }

    print(f"\n=== Tabla (min_samples={ms}) — orden: Menor ruido > Dunn > DBCV > Silhouette > CH > DBI ===")
    print(tbl.to_string(index=False, formatters=fmt_map))

_print_table_for_ms2(res, 2)
_print_table_for_ms2(res, 5)
_print_table_for_ms2(res, 10)
_print_table_for_ms2(res, 20)