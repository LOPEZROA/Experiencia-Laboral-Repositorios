# ================== TUNING HDBSCAN (min_cluster_size, min_samples) + DUNN + DBCV + PERSISTENCIA + GRAFICOS ==================
# !pip -q install geopandas shapely pyproj fiona rtree openpyxl scikit-learn hdbscan --upgrade

import re, unicodedata, warnings
import numpy as np, pandas as pd, geopandas as gpd
from shapely.geometry import Point
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
from scipy.spatial.distance import pdist, squareform  # <-- para Dunn
import hdbscan
import matplotlib.pyplot as plt
warnings.filterwarnings("ignore")

# ---- Config e imports ----
FILE_PATH = '/content/drive/MyDrive/HPM/Bases de trabajo/BASE FINAL.xlsx'
SHEET = 0
PROVINCIAS_SHP = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Provincias/Provincias.shp'
COMUNAS_SHP    = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Comunas/comunas.shp'
TARGET_PROV = 'LLANQUIHUE'
RANDOM_STATE = 42

# ---- Dunn helper ----
def dunn_index_from_square(D_square, labels):
    """
    Dunn = (mínima distancia inter-cluster) / (máximo diámetro intra-cluster).
    D_square: matriz NxN de distancias (p.ej. Euclídea), labels: etiquetas de clusters.
    Ignora ruido (-1) para el cálculo.
    """
    labels = np.asarray(labels)
    uniq = np.array([u for u in np.unique(labels) if u != -1])  # ignorar ruido
    if uniq.size < 2:
        return np.nan

    idx_by_c = {c: np.where(labels == c)[0] for c in uniq}

    # Diámetros intra
    diameters = []
    for idx in idx_by_c.values():
        if idx.size <= 1:
            diameters.append(0.0)
        else:
            sub = D_square[np.ix_(idx, idx)]
            diameters.append(np.max(sub))
    if not diameters:
        return np.nan
    max_diam = np.max(diameters)

    # Mínima distancia inter
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

# -------- FILTRO ROBUSTO (incluye borde + fallback por cercanía en metros) --------
def filtrar_llanquihue(df, target_prov=None, tol_m=250):
    """
    Retorna GeoDataFrame con puntos asignados a la provincia objetivo (EPSG:4326).
    - Usa 'intersects' (incluye borde) en lugar de 'within'.
    - Repara geometrías inválidas con buffer(0).
    - Fallback por cercanía: sjoin_nearest hasta tol_m metros (o buffer si no está disponible).
    Compatible con tu llamada existente: filtrar_llanquihue(df_std)
    """
    if target_prov is None:
        target_prov = TARGET_PROV

    if 'lat' not in df.columns or 'lng' not in df.columns:
        raise ValueError("Faltan 'lat'/'lng'.")

    df = df.copy()
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lng'] = pd.to_numeric(df['lng'], errors='coerce')
    df = df.dropna(subset=['lat','lng'])

    gdf_pts = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df['lng'], df['lat']),
        crs='EPSG:4326'
    )

    prov = gpd.read_file(PROVINCIAS_SHP)
    prov = prov.to_crs('EPSG:4326') if prov.crs else prov.set_crs('EPSG:4326')
    colp = detectar_col_provincia(prov.columns)
    if colp is None:
        raise ValueError("No se identificó columna de provincia.")

    # Repara geometrías inválidas
    try:
        prov['geometry'] = prov.geometry.buffer(0)
    except Exception:
        pass

    # Join principal: incluye puntos de borde
    try:
        j1 = gpd.sjoin(gdf_pts, prov[[colp, 'geometry']], how='left', predicate='intersects')
    except TypeError:
        j1 = gpd.sjoin(gdf_pts, prov[[colp, 'geometry']], how='left', op='intersects')

    # Fallback por cercanía para no asignados
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

    # Filtrado final por la provincia objetivo (normalizada)
    norm = lambda s: canon(s).upper()
    j1['__p__'] = j1[colp].map(norm)
    gdf_ll = j1[joined_mask := (j1['__p__'] == norm(target_prov))].drop(columns=['__p__','index_right'], errors='ignore')

    if gdf_ll.empty:
        raise ValueError(f"No hay puntos en '{target_prov}' (tol={tol_m}m).")

    return gdf_ll.to_crs('EPSG:4326')
# ---------------------------------------------------------------------------

# ---- Load + prep ----
df_raw = pd.read_excel(FILE_PATH, sheet_name=SHEET)
colmap = map_expected_columns(df_raw, EXPECTED_ALL)
df_std = df_raw.rename(columns={colmap[k]: k for k in EXPECTED_ALL if colmap.get(k)})
gdf_ll = filtrar_llanquihue(df_std)  # misma llamada

present = [v for v in CLUSTER_VARS if v in gdf_ll.columns]
if len(present) < 3: raise ValueError(f"Variables insuficientes: {present}")
df_feat = pd.DataFrame(gdf_ll.drop(columns='geometry'))[present].copy()

cont_cols = [c for c in ["Numero de registros","Ultima Edad Registrada"] if c in df_feat.columns]
bin_cols  = [c for c in ["Mayor","Menor","Moderada","Total_Amputaciones"] if c in df_feat.columns]

ct = ColumnTransformer([
    ('cont', Pipeline([('imp', SimpleImputer(strategy='median')), ('sc', StandardScaler())]), cont_cols),
    ('bin',  Pipeline([('imp', SimpleImputer(strategy='most_frequent'))]), bin_cols),
], remainder='drop')
X = ct.fit_transform(df_feat)
n = X.shape[0]

# ---- Grids de min_cluster_size y min_samples ----
def pct_to_int(p): return max(5, int(round(n * p)))
mcs_list = sorted(set([pct_to_int(p) for p in [0.02, 0.03, 0.05, 0.08, 0.10]]))
ms_list  = [None] + [max(5, int(m/2)) for m in mcs_list]  # opcional

# Intentar importar el índice de validez (DBCV)
_validity_fn = None
try:
    from hdbscan.validity import validity_index as _validity_fn
except Exception:
    _validity_fn = None  # si no está disponible, quedará NaN

rows=[]
for mcs in mcs_list:
    for ms in ms_list:
        hdb = hdbscan.HDBSCAN(
            min_cluster_size=mcs,
            min_samples=ms,
            metric='euclidean',
            cluster_selection_method='leaf'
        )
        labels = hdb.fit_predict(X)
        valid = labels != -1
        n_clusters = len(np.unique(labels[valid])) if valid.any() else 0
        noise_ratio = 1 - (valid.sum()/len(labels))

        # Métricas clásicas + Dunn sobre inliers
        if n_clusters >= 2:
            Xv, Lv = X[valid], labels[valid]
            sil = silhouette_score(Xv, Lv, metric='euclidean')
            dbi = davies_bouldin_score(Xv, Lv)
            ch  = calinski_harabasz_score(Xv, Lv)
            Dv  = squareform(pdist(Xv, metric='euclidean'))
            dunn = dunn_index_from_square(Dv, Lv)
        else:
            sil, dbi, ch, dunn = np.nan, np.nan, np.nan, np.nan

        # ---- DBCV (si hay >=2 clústeres y la función existe) ----
        if n_clusters >= 2 and _validity_fn is not None:
            try:
                dbcv = float(_validity_fn(X, labels, metric='euclidean'))
            except Exception:
                dbcv = np.nan
        else:
            dbcv = np.nan

        # ---- Persistencia de clústeres (del árbol condensado) ----
        persistence = getattr(hdb, 'cluster_persistence_', None)
        if persistence is not None and len(persistence) > 0:
            persistence_sum  = float(np.sum(persistence))
            persistence_mean = float(np.mean(persistence))
        else:
            persistence_sum  = np.nan
            persistence_mean = np.nan

        rows.append({
            'min_cluster_size': mcs,
            'min_samples': (ms if ms is not None else -1),  # -1 representa None
            'n_clusters': n_clusters,
            'noise_ratio': round(noise_ratio,3),
            'dbcv': dbcv,
            'persistence_sum': persistence_sum,
            'persistence_mean': persistence_mean,
            'silhouette': sil,
            'davies_bouldin': dbi,
            'calinski_harabasz': ch,
            'dunn': dunn
        })

res = pd.DataFrame(rows).sort_values(
    ['dbcv','persistence_sum','silhouette','dunn','calinski_harabasz','davies_bouldin'],
    ascending=[False, False, False, False, False, True]
)

print("\n=== TUNING HDBSCAN ===")
print("Top 12 combinaciones:")
def fmt_ms(x): return "None" if x==-1 else str(int(x))
def fmt_dunn(x):
    if pd.isna(x): return "nan"
    if np.isinf(x): return "inf"
    return f"{x:.3f}"
tmp = res.head(12).copy()
tmp['min_samples'] = tmp['min_samples'].apply(fmt_ms)
tmp['dunn'] = tmp['dunn'].apply(fmt_dunn)
print(tmp.to_string(index=False, float_format=lambda x: f"{x:.3f}"))

best = res.iloc[0]
ms_txt = "None" if best.min_samples==-1 else str(int(best.min_samples))
best_dunn = "inf" if np.isinf(best.dunn) else ("nan" if pd.isna(best.dunn) else f"{best.dunn:.3f}")
best_dbcv = "nan" if pd.isna(best.dbcv) else f"{best.dbcv:.3f}"
best_pers_sum  = "nan" if pd.isna(best.persistence_sum) else f"{best.persistence_sum:.1f}"
best_pers_mean = "nan" if pd.isna(best.persistence_mean) else f"{best.persistence_mean:.3f}"
print(f"\n>> Recomendado: min_cluster_size={int(best.min_cluster_size)} | min_samples={ms_txt} "
      f"| clusters={int(best.n_clusters)} | ruido={best.noise_ratio:.2f} "
      f"| DBCV={best_dbcv} | Persistencia(sum)={best_pers_sum} (mean={best_pers_mean}) "
      f"| Silhouette={best.silhouette:.3f} | Dunn={best_dunn} | CH={best.calinski_harabasz:.1f} | DBI={best.davies_bouldin:.3f}")

# ================== GRAFICOS SOLICITADOS ==================
def _label_ms_tick(v):
    try:
        v = int(v)
        return "None" if v == -1 else str(v)
    except Exception:
        return str(v)

def _plot_lines(df, x_col, y_col, group_col, title):
    """
    Dibuja curvas (líneas + marcadores) para cada valor único de group_col,
    ordenando por x_col. Útil para ver cómo cambia y_col con x_col,
    condicionado por el otro hiperparámetro.
    """
    plt.figure(figsize=(7,4.5))
    for key, sub in df.groupby(group_col):
        sub = sub.sort_values(x_col)
        label = f"{group_col}="
        if group_col == 'min_samples':
            label += _label_ms_tick(key)
        else:
            try: label += str(int(key))
            except: label += str(key)
        plt.plot(sub[x_col], sub[y_col], marker='o', linestyle='-', label=label)
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    plt.title(title)
    if x_col == 'min_samples':
        ticks = sorted(df[x_col].unique())
        plt.xticks(ticks, [_label_ms_tick(t) for t in ticks])
    plt.grid(True, alpha=0.3)
    plt.legend(title="Series", fontsize=8)
    plt.tight_layout()
    plt.show()


# 3) min_cluster_size x dbcv (series por min_samples)
_plot_lines(res, x_col='min_cluster_size', y_col='dbcv', group_col='min_samples',
            title='DBCV')

# 4) min_samples x dbcv (series por min_cluster_size)
_plot_lines(res, x_col='min_samples', y_col='dbcv', group_col='min_cluster_size',
            title='DBCV')
cols_show2 = ['min_cluster_size','min_samples','n_clusters','noise_ratio',
             'dunn','dbcv','silhouette','calinski_harabasz','davies_bouldin']
def _print_table_for_ms2(df, ms):
    tbl = df[df['min_samples'] == ms][cols_show2].copy()
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

_print_table_for_ms2(res, 9)
_print_table_for_ms2(res, 14)