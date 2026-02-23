# ================== OPTICS (alternativa a DBSCAN) + DUNN + Moran I / LISA ==================
!pip -q install geopandas shapely pyproj fiona rtree openpyxl scikit-learn --upgrade


# ---- Imports ----
import os, re, unicodedata, warnings
from datetime import datetime
import numpy as np, pandas as pd, geopandas as gpd, matplotlib.pyplot as plt
from shapely.geometry import Point
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.cluster import OPTICS
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
from scipy.spatial.distance import pdist, squareform
import matplotlib.patheffects as pe
warnings.filterwarnings("ignore")

# ---- Config ----
FILE_PATH = '/content/drive/MyDrive/HPM/Bases de trabajo/BASE FINAL.xlsx'
SHEET = 0
PROVINCIAS_SHP = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Provincias/Provincias.shp'
COMUNAS_SHP    = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Comunas/comunas.shp'
TARGET_PROV = 'LLANQUIHUE'

# Hiperparámetros OPTICS
MIN_SAMPLES = 10
XI = 0.05
MIN_CLUSTER_SIZE = 97
METRIC = 'braycurtis'

# --- Autocorrelación espacial ---
AUTOCORR_VAR = 'Numero de registros'
K_NEIGHBORS  = 3

RANDOM_STATE = 42

# ---- (Auto) instalación PySAL si falta ----
try:
    from libpysal.weights import KNN
    from esda.moran import Moran, Moran_Local
except Exception:
    import sys, subprocess
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "libpysal", "esda", "mapclassify"], check=False)
    from libpysal.weights import KNN
    from esda.moran import Moran, Moran_Local

# ---- Dunn helper ----
def dunn_index_from_square(D_square, labels):
    labels = np.asarray(labels)
    uniq = np.unique(labels)
    if uniq.size < 2: return np.nan
    idx_by_c = {c: np.where(labels == c)[0] for c in uniq}
    diameters = []
    for idx in idx_by_c.values():
        if idx.size <= 1:
            diameters.append(0.0)
        else:
            sub = D_square[np.ix_(idx, idx)]
            diameters.append(np.max(sub))
    max_diam = np.max(diameters) if len(diameters) else np.nan
    min_inter = np.inf
    ulist = list(uniq)
    for i in range(len(ulist)):
        for j in range(i+1, len(ulist)):
            a = idx_by_c[ulist[i]]; b = idx_by_c[ulist[j]]
            if a.size == 0 or b.size == 0: continue
            m = D_square[np.ix_(a, b)].min()
            if m < min_inter: min_inter = m
    if not np.isfinite(min_inter): return np.nan
    if max_diam == 0: return np.inf
    return float(min_inter / max_diam)

# ---- Correlograma (heatmap) para matrices de correlación (NO incluye tiempo/km) ----
def plot_correlogram(corr: pd.DataFrame, title: str = None):
    cols = list(corr.columns)
    M = corr.to_numpy(dtype=float)
    M_masked = np.ma.masked_invalid(M)

    cmap = plt.cm.coolwarm.copy()
    cmap.set_bad(color='#eaeaea')

    n = len(cols)
    fw = max(6, min(1.0 * n + 2, 20))
    fh = max(4.5, fw * 0.75)

    fig, ax = plt.subplots(figsize=(fw, fh))
    im = ax.imshow(M_masked, vmin=-1, vmax=1, cmap=cmap)

    cbar = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    cbar.set_label("Spearman ρ", rotation=90)

    ax.set_xticks(range(n)); ax.set_yticks(range(n))
    ax.set_xticklabels(cols, rotation=45, ha='right')
    ax.set_yticklabels(cols)
    ax.set_title(title if title else "Correlograma (Spearman)")

    ax.set_xticks(np.arange(-.5, n, 1), minor=True)
    ax.set_yticks(np.arange(-.5, n, 1), minor=True)
    ax.grid(which="minor", color="white", linestyle='-', linewidth=1)
    ax.tick_params(axis='both', which='both', length=0)

    for i in range(n):
        for j in range(n):
            val = M[i, j]
            if np.isfinite(val):
                ax.text(j, i, f"{val:.2f}", ha='center', va='center', fontsize=8)

    plt.tight_layout()
    plt.show()

# ---- Helpers ----
corr_var = [
    "Numero de registros", "Ultima Edad Registrada", "Total_Amputaciones",
    "Mayor", "Menor", "Moderada",
    'AMP_DEDO_MANO','AMP_PULGAR','AMP_DEDO_PIE','AMP_A_NIVEL_PIE','DESART_TOBILLO',
    'AMP_NIVEL_MALEOLO','AMP_DEBAJO_RODILLA','DESART_RODILLA','AMP_ENCIMA_RODILLA'
]

EXPECTED_ALL = [
    "Sexo (Desc)", "lat", "lng",
    "tiempo (minutos)", "km", "Numero de registros",
    "Mayor", "Menor", "Moderada",
    "Ultima Edad Registrada", "Total_Amputaciones"
]

CLUSTER_VARS = [
    "Numero de registros", "Ultima Edad Registrada", "Total_Amputaciones",
    'AMP_DEDO_MANO','AMP_PULGAR','AMP_DEDO_PIE','AMP_A_NIVEL_PIE','DESART_TOBILLO',
    'AMP_NIVEL_MALEOLO','AMP_DEBAJO_RODILLA','DESART_RODILLA','AMP_ENCIMA_RODILLA'
]

# Variables de amputación
AMPUT_VARS = [
    'AMP_DEDO_MANO','AMP_PULGAR','AMP_DEDO_PIE','AMP_A_NIVEL_PIE','DESART_TOBILLO',
    'AMP_NIVEL_MALEOLO','AMP_DEBAJO_RODILLA','DESART_RODILLA','AMP_ENCIMA_RODILLA'
]

def canon(s):
    s = '' if s is None else str(s)
    s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c)!='Mn')
    s = re.sub(r'[^0-9a-zA-Z\s]', ' ', s).lower()
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def normalize_col(s):
    t = ''.join(ch for ch in unicodedata.normalize('NFD', str(s)) if unicodedata.category(ch) != 'Mn')
    t = t.lower()
    return re.sub(r'[^0-9a-z]', '', t)

def map_expected_columns(df, expected_list):
    norm_actual = {normalize_col(c): c for c in df.columns}
    mapping = {name: norm_actual.get(normalize_col(name), None) for name in expected_list}
    miss = [k for k,v in mapping.items() if v is None]
    if miss: print("Aviso: no se encontraron columnas ->", miss)
    return mapping

def detectar_col_provincia(columns):
    candidatos = ['PROVINCIA','Provincia','provincia','NOM_PROV','NOM_PROVIN','NOM_PROVINCIA','PROV_NOM','PROVINC','PROV_NAME','NAME_2']
    for c in candidatos:
        if c in columns: return c
    for c in columns:
        if 'prov' in c.lower(): return c
    return None

# -------- FILTRO ROBUSTO (bordes + fallback por cercanía en metros) --------
def filtrar_llanquihue(df, target_prov=None, tol_m=250):
    if target_prov is None:
        target_prov = TARGET_PROV

    if 'lat' not in df.columns or 'lng' not in df.columns:
        raise ValueError("Faltan columnas 'lat' y/o 'lng'.")

    df = df.copy()
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lng'] = pd.to_numeric(df['lng'], errors='coerce')
    m_ok = df['lat'].notna() & df['lng'].notna()

    gdf_pts = gpd.GeoDataFrame(
        df.loc[m_ok].copy(),
        geometry=gpd.points_from_xy(df.loc[m_ok,'lng'], df.loc[m_ok,'lat']),
        crs='EPSG:4326'
    )

    prov = gpd.read_file(PROVINCIAS_SHP)
    prov = prov.to_crs('EPSG:4326') if prov.crs else prov.set_crs('EPSG:4326')
    col_prov = detectar_col_provincia(prov.columns)
    if col_prov is None:
        raise ValueError(f"No se identificó columna de provincia en: {list(prov.columns)}")

    # Repara geometrías
    try:
        prov['geometry'] = prov.geometry.buffer(0)
    except Exception:
        pass

    # Join principal (incluye borde)
    try:
        j1 = gpd.sjoin(gdf_pts, prov[[col_prov,'geometry']], how='left', predicate='intersects')
    except TypeError:
        j1 = gpd.sjoin(gdf_pts, prov[[col_prov,'geometry']], how='left', op='intersects')

    # Fallback por cercanía en metros para no asignados
    na_idx = j1[col_prov].isna()
    if na_idx.any():
        pts_m  = j1.to_crs(3857)
        prov_m = prov.to_crs(3857)
        try:
            jnear = gpd.sjoin_nearest(
                pts_m.loc[na_idx, ['geometry']],
                prov_m[[col_prov, 'geometry']],
                how='left',
                max_distance=tol_m,
                distance_col='_dist_m'
            )
            j1.loc[na_idx, col_prov] = jnear[col_prov].values
        except Exception:
            prov_buf = prov_m.copy()
            prov_buf['geometry'] = prov_buf.buffer(tol_m)
            try:
                j2 = gpd.sjoin(pts_m.loc[na_idx, ['geometry']], prov_buf[[col_prov,'geometry']],
                               how='left', predicate='intersects')
            except TypeError:
                j2 = gpd.sjoin(pts_m.loc[na_idx, ['geometry']], prov_buf[[col_prov,'geometry']],
                               how='left', op='intersects')
            j2 = j2.to_crs(4326)
            j1.loc[na_idx, col_prov] = j2[col_prov].values

    norm = lambda s: canon(s).upper()
    j1['__p__'] = j1[col_prov].map(norm)
    gdf_ll = j1[j1['__p__'] == norm(target_prov)].drop(columns=['__p__','index_right'], errors='ignore')
    if len(gdf_ll) == 0:
        raise ValueError(f"No hay puntos en '{target_prov}' (tol={tol_m}m).")

    prov_ll = prov[prov[col_prov].map(norm) == norm(target_prov)].to_crs('EPSG:4326')

    comunas = gpd.read_file(COMUNAS_SHP)
    comunas = comunas.to_crs('EPSG:4326') if comunas.crs else comunas.set_crs('EPSG:4326')
    try:
        comunas_ll = gpd.sjoin(comunas, prov_ll[['geometry']], how='inner', predicate='intersects').drop(columns=['index_right'])
    except TypeError:
        comunas_ll = gpd.sjoin(comunas, prov_ll[['geometry']], how='inner', op='intersects').drop(columns=['index_right'])

    return gdf_ll.to_crs('EPSG:4326'), prov_ll.to_crs('EPSG:4326'), comunas_ll.to_crs('EPSG:4326')

# ---- Load + spatial filter ----
df_raw = pd.read_excel(FILE_PATH, sheet_name=SHEET)
colmap = map_expected_columns(df_raw, EXPECTED_ALL)
df_std = df_raw.rename(columns={colmap[k]: k for k in EXPECTED_ALL if colmap.get(k)})

gdf_ll, prov_ll, comunas_ll = filtrar_llanquihue(df_std)

# ---- Features (solo solicitadas) ----
present = [v for v in CLUSTER_VARS if v in gdf_ll.columns]
if len(present) < 3:
    raise ValueError(f"Variables disponibles insuficientes: {present}")
df_feat = pd.DataFrame(gdf_ll.drop(columns='geometry'))[present].copy()

# Tipos (quedarán vacíos si no están en df_feat; está bien)
cat_cols  = ["Sexo (Desc)"] if "Sexo (Desc)" in df_feat.columns else []
cont_cols = [c for c in ["tiempo (minutos)", "km", "Numero de registros", "Ultima Edad Registrada"] if c in df_feat.columns]
bin_cols  = [c for c in ["Mayor","Menor","Moderada","Total_Amputaciones"] if c in df_feat.columns]

# Preprocesamiento
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

# ---- OPTICS ----
opt = OPTICS(min_samples=MIN_SAMPLES, xi=XI, min_cluster_size=MIN_CLUSTER_SIZE, metric=METRIC)
labels = opt.fit_predict(X)   # -1 = ruido

# --------- NUEVA VARIABLE: Grupo (ruido vs cluster N) ---------
gdf_all = gdf_ll.copy()
gdf_all['cluster_OPTICS'] = labels
gdf_all['Grupo'] = np.where(
    gdf_all['cluster_OPTICS'] == -1, 'ruido',
    'cluster ' + gdf_all['cluster_OPTICS'].astype(int).astype(str)
)

df_out = pd.DataFrame(gdf_all.drop(columns='geometry')).copy()

print("\n[Grupo] Distribución por Grupo:")
print(gdf_all['Grupo'].value_counts().sort_index().to_string())

# ================== SALIDAS ==================
print("\n=== RESULTADOS OPTICS (alternativa a DBSCAN) ===")
valid_labels = labels[labels != -1]
n_clusters = len(np.unique(valid_labels)) if valid_labels.size > 0 else 0
n_noise = int(np.sum(labels == -1))
noise_ratio = n_noise / len(labels) if len(labels) else np.nan
print(f"1) Número de clusters encontrados (excluye ruido): {n_clusters}")
print(f"   · Puntos de ruido (-1): {n_noise}  | Proporción de ruido: {noise_ratio:.3f}")

# Métricas internas (solo clusters válidos)
if valid_labels.size > 1 and n_clusters >= 2:
    sil = silhouette_score(X[labels!=-1], labels[labels!=-1], metric=METRIC)
    print(f"2) Silhouette (sin ruido, métrica={METRIC}): {sil:.3f}")
    dbi = davies_bouldin_score(X[labels!=-1], labels[labels!=-1])
    print(f"   Davies–Bouldin (más bajo mejor): {dbi:.3f}")
    ch = calinski_harabasz_score(X[labels!=-1], labels[labels!=-1])
    print(f"   Calinski–Harabasz (más alto mejor): {ch:.1f}")
    D_valid = squareform(pdist(X[labels!=-1], metric=METRIC))
    dunn = dunn_index_from_square(D_valid, labels[labels!=-1])
    dunn_str = "inf" if np.isinf(dunn) else ("nan" if np.isnan(dunn) else f"{dunn:.3f}")
    print(f"   Dunn (más alto mejor, métrica={METRIC}): {dunn_str}")
else:
    print("2) Silhouette / DB / CH / Dunn: no aplican (clusters insuficientes tras excluir ruido)")

# (3) Tablas resumen (EXCLUYE ruido para métricas por cluster)
print("\n3.a) Tamaño por cluster (incluye ruido):")
sizes_all = df_out['cluster_OPTICS'].value_counts().sort_index()
for k_, v_ in sizes_all.items():
    tag = " (ruido)" if k_ == -1 else ""
    print(f"  - Etiqueta {k_}{tag}: {v_} filas")

df_valid = df_out[df_out['cluster_OPTICS'] != -1].copy()
if df_valid.empty or n_clusters == 0:
    print("\n[Resumen] No hay clusters válidos (todo fue marcado como ruido).")
else:
    excluir = {'cluster_OPTICS', 'Sexo (Desc)', 'tiempo (minutos)', 'km', 'Grupo'}
    num_all = [c for c in df_valid.columns if c not in excluir]
    for c in num_all:
        df_valid[c] = pd.to_numeric(df_valid[c], errors='coerce')

    means = df_valid.groupby('cluster_OPTICS')[num_all].mean().round(4)
    print("\n3.b) Promedios por cluster (media, 4 decimales) [sin 'tiempo' ni 'km']:")
    print(means.to_string())

    def iqr(s):
        q1, q3 = np.nanpercentile(s, [25, 75]); return q3 - q1
    def cv(s):
        m = np.nanmean(s); sd = np.nanstd(s, ddof=0); return np.nan if m==0 or np.isnan(m) else sd/m

    agg_std = df_valid.groupby('cluster_OPTICS')[num_all].std(ddof=0).round(4)
    agg_iqr = df_valid.groupby('cluster_OPTICS')[num_all].agg(iqr).round(4)
    agg_cv  = df_valid.groupby('cluster_OPTICS')[num_all].agg(cv).round(4)

    print("\n3.c) Variabilidad — Desviación estándar por cluster:")
    print(agg_std.to_string())
    print("\n3.c) Variabilidad — IQR por cluster:")
    print(agg_iqr.to_string())
    print("\n3.c) Variabilidad — Coeficiente de Variación (CV) por cluster:")
    print(agg_cv.to_string())

    grp = df_valid.groupby('cluster_OPTICS')[num_all]
    sk = grp.skew(numeric_only=True).round(3)
    ku = grp.apply(lambda g: g.kurt(numeric_only=True)).round(3)
    sk.columns = pd.MultiIndex.from_product([sk.columns, ['skew']])
    ku.columns = pd.MultiIndex.from_product([ku.columns, ['kurtosis']])
    sk_kurt = pd.concat([sk, ku], axis=1).sort_index(axis=1)
    print("\n3.e) Asimetría (skew) y Curtosis por cluster:")
    print(sk_kurt.to_string())

    print("\n3.f) Distancias internas por cluster (métrica: %s):" % METRIC)
    internal_rows = []
    X_no_noise = X[labels!=-1]; lab_no_noise = labels[labels!=-1]
    for cid in sorted(df_valid['cluster_OPTICS'].unique()):
        idx = np.where(lab_no_noise == cid)[0]
        n_i = idx.size
        if n_i >= 2:
            Dij = pdist(X_no_noise[idx], metric=METRIC)
            mean_intra = float(np.mean(Dij)); max_intra = float(np.max(Dij))
        else:
            mean_intra, max_intra = np.nan, np.nan
        internal_rows.append({'cluster': int(cid), 'n': int(n_i),
                              'mean_intra_dist': round(mean_intra, 4) if np.isfinite(mean_intra) else np.nan,
                              'max_intra_dist' : round(max_intra, 4)  if np.isfinite(max_intra)  else np.nan})
    print(pd.DataFrame(internal_rows).to_string(index=False))

    # Suma total de casos registrados por cluster
    if 'Numero de registros' in df_valid.columns:
        casos_sum = (df_valid.groupby('cluster_OPTICS')['Numero de registros']
                            .sum().astype(float).round(0).astype(int))
        print("\n3.g) Suma total de casos registrados (Numero de registros) por cluster:")
        print(casos_sum.to_string())

    # Suma de conteos por cluster (ancha)
    conteo_cols = [c for c in [
        'Numero de registros','Mayor','Menor','Moderada','Total_Amputaciones',
        'AMP_DEDO_MANO','AMP_PULGAR','AMP_DEDO_PIE','AMP_A_NIVEL_PIE','DESART_TOBILLO',
        'AMP_NIVEL_MALEOLO','AMP_DEBAJO_RODILLA','DESART_RODILLA','AMP_ENCIMA_RODILLA'
    ] if c in df_valid.columns]
    if len(conteo_cols) > 0:
        for c in conteo_cols:
            df_valid[c] = pd.to_numeric(df_valid[c], errors='coerce')
        sum_counts = (df_valid.groupby('cluster_OPTICS')[conteo_cols]
                             .sum().astype(float).round(0))
        sum_counts.index.name = 'cluster_OPTICS'
        print("\n3.g) Suma de conteos por cluster [formato ancho, sin 'tiempo' ni 'km']:")
        print(sum_counts.to_string())

    # 3.h) Asociación — Correlación de Spearman (por cluster) + Correlograma
    print("\n3.h) Asociación — Correlación de Spearman (por cluster) + correlograma:")
    base_corr_cols = list(corr_var)

    for k_ in sorted(df_valid['cluster_OPTICS'].unique()):
        sub = df_valid[df_valid['cluster_OPTICS'] == k_][base_corr_cols]
        n_vars = sub.shape[1]
        n_rows_validas = (~sub.isna()).any(axis=1).sum()
        if n_vars < 2 or n_rows_validas < 2:
            print(f"\n  > Cluster {k_}: insuficiente para correlación (variables={n_vars}, filas con datos={n_rows_validas}).")
            continue

        corr = sub.corr(method='spearman')
        print(f"\n  > Cluster {k_} — matriz Spearman:")
        print(corr.round(2).to_string())

        plot_correlogram(corr, title=f"Correlograma (Spearman) — Cluster {k_}")

# ---------- GRÁFICOS: SOLO VARIABLES DE AMPUTACIÓN (clusters y ruido) ----------
AMPUT_VARS_PRESENT = [c for c in AMPUT_VARS if c in gdf_all.columns]

def _without_total_amput(cols):
    bad = {'total_amputaciones', 'total amputaciones'}
    return [c for c in cols if str(c).strip().lower() not in bad]

if len(AMPUT_VARS_PRESENT) == 0:
    print("\n[Amputaciones] No hay variables de amputación presentes en la base.")
else:
    def _denom_total(df_sub, sums_amput):
        for cand in ['Total_Amputaciones', 'Total_amputaciones']:
            if cand in df_sub.columns:
                d = pd.to_numeric(df_sub[cand], errors='coerce').sum()
                if np.isfinite(d) and d > 0: return float(d), cand
        d = float(sums_amput.sum())
        return (d if d>0 else np.nan), "suma de amputaciones"

    # Clusters válidos (BARRAS SIN 'Total_Amputaciones')
    if n_clusters > 0:
        for cid in sorted(np.unique(valid_labels)):
            sub = gdf_all[gdf_all['cluster_OPTICS']==cid]
            if sub.empty: continue

            cols_plot = _without_total_amput(AMPUT_VARS_PRESENT)
            df_num = sub[cols_plot].apply(pd.to_numeric, errors='coerce').fillna(0)

            sums = df_num.sum()
            denom, ref_txt = _denom_total(sub, sums)
            perc = (sums/denom*100.0).clip(lower=0, upper=100) if np.isfinite(denom) and denom>0 else pd.Series([np.nan]*len(sums), index=sums.index)

            x = np.arange(len(sums))
            fig, ax1 = plt.subplots(figsize=(12,5))
            bars = ax1.bar(x, sums.values)
            ax1.set_xticks(x); ax1.set_xticklabels(sums.index, rotation=45, ha='right')
            ax1.set_ylabel('Suma de conteos')
            ax1.set_title(f'Amputaciones | Cluster {cid} , Total amputaciones = {int(denom) if np.isfinite(denom) else 0}')
            ax1.grid(axis='y', linestyle='--', alpha=0.4)
            ax2 = ax1.twinx(); ax2.set_ylim(0,105); ax2.set_ylabel('% sobre el total del cluster')
            for b, v, p in zip(bars, sums.values, perc.values):
                txt = f'{int(v)} ({p:.1f}%)' if np.isfinite(p) else f'{int(v)}'
                ax1.text(b.get_x()+b.get_width()/2, b.get_height(), txt, ha='center', va='bottom', fontsize=9)
            plt.tight_layout(); plt.show()

    # Ruido (-1) (BARRAS SIN 'Total_Amputaciones')
    subN = gdf_all[gdf_all['cluster_OPTICS']==-1]
    if subN.empty:
        print("\n[Ruido] No hay puntos etiquetados como ruido (-1).")
    else:
        cols_plotN = _without_total_amput(AMPUT_VARS_PRESENT)
        df_numN = subN[cols_plotN].apply(pd.to_numeric, errors='coerce').fillna(0)

        sumsN = df_numN.sum()
        denomN, ref_txtN = _denom_total(subN, sumsN)
        percN = (sumsN/denomN*100.0).clip(lower=0, upper=100) if np.isfinite(denomN) and denomN>0 else pd.Series([np.nan]*len(sumsN), index=sumsN.index)

        x = np.arange(len(sumsN))
        fig, ax1 = plt.subplots(figsize=(12,5))
        bars = ax1.bar(x, sumsN.values)
        ax1.set_xticks(x); ax1.set_xticklabels(sumsN.index, rotation=45, ha='right')
        ax1.set_ylabel('Suma de conteos')
        ax1.set_title(f'Amputaciones | Ruido, Total amputaciones ={int(denomN) if np.isfinite(denomN) else 0}')
        ax1.grid(axis='y', linestyle='--', alpha=0.4)
        ax2 = ax1.twinx(); ax2.set_ylim(0,105); ax2.set_ylabel('% sobre el total (ruido)')
        for b, v, p in zip(bars, sumsN.values, percN.values):
            txt = f'{int(v)} ({p:.1f}%)' if np.isfinite(p) else f'{int(v)}'
            ax1.text(b.get_x()+b.get_width()/2, b.get_height(), txt, ha='center', va='bottom', fontsize=9)
        plt.tight_layout(); plt.show()

# ==== RESÚMENES EXTRA: GLOBAL y RUIDO (-1) ==================================================
def resumen_y_correlograma(df_src: pd.DataFrame, titulo: str):
    """
    Imprime tablas resumen (promedio, std, CV, sumas, skew, kurtosis)
    y dibuja el correlograma Spearman (sin tiempo/km).
    """
    if df_src.empty:
        print(f"\n[{titulo}] Sin datos.")
        return

    excluir = {'cluster_OPTICS','Sexo (Desc)','tiempo (minutos)','km','Grupo'}
    num_cols = [c for c in df_src.columns if c not in excluir]
    df_num = df_src.copy()
    for c in num_cols:
        df_num[c] = pd.to_numeric(df_num[c], errors='coerce')

    mean_s = df_num[num_cols].mean(numeric_only=True)
    std_s  = df_num[num_cols].std(ddof=0, numeric_only=True)

    def _cv_series(m, s):
        cv_vals = []
        for col in m.index:
            mu = m[col]; sd = s[col]
            cv_vals.append(np.nan if (not np.isfinite(mu) or mu==0) else sd/mu)
        return pd.Series(cv_vals, index=m.index)

    cv_s = _cv_series(mean_s, std_s)

    print(f"\n[{titulo}] Promedio (media):")
    print(mean_s.round(4).to_string())
    print(f"\n[{titulo}] Desviación estándar:")
    print(std_s.round(4).to_string())
    print(f"\n[{titulo}] Coeficiente de variación (CV):")
    print(cv_s.round(4).to_string())

    conteo_cols = [c for c in [
        'Numero de registros','Mayor','Menor','Moderada','Total_Amputaciones',
        'AMP_DEDO_MANO','AMP_PULGAR','AMP_DEDO_PIE','AMP_A_NIVEL_PIE','DESART_TOBILLO',
        'AMP_NIVEL_MALEOLO','AMP_DEBAJO_RODILLA','DESART_RODILLA','AMP_ENCIMA_RODILLA'
    ] if c in df_num.columns]
    if len(conteo_cols) > 0:
        for c in conteo_cols:
            df_num[c] = pd.to_numeric(df_num[c], errors='coerce')
        sum_s = df_num[conteo_cols].sum().astype(float).round(0)
        print(f"\n[{titulo}] Suma de conteos:")
        print(sum_s.to_string())

    skew_s = df_num[num_cols].skew(numeric_only=True)
    kurt_s = df_num[num_cols].kurt(numeric_only=True)
    print(f"\n[{titulo}] Asimetría (skew):")
    print(skew_s.round(3).to_string())
    print(f"\n[{titulo}] Curtosis:")
    print(kurt_s.round(3).to_string())

    cols_corr = [c for c in corr_var if c in df_num.columns]
    sub = df_num[cols_corr]
    n_vars = sub.shape[1]
    n_rows_validas = (~sub.isna()).any(axis=1).sum()
    if n_vars < 2 or n_rows_validas < 2:
        print(f"\n[{titulo}] Correlograma: datos insuficientes (variables={n_vars}, filas con datos={n_rows_validas}).")
        return
    corr = sub.corr(method='spearman')
    print(f"\n[{titulo}] Matriz Spearman:")
    print(corr.round(2).to_string())
    plot_correlogram(corr, title=f"Correlograma (Spearman) — {titulo}")

# --- Llamadas: GLOBAL y RUIDO ---
resumen_y_correlograma(df_out, "Global (todos los datos)")
resumen_y_correlograma(df_out[df_out['cluster_OPTICS']==-1], "Ruido (-1)")
# ==============================================================================================

# (4) MAPA (clusters válidos y ruido)
if all(c in gdf_ll.columns for c in ['lat','lng']):
    gmap = gdf_ll.copy()
    gmap['cluster_OPTICS'] = labels
    uniq = np.sort(np.unique(labels[labels!=-1]))
    cmap = plt.cm.get_cmap('tab20', max(len(uniq), 3))
    colors = {c: cmap(i % 20) for i, c in enumerate(uniq)}

    fig, ax = plt.subplots(1,1, figsize=(10,10))
    prov_ll.boundary.plot(ax=ax, linewidth=1.5, color='black')
    comunas_ll.boundary.plot(ax=ax, linewidth=0.8, color='gray')

    if (gmap['cluster_OPTICS']==-1).any():
        gmap[gmap['cluster_OPTICS']==-1].plot(ax=ax, color='black', markersize=18, alpha=0.9,
                                              marker='x', label='Ruido (-1)')

    for c in uniq:
        msk = gmap['cluster_OPTICS'] == c
        gmap[msk].plot(ax=ax, color=colors[c], markersize=28, alpha=0.9,
                       marker='o', edgecolor='black', linewidth=0.3, label=f'Cluster {c}')

    if len(uniq) > 0:
        cent = gmap[gmap['cluster_OPTICS']!=-1].groupby('cluster_OPTICS')[['lng','lat']].mean()
        ax.scatter(cent['lng'], cent['lat'], s=260, marker='X', color='white',
                   edgecolor='black', linewidth=1.5, zorder=5)
        for cid, row in cent.iterrows():
            ax.text(row['lng'], row['lat'], str(cid), ha='center', va='center', color='black',
                    path_effects=[pe.withStroke(linewidth=2.5, foreground='white')],
                    fontsize=11, fontweight='bold', zorder=6)

    ax.set_title(f"Mapa: OPTICS (métrica={METRIC}) — Llanquihue")
    ax.set_axis_off()
    ax.legend(loc='lower left', frameon=True, fontsize=9, title='Etiqueta', markerscale=1.2)
    plt.tight_layout(); plt.show()
else:
    print("\n[Mapa] No se encontraron columnas 'lat'/'lng' para dibujar el mapa.")

# ================== (NUEVO) RESÚMENES Y GRÁFICOS DE 'tiempo (minutos)' y 'km' ==================
VARS_TK = [v for v in ["tiempo (minutos)", "km"] if v in df_out.columns]
if len(VARS_TK) == 0:
    print("\n[Tiempo/Km] No se encontraron las columnas 'tiempo (minutos)' ni 'km'.")
else:
    # Convertir a numérico (sobre la base completa etiquetada)
    for v in VARS_TK:
        df_out[v] = pd.to_numeric(df_out[v], errors='coerce')

    # ---- 1) Tablas resumen por cluster (excluye ruido)
    df_cl = df_out[df_out['cluster_OPTICS'] != -1].copy()
    if df_cl.empty or n_clusters == 0:
        print("\n[Tiempo/Km] No hay clusters válidos para generar tablas resumen (sección 1).")
    else:
        for v in VARS_TK:
            desc = df_cl.groupby('cluster_OPTICS')[v].agg(
                n='count',
                n_missing=lambda s: s.isna().sum(),
                mean='mean',
                std=lambda s: s.std(ddof=1),
                cv=lambda s: (s.std(ddof=1)/s.mean()) if (np.isfinite(s.mean()) and s.mean()!=0) else np.nan,
                skew=lambda s: s.skew(),
                kurtosis=lambda s: s.kurt(),
                p05=lambda s: np.nanpercentile(s, 5),
                p25=lambda s: np.nanpercentile(s, 25),
                p50='median',
                p75=lambda s: np.nanpercentile(s, 75),
                p95=lambda s: np.nanpercentile(s, 95),
                min='min',
                max='max'
            ).round(3)
            cols_order = ['n','n_missing','mean','std','cv','skew','kurtosis',
                          'p05','p25','p50','p75','p95','min','max']
            desc = desc[[c for c in cols_order if c in desc.columns]]
            print(f"\n[1] Resumen por cluster — {v} (sin ruido)")
            print(desc.to_string())

    # ---- 2) Tablas resumen del ruido
    df_noise = df_out[df_out['cluster_OPTICS'] == -1].copy()
    if df_noise.empty:
        print("\n[2] Resumen del ruido: no hay observaciones etiquetadas como ruido (-1).")
    else:
        for v in VARS_TK:
            s_all = pd.to_numeric(df_noise[v], errors='coerce'); s = s_all.dropna()
            mu  = np.nanmean(s) if s.size else np.nan
            std = np.nanstd(s, ddof=1) if s.size>1 else np.nan
            cv  = (std/mu) if (np.isfinite(std) and np.isfinite(mu) and mu!=0) else np.nan
            row = {
                'n': int(s.size),
                'n_missing': int(s_all.isna().sum()),
                'mean': mu, 'std': std, 'cv': cv,
                'skew': s.skew() if s.size>2 else np.nan,
                'kurtosis': s.kurt() if s.size>3 else np.nan,
                'p05': np.nanpercentile(s, 5) if s.size>0 else np.nan,
                'p25': np.nanpercentile(s, 25) if s.size>0 else np.nan,
                'p50': np.nanmedian(s) if s.size>0 else np.nan,
                'p75': np.nanpercentile(s, 75) if s.size>0 else np.nan,
                'p95': np.nanpercentile(s, 95) if s.size>0 else np.nan,
                'min': np.nanmin(s) if s.size>0 else np.nan,
                'max': np.nanmax(s) if s.size>0 else np.nan
            }
            print(f"\n[2] Resumen ruido (-1) — {v}")
            print(pd.DataFrame([row], index=['ruido']).round(3).to_string())

    # ---- 3) Boxplots (clusters)
    if df_cl.empty or n_clusters == 0:
        print("\n[3] Boxplots (clusters): no hay clusters válidos.")
    else:
        for v in VARS_TK:
            data = [df_cl[df_cl['cluster_OPTICS']==cid][v].dropna().values
                    for cid in sorted(df_cl['cluster_OPTICS'].unique())]
            labels_bp = [f"C{cid}" for cid in sorted(df_cl['cluster_OPTICS'].unique())]
            plt.figure(figsize=(8,5))
            plt.boxplot(data, showfliers=True, labels=labels_bp)
            plt.title(f"Boxplot por cluster — {v}")
            plt.xlabel("Cluster")
            plt.ylabel(v)
            plt.grid(axis='y', linestyle='--', alpha=0.4)
            plt.tight_layout(); plt.show()

    # ---- 4) Boxplots (ruido)
    if df_noise.empty:
        print("\n[4] Boxplots (ruido): no hay ruido (-1).")
    else:
        for v in VARS_TK:
            vals = df_noise[v].dropna().values
            if vals.size == 0:
                print(f"\n[4] Boxplot (ruido) — {v}: sin datos no nulos.")
                continue
            plt.figure(figsize=(5,5))
            plt.boxplot([vals], labels=['ruido'])
            plt.title(f"Boxplot — {v} (solo ruido)")
            plt.ylabel(v)
            plt.grid(axis='y', linestyle='--', alpha=0.4)
            plt.tight_layout(); plt.show()

    # ---- 5) BARRAS MÚLTIPLES (clusters): reemplaza histogramas superpuestos
    if df_cl.empty or n_clusters == 0:
        print("\n[5] Barras múltiples (clusters): no hay clusters válidos.")
    else:
        for v in VARS_TK:
            # Bins comunes usando TODOS los datos (clusters + ruido) para comparabilidad
            all_vals = df_out[v].dropna().values
            if all_vals.size == 0:
                print(f"\n[5] Barras múltiples — {v}: sin datos.")
                continue
            bins = np.histogram_bin_edges(all_vals, bins='auto')
            # Limitar número de bins si es excesivo
            if len(bins) - 1 > 14:
                bins = np.linspace(np.min(all_vals), np.max(all_vals), 15)

            clusters = sorted(df_cl['cluster_OPTICS'].unique())
            counts_by_cluster = {}
            for cid in clusters:
                arr = df_cl[df_cl['cluster_OPTICS']==cid][v].dropna().values
                if arr.size == 0:
                    counts = np.zeros(len(bins)-1, dtype=int)
                else:
                    counts, _ = np.histogram(arr, bins=bins)
                counts_by_cluster[cid] = counts

            # Plot de barras múltiples
            x = np.arange(len(bins)-1)  # una barra por bin (por cluster)
            g = len(clusters)
            width = 0.8 / max(g,1)
            fig, ax = plt.subplots(figsize=(10,5))

            for i, cid in enumerate(clusters):
                pos = x - 0.4 + width/2 + i*width
                ax.bar(pos, counts_by_cluster[cid], width=width, label=f"C{cid}")

            # Etiquetas de bins
            bin_labels = [f"[{bins[i]:.2f}, {bins[i+1]:.2f})" for i in range(len(bins)-1)]
            ax.set_xticks(x)
            ax.set_xticklabels(bin_labels, rotation=45, ha='right')

            ax.set_title(f"Distribución por bins (barras múltiples) — {v}")
            ax.set_xlabel(v)
            ax.set_ylabel("Frecuencia")
            ax.grid(axis='y', linestyle='--', alpha=0.3)
            ax.legend(title='Cluster', fontsize=9, ncol=min(4, g))
            plt.tight_layout(); plt.show()

    # ---- 6) BARRAS (ruido): reemplaza histograma de ruido
    if df_noise.empty:
        print("\n[6] Barras (ruido): no hay ruido (-1).")
    else:
        for v in VARS_TK:
            vals_all = df_out[v].dropna().values
            if vals_all.size == 0:
                print(f"\n[6] Barras (ruido) — {v}: sin datos.")
                continue
            bins = np.histogram_bin_edges(vals_all, bins='auto')
            if len(bins) - 1 > 14:
                bins = np.linspace(np.min(vals_all), np.max(vals_all), 15)

            arr = df_noise[v].dropna().values
            if arr.size == 0:
                print(f"\n[6] Barras (ruido) — {v}: sin datos no nulos.")
                continue
            counts, _ = np.histogram(arr, bins=bins)

            x = np.arange(len(bins)-1)
            fig, ax = plt.subplots(figsize=(8,4))
            ax.bar(x, counts, width=0.8)
            bin_labels = [f"[{bins[i]:.2f}, {bins[i+1]:.2f})" for i in range(len(bins)-1)]
            ax.set_xticks(x)
            ax.set_xticklabels(bin_labels, rotation=45, ha='right')

            ax.set_title(f"Distribución por bins — {v} (solo ruido)")
            ax.set_xlabel(v); ax.set_ylabel("Frecuencia")
            ax.grid(axis='y', linestyle='--', alpha=0.3)
            plt.tight_layout(); plt.show()

# ================== (NUEVO) EXPORTAR Y DESCARGAR DIRECTO (SIN GUARDAR EN DRIVE) ==================
from datetime import datetime
STAMP = datetime.now().strftime('%Y%m%d_%H%M%S')
OUT_XLSX = f"/content/BASE_FINAL_con_grupo_{STAMP}.xlsx"
OUT_CSV  = f"/content/BASE_FINAL_con_grupo_{STAMP}.csv"

# Reordenar columnas: primero Grupo y cluster_OPTICS
cols = list(df_out.columns)
for k in ['Grupo', 'cluster_OPTICS']:
    if k in cols:
        cols.remove(k)
cols_export = (['Grupo', 'cluster_OPTICS'] + cols)
df_export = df_out[cols_export].copy()

# Guardar a Excel (hoja con datos + hoja resumen de tamaños) en /content
with pd.ExcelWriter(OUT_XLSX, engine='openpyxl') as writer:
    df_export.to_excel(writer, index=False, sheet_name='BASE_CON_GRUPO')
    resumen = (
        df_export['Grupo']
        .value_counts(dropna=False)
        .rename_axis('Grupo')
        .to_frame('n')
        .sort_index()
    )
    resumen.to_excel(writer, sheet_name='RESUMEN_GRUPOS')

# (Opcional) CSV en /content
df_export.to_csv(OUT_CSV, index=False, encoding='utf-8-sig')

print(f"[OK] Archivos creados en memoria local:")
print(f"  - Excel: {OUT_XLSX}")
print(f"  - CSV  : {OUT_CSV}")

# Disparar DESCARGA DIRECTA si estás en Google Colab
try:
    from google.colab import files
    files.download(OUT_XLSX)   # Si prefieres CSV, cambia a OUT_CSV
    print("[Descarga] Iniciada descarga del Excel.")
except Exception as e:
    print("[Descarga] No se inició descarga directa (¿no estás en Colab?).")
    print("Puedes descargar manualmente desde el panel de archivos en la izquierda (ruta /content).")
