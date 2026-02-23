# ================== K-MEDOIDS con distancia BRAY–CURTIS (+ DUNN) ==================
# (Alternativa correcta a K-Means con métricas no euclídeas)
!pip -q install pyclustering



# ---- Imports ----
import re, unicodedata, warnings
import numpy as np, pandas as pd, geopandas as gpd, matplotlib.pyplot as plt
from shapely.geometry import Point
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, RobustScaler, FunctionTransformer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
from scipy.spatial.distance import pdist, squareform
from pyclustering.cluster.kmedoids import kmedoids
import matplotlib.patheffects as pe
warnings.filterwarnings("ignore")

# ---- Config ----
FILE_PATH = '/content/drive/MyDrive/HPM/Bases de trabajo/BASE FINAL.xlsx'
SHEET = 0
PROVINCIAS_SHP = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Provincias/Provincias.shp'
COMUNAS_SHP    = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Comunas/comunas.shp'
TARGET_PROV = 'LLANQUIHUE'
K = 4
RANDOM_STATE = 42

# ---- Helpers ----
EXPECTED_ALL = [
    "Sexo (Desc)", "lat", "lng",
    "tiempo (minutos)", "km", "Numero de registros",
    "Mayor", "Menor", "Moderada",
    "Ultima Edad Registrada", "Total_Amputaciones"
]

CLUSTER_VARS = [
    "Numero de registros", "Ultima Edad Registrada",
    "Total_Amputaciones",
    'AMP_DEDO_MANO','AMP_PULGAR','AMP_DEDO_PIE',
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
    # normaliza nombres de columnas para hacer matching flexible
    t = ''.join(
        ch
        for ch in unicodedata.normalize('NFD', str(s))
        if unicodedata.category(ch) != 'Mn'
    )
    t = t.lower()
    return re.sub(r'[^0-9a-z]', '', t)

def map_expected_columns(df, expected_list):
    norm_actual = {normalize_col(c): c for c in df.columns}
    mapping = {name: norm_actual.get(normalize_col(name), None) for name in expected_list}
    missing = [k for k,v in mapping.items() if v is None]
    if missing:
        print("Aviso: no se encontraron columnas ->", missing)
    return mapping

def detectar_col_provincia(columns):
    candidatos = [
        'PROVINCIA','Provincia','provincia',
        'NOM_PROV','NOM_PROVIN','NOM_PROVINCIA',
        'PROV_NOM','PROVINC','PROV_NAME','NAME_2'
    ]
    for c in candidatos:
        if c in columns:
            return c
    for c in columns:
        if 'prov' in c.lower():
            return c
    return None

def detectar_col_comuna(columns):
    """
    Detecta la columna que contiene el nombre de la comuna en el shapefile.
    Retorna algún nombre sí o sí (fallback razonable).
    """
    candidatos = [
        'COMUNA','Comuna','comuna',
        'NOM_COM','NOM_COMUNA','NOMBRE','NOM_COMU',
        'NAME','NAME_3','name','nom_com',
        'CUT_COM','CUT_COMUNA'
    ]
    for c in candidatos:
        if c in columns:
            return c

    # heurística por nombre
    for c in columns:
        if 'comun' in c.lower() or 'name' in c.lower():
            return c

    # fallback final: la primera columna del GeoDataFrame
    return list(columns)[0]

# -------- FILTRO ROBUSTO (bordes + fallback por cercanía en metros) --------
def filtrar_llanquihue(df, target_prov=None, tol_m=250):
    """
    Retorna (gdf_ll, prov_ll, comunas_ll, comuna_col) en EPSG:4326.
    - Incluye puntos en el borde (predicate='intersects').
    - Repara geometrías inválidas con buffer(0).
    - Fallback: sjoin_nearest hasta tol_m metros (o buffer si no está disponible).
    - Detecta la columna que contiene el nombre de la comuna.
    """
    if target_prov is None:
        target_prov = TARGET_PROV

    # 1. Validación y armado de puntos
    if 'lat' not in df.columns or 'lng' not in df.columns:
        raise ValueError("Faltan columnas 'lat' y/o 'lng'.")
    df = df.copy()
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lng'] = pd.to_numeric(df['lng'], errors='coerce')
    m_ok = df['lat'].notna() & df['lng'].notna()
    gdf_pts = gpd.GeoDataFrame(
        df.loc[m_ok].copy(),
        geometry=gpd.points_from_xy(df.loc[m_ok, 'lng'], df.loc[m_ok, 'lat']),
        crs='EPSG:4326'
    )

    # 2. Provincias
    prov = gpd.read_file(PROVINCIAS_SHP)
    prov = prov.to_crs('EPSG:4326') if prov.crs else prov.set_crs('EPSG:4326')
    col_prov = detectar_col_provincia(prov.columns)
    if col_prov is None:
        raise ValueError(f"No se identificó columna de provincia en: {list(prov.columns)}")
    try:
        prov['geometry'] = prov.geometry.buffer(0)  # repara geometrías inválidas
    except Exception:
        pass

    # Join principal (borde incluido)
    try:
        j1 = gpd.sjoin(gdf_pts, prov[[col_prov,'geometry']], how='left', predicate='intersects')
    except TypeError:
        j1 = gpd.sjoin(gdf_pts, prov[[col_prov,'geometry']], how='left', op='intersects')

    # Fallback por cercanía para puntos no asignados
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
            # Respaldo con buffer si sjoin_nearest no existe
            prov_buf = prov_m.copy()
            prov_buf['geometry'] = prov_buf.buffer(tol_m)
            try:
                j2 = gpd.sjoin(
                    pts_m.loc[na_idx, ['geometry']],
                    prov_buf[[col_prov, 'geometry']],
                    how='left',
                    predicate='intersects'
                )
            except TypeError:
                j2 = gpd.sjoin(
                    pts_m.loc[na_idx, ['geometry']],
                    prov_buf[[col_prov, 'geometry']],
                    how='left',
                    op='intersects'
                )
            j2 = j2.to_crs(4326)
            j1.loc[na_idx, col_prov] = j2[col_prov].values

    # Normaliza y filtra provincia objetivo
    norm = lambda s: canon(s).upper()
    j1['__p__'] = j1[col_prov].map(norm)
    gdf_ll = j1[j1['__p__'] == norm(target_prov)].drop(columns=['__p__','index_right'], errors='ignore')
    if len(gdf_ll) == 0:
        raise ValueError(f"No hay puntos en '{target_prov}' (tol={tol_m}m).")

    prov_ll = prov[prov[col_prov].map(norm) == norm(target_prov)].to_crs('EPSG:4326')

    # 3. Comunas dentro de la provincia objetivo
    comunas = gpd.read_file(COMUNAS_SHP)
    comunas = comunas.to_crs('EPSG:4326') if comunas.crs else comunas.set_crs('EPSG:4326')
    try:
        comunas_ll = gpd.sjoin(
            comunas,
            prov_ll[['geometry']],
            how='inner',
            predicate='intersects'
        ).drop(columns=['index_right'])
    except TypeError:
        comunas_ll = gpd.sjoin(
            comunas,
            prov_ll[['geometry']],
            how='inner',
            op='intersects'
        ).drop(columns=['index_right'])
    comunas_ll = comunas_ll.to_crs('EPSG:4326')

    # Identificar columna del nombre de comuna
    comuna_col = detectar_col_comuna(comunas_ll.columns)

    return (
        gdf_ll.to_crs('EPSG:4326'),
        prov_ll.to_crs('EPSG:4326'),
        comunas_ll.to_crs('EPSG:4326'),
        comuna_col
    )

# ---- Dunn index (usando matriz de distancias cuadrada) ----
def dunn_index_from_square(D_square, labels):
    """
    Dunn = (mínima distancia inter-cluster) / (máximo diámetro intra-cluster).
    D_square: matriz NxN de distancias (Bray–Curtis en este caso).
    labels: vector de etiquetas de clusters (>=2 clusters requeridos).
    """
    labels = np.asarray(labels)
    uniq = np.unique(labels)
    if uniq.size < 2:
        return np.nan

    idx_by_c = {c: np.where(labels == c)[0] for c in uniq}

    # diámetro intra cluster
    diameters = []
    for idx in idx_by_c.values():
        if idx.size <= 1:
            diameters.append(0.0)
        else:
            diameters.append(np.max(D_square[np.ix_(idx, idx)]))
    max_diam = np.max(diameters) if len(diameters) else np.nan

    # mínima distancia inter cluster
    min_inter = np.inf
    ulist = list(uniq)
    for i in range(len(ulist)):
        for j in range(i+1, len(ulist)):
            a, b = idx_by_c[ulist[i]], idx_by_c[ulist[j]]
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

# ---- Load + filtro espacial ----
df_raw = pd.read_excel(FILE_PATH, sheet_name=SHEET)
colmap = map_expected_columns(df_raw, EXPECTED_ALL)
df_std = df_raw.rename(columns={colmap[k]: k for k in EXPECTED_ALL if colmap.get(k)})

gdf_ll, prov_ll, comunas_ll, comuna_col = filtrar_llanquihue(
    df_std,
    target_prov=TARGET_PROV,
    tol_m=250
)

# ---- Features (solo solicitadas) ----
present = [v for v in CLUSTER_VARS if v in gdf_ll.columns]
if len(present) < 3:
    raise ValueError(f"Variables disponibles insuficientes para clusterizar: {present}")
df_feat = pd.DataFrame(gdf_ll.drop(columns='geometry'))[present].copy()

# ---- Tipos ----
cat_cols = ["Sexo (Desc)"] if "Sexo (Desc)" in df_feat.columns else []
cont_cols = [c for c in ["tiempo (minutos)", "km", "Numero de registros", "Ultima Edad Registrada"] if c in df_feat.columns]
bin_cols  = [c for c in ["Mayor","Menor","Moderada","Total_Amputaciones"] if c in df_feat.columns]

# ---- Preprocesamiento (log1p + robust para continuas; OHE para categórica) ----
try:
    ohe = OneHotEncoder(handle_unknown='ignore', sparse_output=False)
except TypeError:
    ohe = OneHotEncoder(handle_unknown='ignore', sparse=False)

def log1p_nonneg(X):
    X = np.asarray(X, dtype=float)
    X[~np.isfinite(X)] = 0.0
    X[X < 0.0] = 0.0
    return np.log1p(X)

cont_pipeline = Pipeline([
    ('imp',  SimpleImputer(strategy='median')),
    ('logp', FunctionTransformer(log1p_nonneg, feature_names_out='one-to-one')),
    ('sc',   RobustScaler(with_centering=True, with_scaling=True))
])

ct = ColumnTransformer(
    transformers=[
        ('cont', cont_pipeline, cont_cols),
        ('bin',  Pipeline([('imp', SimpleImputer(strategy='most_frequent'))]), bin_cols),
        ('cat',  Pipeline([('imp', SimpleImputer(strategy='most_frequent')),
                           ('oh', ohe)]), cat_cols),
    ],
    remainder='drop'
)

X = ct.fit_transform(df_feat)

# ---- Distancias BRAY–CURTIS + K-MEDOIDS (pyclustering) ----
D = squareform(pdist(X, metric='braycurtis'))

# Semillas reproducibles
rng = np.random.RandomState(RANDOM_STATE)
initial_medoids = rng.choice(np.arange(X.shape[0]), size=K, replace=False).tolist()

kmed = kmedoids(D, initial_medoids, data_type='distance_matrix')
kmed.process()
clusters = kmed.get_clusters()
medoids  = kmed.get_medoids()

labels = -1 * np.ones(X.shape[0], dtype=int)
for cid, idxs in enumerate(clusters):
    labels[np.array(idxs, dtype=int)] = cid

# ================== SALIDAS ==================
print("\n=== RESULTADOS K-Medoids (BRAY–CURTIS) ===")
print(f"Clusters (K): {K} | Medoides (índices): {medoids}")

if len(np.unique(labels)) >= 2 and (labels >= 0).sum() > K:
    sil = silhouette_score(D, labels, metric='precomputed')
    print(f"Silhouette (Bray–Curtis): {sil:.3f}")

    dunn = dunn_index_from_square(D, labels)
    if np.isinf(dunn):
        print("Dunn: ∞ (hay clusters de tamaño 1 con separación alta)")
    elif np.isnan(dunn):
        print("Dunn: n/d (no se pudo calcular)")
    else:
        print(f"Dunn (Bray–Curtis; más alto mejor): {dunn:.3f}")

    dbi = davies_bouldin_score(X, labels)
    print(f"Davies–Bouldin (más bajo mejor): {dbi:.3f}")

    ch = calinski_harabasz_score(X, labels)
    print(f"Calinski–Harabasz (más alto mejor): {ch:.1f}")
else:
    print("Silhouette / Dunn / DB / CH: no aplican (clusters insuficientes)")

# Etiquetas al DF (alias doble por compatibilidad con otros scripts)
df_out = df_feat.copy()
df_out['cluster_kmedoids_braycurtis'] = labels
df_out['cluster_kmeans'] = labels  # alias opcional

# ---- Agregar tiempo/km al df_out para reportes
extra_cols = [c for c in ["tiempo (minutos)", "km"] if c in gdf_ll.columns]
for c in extra_cols:
    df_out[c] = pd.to_numeric(gdf_ll[c].values, errors='coerce')

# ---- Resúmenes por cluster ----
sizes = df_out['cluster_kmedoids_braycurtis'].value_counts().sort_index()
print("\nTamaño por cluster (n):")
for k_, v_ in sizes.items():
    print(f"  - Cluster {k_}: {v_} filas")

num_all = [c for c in df_out.columns if c not in ['cluster_kmedoids_braycurtis', 'cluster_kmeans', 'Sexo (Desc)']]
for c in num_all:
    df_out[c] = pd.to_numeric(df_out[c], errors='coerce')

means = df_out.groupby('cluster_kmedoids_braycurtis')[num_all].mean().round(4)
print("\nPromedios por cluster (media, 4 decimales):")
print(means.to_string())

def iqr(s):
    q1, q3 = np.nanpercentile(s, [25, 75])
    return q3 - q1

def cv(s):
    m = np.nanmean(s)
    sd = np.nanstd(s, ddof=0)
    return np.nan if m==0 or np.isnan(m) else sd/m

agg_std = df_out.groupby('cluster_kmedoids_braycurtis')[num_all].std(ddof=0).round(4)
agg_iqr = df_out.groupby('cluster_kmedoids_braycurtis')[num_all].agg(iqr).round(4)
agg_cv  = df_out.groupby('cluster_kmedoids_braycurtis')[num_all].agg(cv).round(4)

print("\nDesviación estándar por cluster:")
print(agg_std.to_string())
print("\nIQR por cluster:")
print(agg_iqr.to_string())
print("\nCoeficiente de Variación (CV) por cluster:")
print(agg_cv.to_string())

print("\nCorrelación de Spearman por cluster:")
for k_ in sorted(df_out['cluster_kmedoids_braycurtis'].unique()):
    sub = df_out[df_out['cluster_kmedoids_braycurtis']==k_][num_all]
    corr = sub.corr(method='spearman')
    print(f"\n  > Cluster {k_} — matriz Spearman:")
    print(corr.round(2).to_string())

if "Sexo (Desc)" in df_out.columns:
    print("\nDistribución de Sexo (Desc) por cluster (%):")
    tmp = df_out.copy()
    tmp["Sexo (Desc)"] = tmp["Sexo (Desc)"].astype(str).str.strip().fillna("missing")
    sex_pct = (tmp.pivot_table(
                    index='cluster_kmedoids_braycurtis',
                    columns='Sexo (Desc)',
                    aggfunc=len,
                    fill_value=0
               ).apply(lambda r: r/r.sum()*100, axis=1).round(1))
    print(sex_pct.to_string())

# ---------------- MAPA geográfico con colores de comuna + puntos ----------------
if all(c in gdf_ll.columns for c in ['lat','lng']):
    gmap = gdf_ll.copy()
    gmap['cluster_kmedoids_braycurtis'] = labels

    # Paleta por cluster para los puntos
    uniq = np.sort(np.unique(labels))
    cmap = plt.cm.get_cmap('tab10', max(len(uniq), 3))
    colors_pts = {c: cmap(i % 10) for i, c in enumerate(uniq)}

    # Forzar cluster 3 a verde
    if 3 in colors_pts:
        colors_pts[3] = "green"  # puedes cambiar a "#00cc44" si quieres más brillante

    # Paleta fija por comuna (polígonos)
    comuna_colors = {
        "puerto montt": "#fdecf5",
        "puerto varas": "#efefef",
        "calbuco": "#fadcdd",
        "llanquihue": "#ffebd9",
        "los muermos": "#ffffe0",
        "frutillar": "#efe4f1",
        "fresia": "#e4f3e4",
        "maullin": "#f1e5de",
        "cochamo": "#e1ebf4"
    }

    # Normalizar nombre comuna para mapear color
    def norm_name(x):
        x = '' if x is None else str(x)
        x = ''.join(ch for ch in unicodedata.normalize('NFD', x) if unicodedata.category(ch) != 'Mn')
        x = x.lower().strip()
        return x

    comunas_ll["_comuna_norm_"] = comunas_ll[comuna_col].apply(norm_name)
    comunas_ll["__fillcolor__"] = comunas_ll["_comuna_norm_"].map(comuna_colors)

    fig, ax = plt.subplots(1,1, figsize=(10,10))

    # 1. Dibujar comunas con su color asignado
    for _, row in comunas_ll.iterrows():
        fc = row["__fillcolor__"] if pd.notna(row["__fillcolor__"]) else "#dddddd"
        gpd.GeoSeries([row.geometry], crs=comunas_ll.crs).plot(
            ax=ax,
            facecolor=fc,
            edgecolor='gray',
            linewidth=0.8,
            alpha=1.0
        )

    # 2. Borde provincia más grueso
    prov_ll.boundary.plot(ax=ax, linewidth=1.5, color='black')

    # 3. Puntos de los clusters con transparencia para ver sobreposición
    for c in uniq:
        msk = gmap['cluster_kmedoids_braycurtis'] == c
        gmap[msk].plot(
            ax=ax,
            color=colors_pts[c],
            markersize=28,
            alpha=0.65,   # translucidez para ver densidad de puntos sobrepuestos
            marker='o',
            edgecolor='black',
            linewidth=0.3,
            label=f'Cluster {c}',
            zorder=4
        )

    # 4. "Centroides" promedio de lat/lng por cluster
    cent = gmap.groupby('cluster_kmedoids_braycurtis')[['lng','lat']].mean()
    ax.scatter(
        cent['lng'],
        cent['lat'],
        s=260,
        marker='X',
        color='white',
        edgecolor='black',
        linewidth=1.5,
        zorder=5
    )
    for cid, rowc in cent.iterrows():
        ax.text(
            rowc['lng'],
            rowc['lat'],
            str(cid),
            ha='center', va='center',
            color='black',
            path_effects=[pe.withStroke(linewidth=2.5, foreground='white')],
            fontsize=11,
            fontweight='bold',
            zorder=6
        )

    ax.set_title('Mapa: K-Medoids (Bray–Curtis) — Provincia de Llanquihue')
    ax.set_axis_off()
    ax.legend(
        loc='lower left',
        frameon=True,
        fontsize=9,
        title='Cluster',
        markerscale=1.2
    )
    plt.tight_layout()
    plt.savefig("k-medoid.svg", format ="svg")
    plt.show()

else:
    print("\n[Mapa] No se encontraron columnas 'lat'/'lng' en los datos filtrados; no se puede dibujar el mapa.")