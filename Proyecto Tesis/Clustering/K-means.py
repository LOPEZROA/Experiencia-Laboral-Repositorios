
# ---- Imports ----
import re, unicodedata, warnings
import numpy as np, pandas as pd, geopandas as gpd, matplotlib.pyplot as plt
from shapely.geometry import Point
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
from scipy.spatial.distance import pdist, squareform   # <-- para Dunn
import matplotlib.patheffects as pe
warnings.filterwarnings("ignore")
# ================== K-MEANS (variables solicitadas) + DUNN ==================
!pip -q install geopandas shapely pyproj fiona rtree openpyxl scikit-learn --upgrade
# ---- Helpers ----


# ---- Config ----
FILE_PATH = '/content/drive/MyDrive/HPM/Bases de trabajo/BASE PRUEBA.xlsx'
SHEET = 0
PROVINCIAS_SHP = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Provincias/Provincias.shp'
COMUNAS_SHP    = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Comunas/comunas.shp'
TARGET_PROV = 'LLANQUIHUE'
K = 2
RANDOM_STATE = 42

EXPECTED_ALL = [
    "Sexo (Desc)", "lat", "lng",
    "tiempo (minutos)", "km", "Numero de registros",
    "Mayor", "Menor", "Moderada",
    "Ultima Edad Registrada", "Total_Amputaciones","Comuna"
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
    t = ''.join(ch for ch in unicodedata.normalize('NFD', str(s)) if unicodedata.category(ch) != 'Mn')
    t = t.lower()
    return re.sub(r'[^0-9a-z]', '', t)

def map_expected_columns(df, expected_list):
    norm_actual = {normalize_col(c): c for c in df.columns}
    mapping = {}
    for name in expected_list:
        mapping[name] = norm_actual.get(normalize_col(name), None)
    missing = [k for k,v in mapping.items() if v is None]
    if missing:
        print("Aviso: no se encontraron columnas ->", missing)
    return mapping

def detectar_col_provincia(columns):
    candidatos = ['PROVINCIA','Provincia','provincia','NOM_PROV','NOM_PROVIN',
                  'NOM_PROVINCIA','PROV_NOM','PROVINC','PROV_NAME','NAME_2']
    for c in candidatos:
        if c in columns: return c
    for c in columns:
        if 'prov' in c.lower(): return c
    return None

def detectar_col_comuna(columns):
    """
    Trata de adivinar el nombre de columna que guarda el nombre de la comuna
    dentro del shapefile de comunas.
    """
    candidatos = [
        'COMUNA','Comuna','comuna',
        'NOM_COM','NOM_COMUNA','NOMBRE','NOM_COMU',
        'NAME','NAME_3','name','nom_com',
        'CUT_COM','CUT_COMUNA'  # a veces sólo viene código, lo dejamos al final
    ]
    for c in candidatos:
        if c in columns:
            return c
    # fallback heurístico
    for c in columns:
        if 'comun' in c.lower() or 'name' in c.lower():
            return c
    # si no encontramos nada, devolvemos la primera col de tipo object
    return None

def filtrar_llanquihue(df, target_prov=None, tol_m=250):
    """
    Filtra puntos que caen en la provincia objetivo (por defecto TARGET_PROV) usando:
      1) sjoin con predicate='intersects' (incluye borde)
      2) fallback sjoin_nearest (max_distance=tol_m metros) para casos rozando el límite
    Retorna: (gdf_ll, prov_ll, comunas_ll) en EPSG:4326.
    """
    import re, unicodedata, warnings
    import numpy as np, pandas as pd, geopandas as gpd
    warnings.filterwarnings("ignore")

    if target_prov is None:
        try:
            target_prov = TARGET_PROV
        except NameError:
            raise ValueError("Debes pasar target_prov o definir TARGET_PROV.")

    def canon_local(s):
        s = '' if s is None else str(s)
        s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c)!='Mn')
        s = re.sub(r'[^0-9a-zA-Z\s]', ' ', s).lower()
        s = re.sub(r'\s+', ' ', s).strip()
        return s

    def detectar_col_provincia_local(columns):
        candidatos = ['PROVINCIA','Provincia','provincia','NOM_PROV','NOM_PROVIN',
                      'NOM_PROVINCIA','PROV_NOM','PROVINC','PROV_NAME','NAME_2']
        for c in candidatos:
            if c in columns:
                return c
        for c in columns:
            if 'prov' in c.lower():
                return c
        return None

    # --- validaciones base ---
    if 'lat' not in df.columns or 'lng' not in df.columns:
        raise ValueError("Faltan columnas 'lat' y/o 'lng'.")

    df = df.copy()
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lng'] = pd.to_numeric(df['lng'], errors='coerce')

    # construimos gdf con coordenadas válidas
    m_ok = df['lat'].notna() & df['lng'].notna()
    gdf_pts = gpd.GeoDataFrame(
        df.loc[m_ok].copy(),
        geometry=gpd.points_from_xy(df.loc[m_ok, 'lng'], df.loc[m_ok, 'lat']),
        crs='EPSG:4326'
    )

    # --- PROVINCIAS ---
    prov = gpd.read_file(PROVINCIAS_SHP)
    prov = prov.set_crs('EPSG:4326') if prov.crs is None else prov.to_crs('EPSG:4326')

    col_prov = detectar_col_provincia_local(prov.columns)
    if col_prov is None:
        raise ValueError(f"No se identificó columna de provincia en: {list(prov.columns)}")

    try:
        prov['geometry'] = prov.geometry.buffer(0)
    except Exception:
        pass

    # join por intersección
    try:
        j1 = gpd.sjoin(gdf_pts, prov[[col_prov, 'geometry']], how='left', predicate='intersects')
    except TypeError:
        j1 = gpd.sjoin(gdf_pts, prov[[col_prov, 'geometry']], how='left', op='intersects')

    # fallback con nearest
    na_idx = j1[col_prov].isna()
    if na_idx.any():
        pts_m   = j1.to_crs(3857)
        prov_m  = prov.to_crs(3857)
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
                j2 = gpd.sjoin(pts_m.loc[na_idx, ['geometry']], prov_buf[[col_prov, 'geometry']],
                               how='left', predicate='intersects')
            except TypeError:
                j2 = gpd.sjoin(pts_m.loc[na_idx, ['geometry']], prov_buf[[col_prov, 'geometry']],
                               how='left', op='intersects')
            j2 = j2.to_crs(4326)
            j1.loc[na_idx, col_prov] = j2[col_prov].values

    # filtra provincia objetivo
    norm = lambda s: canon_local(s).upper()
    j1['__prov_norm__'] = j1[col_prov].map(norm)
    gdf_ll = j1[j1['__prov_norm__'] == norm(target_prov)].drop(columns=['__prov_norm__', 'index_right'], errors='ignore')
    if len(gdf_ll) == 0:
        raise ValueError(f"No hay puntos en la provincia '{target_prov}' tras aplicar intersección y fallback (tol={tol_m}m).")

    # subcapas de salida en 4326
    prov_ll = prov[prov[col_prov].map(norm) == norm(target_prov)].to_crs('EPSG:4326')

    # Comunas que intersectan esa provincia
    comunas = gpd.read_file(COMUNAS_SHP)
    comunas = comunas.set_crs('EPSG:4326') if comunas.crs is None else comunas.to_crs('EPSG:4326')
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

    gdf_ll     = gdf_ll.to_crs('EPSG:4326')
    prov_ll    = prov_ll.to_crs('EPSG:4326')
    comunas_ll = comunas_ll.to_crs('EPSG:4326')

    # Adjuntamos el nombre de comuna detectado (lo usaremos en el mapa)
    comuna_col = detectar_col_comuna(comunas_ll.columns)
    if comuna_col is None:
        # si no encontramos una columna "nombre comuna", creamos algo genérico
        comuna_col = "COMUNA_TMP"
        comunas_ll[comuna_col] = np.arange(len(comunas_ll)).astype(str)

    return gdf_ll, prov_ll, comunas_ll, comuna_col

# ---- Índice de Dunn ----
def dunn_index(X, labels):
    """
    Dunn = min_intercluster_distance / max_intracluster_diameter
    Devuelve (dunn, lista_clusters_singleton).
    Nota: si existe algún cluster con 1 punto, su diámetro es 0 y Dunn puede inflarse.
    """
    labels = np.asarray(labels)
    uniq = np.unique(labels)
    if uniq.size < 2:
        return np.nan, []

    D = squareform(pdist(X, metric='euclidean'))

    diameters = []
    singleton = []
    idx_by_c = {c: np.where(labels == c)[0] for c in uniq}

    for c, idx in idx_by_c.items():
        if idx.size <= 1:
            diameters.append(0.0)
            singleton.append(int(c))
        else:
            sub = D[np.ix_(idx, idx)]
            diameters.append(np.max(sub))
    max_diam = np.max(diameters) if len(diameters) else np.nan

    min_inter = np.inf
    ulist = list(uniq)
    for i in range(len(ulist)):
        for j in range(i+1, len(ulist)):
            a = idx_by_c[ulist[i]]
            b = idx_by_c[ulist[j]]
            if a.size == 0 or b.size == 0:
                continue
            m = D[np.ix_(a, b)].min()
            if m < min_inter:
                min_inter = m

    if not np.isfinite(min_inter):
        return np.nan, singleton
    if max_diam == 0:
        return np.inf, singleton
    return float(min_inter / max_diam), singleton

# ---- Load + spatial filter ----
df_raw = pd.read_excel(FILE_PATH, sheet_name=SHEET)
colmap = map_expected_columns(df_raw, EXPECTED_ALL)
df_std = df_raw.rename(columns={colmap[k]: k for k in EXPECTED_ALL if colmap.get(k)})

gdf_ll, prov_ll, comunas_ll, comuna_col = filtrar_llanquihue(df_std)

# ---- Dataset de features SOLO para clustering ----
present = [v for v in CLUSTER_VARS if v in gdf_ll.columns]
if len(present) < 3:
    raise ValueError(f"Variables disponibles insuficientes para clusterizar: {present}")

df_feat = pd.DataFrame(gdf_ll.drop(columns='geometry'))[present].copy()

# Tipos
cat_cols = [c for c in df_feat.columns if c == "Sexo (Desc)"]
cont_cols = [c for c in ["Numero de registros", "Ultima Edad Registrada"] if c in df_feat.columns]
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

# ---- KMeans ----
km = KMeans(n_clusters=K, n_init=10, random_state=RANDOM_STATE)
labels = km.fit_predict(X)

# ================== SALIDAS SOLICITADAS ==================
print("\n=== RESULTADOS K-Means (variables solicitadas) ===")
print(f"1) Número de clusters usados (K): {K}")

# (Silhouette + Davies–Bouldin + Calinski–Harabasz + Dunn)
if len(np.unique(labels)) >= 2:
    sil = silhouette_score(X, labels)
    print(f"2) Silhouette (sobre datos preprocesados): {sil:.3f}")

    dbi = davies_bouldin_score(X, labels)
    print(f"   Davies–Bouldin (más bajo mejor): {dbi:.3f}")

    ch = calinski_harabasz_score(X, labels)
    print(f"   Calinski–Harabasz (más alto mejor): {ch:.1f}")

    dunn, singletons = dunn_index(X, labels)
    if np.isinf(dunn):
        print(f"   Dunn (↑ mejor): inf  (posible cluster singleton con diámetro 0: {singletons})")
    else:
        print(f"   Dunn (↑ mejor): {dunn:.3f}" + (f"  [singletons: {singletons}]" if singletons else ""))
else:
    sil = np.nan
    print("2) Silhouette: no aplica (solo 1 cluster)")
    print("   Davies–Bouldin / CH / Dunn: no aplican (solo 1 cluster)")

# Añadir etiquetas y **anexar tiempo/km** para los resúmenes
df_out = df_feat.copy()
df_out['cluster_kmeans'] = labels

# ---- columnas extra para reportes (tiempo/km)
extra_cols = [c for c in ["tiempo (minutos)", "km"] if c in gdf_ll.columns]
for c in extra_cols:
    df_out[c] = pd.to_numeric(gdf_ll[c].values, errors='coerce')

# 3.a Tamaños
sizes = df_out['cluster_kmeans'].value_counts().sort_index()
print("\n3.a) Tamaño por cluster (n):")
for k_, v_ in sizes.items():
    print(f"  - Cluster {k_}: {v_} filas")

# 3.b Promedios
num_all = [c for c in df_out.columns if c not in ['cluster_kmeans', 'Sexo (Desc)']]
for c in num_all:
    df_out[c] = pd.to_numeric(df_out[c], errors='coerce')
means = df_out.groupby('cluster_kmeans')[num_all].mean().round(4)
print("\n3.b) Promedios por cluster (media, 4 decimales):")
print(means.to_string())

# 3.c Variabilidad
def iqr(s):
    q1, q3 = np.nanpercentile(s, [25, 75])
    return q3 - q1

def cv(s):
    m = np.nanmean(s)
    sd = np.nanstd(s, ddof=0)
    return np.nan if m==0 or np.isnan(m) else sd/m

agg_std = df_out.groupby('cluster_kmeans')[num_all].std(ddof=0).round(4)
agg_iqr = df_out.groupby('cluster_kmeans')[num_all].agg(iqr).round(4)
agg_cv  = df_out.groupby('cluster_kmeans')[num_all].agg(cv).round(4)

print("\n3.c) Variabilidad — Desviación estándar por cluster:")
print(agg_std.to_string())
print("\n3.c) Variabilidad — IQR por cluster:")
print(agg_iqr.to_string())
print("\n3.c) Variabilidad — Coeficiente de Variación (CV) por cluster:")
print(agg_cv.to_string())

# 3.d Spearman dentro de cada cluster
print("\n3.d) Asociación — Correlación de Spearman (por cluster):")
for k_ in sorted(df_out['cluster_kmeans'].unique()):
    sub = df_out[df_out['cluster_kmeans']==k_][num_all]
    corr = sub.corr(method='spearman')
    print(f"\n  > Cluster {k_} — matriz Spearman:")
    print(corr.round(2).to_string())

# Distribución de sexo (si existe)
if "Sexo (Desc)" in df_out.columns:
    print("\n3.d) Asociación — Distribución de Sexo (Desc) por cluster (%):")
    tmp = df_out.copy()
    tmp["Sexo (Desc)"] = tmp["Sexo (Desc)"].astype(str).str.strip().fillna("missing")
    sex_pct = (tmp.pivot_table(index='cluster_kmeans', columns='Sexo (Desc)', aggfunc=len, fill_value=0)
                  .apply(lambda r: r/r.sum()*100, axis=1).round(1))
    print(sex_pct.to_string())

# ---------------- MAPA geográfico con colores de comuna + puntos ----------------

if all(c in gdf_ll.columns for c in ['lat','lng']):
    gmap = gdf_ll.copy()
    gmap['cluster_kmeans'] = labels

    # Paleta por cluster (puntos)
    uniq = np.sort(np.unique(labels))
    cmap = plt.cm.get_cmap('tab10', max(len(uniq), 3))
    colors_pts = {c: cmap(i % 10) for i, c in enumerate(uniq)}

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

    # Creamos una columna con nombre normalizado de comuna para hacer el mapeo de color
    def norm_name(x):
        x = '' if x is None else str(x)
        x = ''.join(ch for ch in unicodedata.normalize('NFD', x) if unicodedata.category(ch) != 'Mn')
        x = x.lower().strip()
        return x

    comunas_ll["_comuna_norm_"] = comunas_ll[comuna_col].apply(norm_name)
    comunas_ll["__fillcolor__"] = comunas_ll["_comuna_norm_"].map(comuna_colors)

    fig, ax = plt.subplots(1,1, figsize=(10,10))

    # 1. Dibujamos comunas con su color definido (si no está en diccionario, gris claro)
    for _, row in comunas_ll.iterrows():
        fc = row["__fillcolor__"] if pd.notna(row["__fillcolor__"]) else "#dddddd"
        row_geometry = gpd.GeoSeries([row.geometry], crs=comunas_ll.crs)
        row_geometry.plot(ax=ax,
                          facecolor=fc,
                          edgecolor='gray',
                          linewidth=0.8,
                          alpha=1.0)

    # 2. Borde provincia más grueso
    prov_ll.boundary.plot(ax=ax, linewidth=1.5, color='black')

    # 3. Puntos de los clusters (más translúcidos para notar la superposición)
    for c in uniq:
        msk = gmap['cluster_kmeans'] == c
        gmap[msk].plot(
            ax=ax,
            color=colors_pts[c],
            markersize=28,
            alpha=0.65,                # <= aquí bajamos opacidad de los puntos
            marker='o',
            edgecolor='black',
            linewidth=0.3,
            label=f'Cluster {c}',
            zorder=4
        )

    # 4. Centroides por cluster
    cent = gmap.groupby('cluster_kmeans')[['lng','lat']].mean()
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

    ax.set_title('Mapa: K-Means (Provincia de Llanquihue)')
    ax.set_axis_off()
    ax.legend(
        loc='lower left',
        frameon=True,
        fontsize=9,
        title='Cluster',
        markerscale=1.2
    )
    plt.tight_layout();plt.savefig("k-means.svg", format ="svg");plt.show()

else:
    print("\n[Mapa] No se encontraron columnas 'lat'/'lng' en los datos filtrados; no se puede dibujar el mapa.")
