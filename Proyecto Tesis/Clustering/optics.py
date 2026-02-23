# ================== OPTICS (alternativa a DBSCAN) + DUNN ==================
#!pip -q install geopandas shapely pyproj fiona rtree openpyxl scikit-learn --upgrade

# ---- Imports ----
import re, unicodedata, warnings
import numpy as np, pandas as pd, geopandas as gpd, matplotlib.pyplot as plt
from shapely.geometry import Point
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.cluster import OPTICS
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
from scipy.spatial.distance import pdist, squareform   # <-- para Dunn
import matplotlib.patheffects as pe
warnings.filterwarnings("ignore")

# ---- Config ----
FILE_PATH = '/content/drive/MyDrive/HPM/Bases de trabajo/BASE FINAL.xlsx'
SHEET = 0
PROVINCIAS_SHP = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Provincias/Provincias.shp'
COMUNAS_SHP    = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Comunas/comunas.shp'
TARGET_PROV = 'LLANQUIHUE'

# Hiperparámetros OPTICS
MIN_SAMPLES = 10         # vecinos mínimos
XI = 0.05                # más bajo => detecta más clusters
MIN_CLUSTER_SIZE = 97    # proporción (0–1) o entero
METRIC = 'braycurtis'    # distancia para clustering y métricas
RANDOM_STATE = 42


# ---- Dunn helper ----
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
    t = ''.join(
        ch
        for ch in unicodedata.normalize('NFD', str(s))
        if unicodedata.category(ch) != 'Mn'
    )
    return re.sub(r'[^0-9a-z]', '', t.lower())

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
    Trata de detectar el nombre de la comuna en el shapefile.
    Devuelve siempre algo razonable.
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

    # fallback: primera columna
    return list(columns)[0]

# -------- FILTRO ROBUSTO (bordes + fallback por cercanía en metros) --------
def filtrar_llanquihue(df, target_prov=None, tol_m=250):
    """
    Retorna (gdf_ll, prov_ll, comunas_ll, comuna_col) en EPSG:4326.
    - Incluye puntos en el borde (predicate='intersects').
    - Repara geometrías con buffer(0).
    - Fallback: sjoin_nearest hasta tol_m metros (o buffer si no está disponible).
    - Detecta la columna con el nombre de comuna para luego colorear el mapa.
    """
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

    # Provincias
    prov = gpd.read_file(PROVINCIAS_SHP)
    prov = prov.to_crs('EPSG:4326') if prov.crs else prov.set_crs('EPSG:4326')
    col_prov = detectar_col_provincia(prov.columns)
    if col_prov is None:
        raise ValueError(f"No se identificó columna de provincia en: {list(prov.columns)}")

    # Arreglar geometrías inválidas
    try:
        prov['geometry'] = prov.geometry.buffer(0)
    except Exception:
        pass

    # Join principal con provincia (incluye borde)
    try:
        j1 = gpd.sjoin(gdf_pts, prov[[col_prov,'geometry']], how='left', predicate='intersects')
    except TypeError:
        j1 = gpd.sjoin(gdf_pts, prov[[col_prov,'geometry']], how='left', op='intersects')

    # Fallback por cercanía en metros para puntos sin asignación
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
                j2 = gpd.sjoin(
                    pts_m.loc[na_idx, ['geometry']],
                    prov_buf[[col_prov,'geometry']],
                    how='left',
                    predicate='intersects'
                )
            except TypeError:
                j2 = gpd.sjoin(
                    pts_m.loc[na_idx, ['geometry']],
                    prov_buf[[col_prov,'geometry']],
                    how='left',
                    op='intersects'
                )
            j2 = j2.to_crs(4326)
            j1.loc[na_idx, col_prov] = j2[col_prov].values

    # Filtrar solo la provincia deseada
    norm = lambda s: canon(s).upper()
    j1['__p__'] = j1[col_prov].map(norm)
    gdf_ll = j1[j1['__p__'] == norm(target_prov)].drop(columns=['__p__','index_right'], errors='ignore')
    if len(gdf_ll) == 0:
        raise ValueError(f"No hay puntos en '{target_prov}' (tol={tol_m}m).")

    prov_ll = prov[prov[col_prov].map(norm) == norm(target_prov)].to_crs('EPSG:4326')

    # Comunas dentro de la provincia objetivo
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

    comuna_col = detectar_col_comuna(comunas_ll.columns)

    return (
        gdf_ll.to_crs('EPSG:4326'),
        prov_ll.to_crs('EPSG:4326'),
        comunas_ll.to_crs('EPSG:4326'),
        comuna_col
    )

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
    raise ValueError(f"Variables disponibles insuficientes: {present}")
df_feat = pd.DataFrame(gdf_ll.drop(columns='geometry'))[present].copy()

# Tipos
cat_cols  = ["Sexo (Desc)"] if "Sexo (Desc)" in df_feat.columns else []
cont_cols = [c for c in ["tiempo (minutos)", "km", "Numero de registros", "Ultima Edad Registrada"] if c in df_feat.columns]
bin_cols  = [c for c in ["Mayor","Menor","Moderada","Total_Amputaciones"] if c in df_feat.columns]

# Preprocesamiento (mismo criterio que DBSCAN)
try:
    ohe = OneHotEncoder(handle_unknown='ignore', sparse_output=False)
except TypeError:
    ohe = OneHotEncoder(handle_unknown='ignore', sparse=False)

ct = ColumnTransformer(
    transformers=[
        ('cont', Pipeline([
            ('imp', SimpleImputer(strategy='median')),
            ('sc',  StandardScaler())
        ]), cont_cols),
        ('bin',  Pipeline([
            ('imp', SimpleImputer(strategy='most_frequent'))
        ]), bin_cols),
        ('cat',  Pipeline([
            ('imp', SimpleImputer(strategy='most_frequent')),
            ('oh',  ohe)
        ]), cat_cols)
    ],
    remainder='drop'
)
X = ct.fit_transform(df_feat)

# ---- OPTICS ----
opt = OPTICS(
    min_samples=MIN_SAMPLES,
    xi=XI,
    min_cluster_size=MIN_CLUSTER_SIZE,
    metric=METRIC
)
labels = opt.fit_predict(X)   # -1 = ruido

# ================== SALIDAS ==================
print("\n=== RESULTADOS OPTICS (alternativa a DBSCAN) ===")

# (1) Número de clusters (excluye ruido -1)
valid_labels = labels[labels != -1]
n_clusters = len(np.unique(valid_labels)) if valid_labels.size > 0 else 0
n_noise = np.sum(labels == -1)
print(f"1) Número de clusters encontrados (excluye ruido): {n_clusters}")
print(f"   · Puntos de ruido (-1): {n_noise}")

# (2) Métricas internas (solo sobre puntos no-ruido con ≥2 clusters)
if valid_labels.size > 1 and n_clusters >= 2:
    mask_valid = labels != -1
    X_valid    = X[mask_valid]
    lab_valid  = labels[mask_valid]

    sil = silhouette_score(X_valid, lab_valid, metric=METRIC)
    dbi = davies_bouldin_score(X_valid, lab_valid)
    ch  = calinski_harabasz_score(X_valid, lab_valid)

    D_valid = squareform(pdist(X_valid, metric=METRIC))
    dunn    = dunn_index_from_square(D_valid, lab_valid)
    dunn_str = "inf" if np.isinf(dunn) else ("nan" if np.isnan(dunn) else f"{dunn:.3f}")

    print(f"2) Silhouette (sin ruido, {METRIC}): {sil:.3f}")
    print(f"   Davies–Bouldin (↓ mejor): {dbi:.3f}")
    print(f"   Calinski–Harabasz (↑ mejor): {ch:.1f}")
    print(f"   Dunn (↑ mejor, {METRIC}): {dunn_str}")
else:
    print("2) Silhouette / DB / CH / Dunn: no aplican (clusters insuficientes tras excluir ruido)")

# (3) Tablas resumen por cluster
df_out = df_feat.copy()
df_out['cluster_OPTICS'] = labels

# anexar "tiempo (minutos)" y "km" para reportes numéricos
extra_cols = [c for c in ["tiempo (minutos)", "km"] if c in gdf_ll.columns]
for c in extra_cols:
    df_out[c] = pd.to_numeric(gdf_ll[c].values, errors='coerce')

print("\n3.a) Tamaño por etiqueta (incluye ruido):")
sizes_all = df_out['cluster_OPTICS'].value_counts().sort_index()
for k_, v_ in sizes_all.items():
    tag = " (ruido)" if k_ == -1 else ""
    print(f"  - Etiqueta {k_}{tag}: {v_} filas")

df_valid = df_out[df_out['cluster_OPTICS'] != -1].copy()
if df_valid.empty or n_clusters == 0:
    print("\n[Resumen] No hay clusters válidos (todo fue marcado como ruido).")
else:
    num_all = [c for c in df_valid.columns if c not in ['cluster_OPTICS', 'Sexo (Desc)']]
    for c in num_all:
        df_valid[c] = pd.to_numeric(df_valid[c], errors='coerce')

    means = df_valid.groupby('cluster_OPTICS')[num_all].mean().round(4)
    print("\n3.b) Promedios por cluster (media, 4 decimales):")
    print(means.to_string())

    def iqr(s):
        q1, q3 = np.nanpercentile(s, [25, 75])
        return q3 - q1
    def cv(s):
        m = np.nanmean(s); sd = np.nanstd(s, ddof=0)
        return np.nan if m==0 or np.isnan(m) else sd/m

    agg_std = df_valid.groupby('cluster OPTICS' if 'cluster OPTICS' in df_valid.columns else 'cluster_OPTICS')[num_all].std(ddof=0).round(4)
    agg_iqr = df_valid.groupby('cluster OPTICS' if 'cluster OPTICS' in df_valid.columns else 'cluster_OPTICS')[num_all].agg(iqr).round(4)
    agg_cv  = df_valid.groupby('cluster OPTICS' if 'cluster OPTICS' in df_valid.columns else 'cluster_OPTICS')[num_all].agg(cv).round(4)

    print("\n3.c) Variabilidad — Desviación estándar por cluster:")
    print(agg_std.to_string())
    print("\n3.c) Variabilidad — IQR por cluster:")
    print(agg_iqr.to_string())
    print("\n3.c) Variabilidad — Coeficiente de Variación (CV) por cluster:")
    print(agg_cv.to_string())

    print("\n3.d) Asociación — Correlación de Spearman (por cluster):")
    for k_ in sorted(df_valid['cluster_OPTICS'].unique()):
        sub = df_valid[df_valid['cluster_OPTICS']==k_][num_all]
        corr = sub.corr(method='spearman')
        print(f"\n  > Cluster {k_} — matriz Spearman:")
        print(corr.round(2).to_string())

    if "Sexo (Desc)" in df_valid.columns:
        print("\n3.d) Asociación — Distribución de Sexo (Desc) por cluster (%):")
        tmp = df_valid.copy()
        tmp["Sexo (Desc)"] = tmp["Sexo (Desc)"].astype(str).str.strip().fillna("missing")
        sex_pct = (tmp.pivot_table(
                        index='cluster_OPTICS',
                        columns='Sexo (Desc)',
                        aggfunc=len,
                        fill_value=0
                   ).apply(lambda r: r/r.sum()*100, axis=1).round(1))
        print(sex_pct.to_string())

# (4) MAPA geográfico con comunas coloreadas y puntos translúcidos
if all(c in gdf_ll.columns for c in ['lat','lng']):
    gmap = gdf_ll.copy()
    gmap['cluster_OPTICS'] = labels

    # Paleta de puntos de clusters válidos (excluye ruido -1)
    uniq = np.sort(np.unique(labels[labels!=-1]))
    cmap = plt.cm.get_cmap('tab20', max(len(uniq), 3))
    colors_pts = {c: cmap(i % 20) for i, c in enumerate(uniq)}

    # Paleta fija de comunas (relleno polígono)
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

    # Normalizar nombre de comuna para mapear al color correcto
    def norm_name(x):
        x = '' if x is None else str(x)
        x = ''.join(ch for ch in unicodedata.normalize('NFD', x) if unicodedata.category(ch) != 'Mn')
        x = x.lower().strip()
        return x

    comunas_ll["_comuna_norm_"] = comunas_ll[comuna_col].apply(norm_name)
    comunas_ll["__fillcolor__"] = comunas_ll["_comuna_norm_"].map(comuna_colors)

    fig, ax = plt.subplots(1,1, figsize=(10,10))

    # 4.1 Dibujar cada comuna con su color
    for _, row in comunas_ll.iterrows():
        fc = row["__fillcolor__"] if pd.notna(row["__fillcolor__"]) else "#dddddd"
        gpd.GeoSeries([row.geometry], crs=comunas_ll.crs).plot(
            ax=ax,
            facecolor=fc,
            edgecolor='gray',
            linewidth=0.8,
            alpha=1.0
        )

    # 4.2 Borde de la provincia
    prov_ll.boundary.plot(ax=ax, linewidth=1.5, color='black')

    # 4.3 Puntos de ruido (-1): MISMA TRANSPARENCIA que los clusters (alpha=0.45),
    #     porque el ruido NO DEBE SER OPACO.
    if (gmap['cluster_OPTICS']==-1).any():
        gmap[gmap['cluster_OPTICS']==-1].plot(
            ax=ax,
            color='black',
            markersize=22,
            alpha=1,      # <- translúcido, NO opaco
            marker='x',
            edgecolor='black',
            linewidth=0.3,
            label='Ruido (-1)',
            zorder=5
        )

    # 4.4 Puntos de clusters válidos (también translúcidos para ver sobreposición)
    for c in uniq:
        msk = gmap['cluster_OPTICS'] == c
        gmap[msk].plot(
            ax=ax,
            color=colors_pts[c],
            markersize=28,
            alpha=0.75,      # <- translúcido
            marker='o',
            edgecolor='black',
            linewidth=0.3,
            label=f'Cluster {c}',
            zorder=4
        )

    # 4.5 Centroides geográficos promedio por cluster (sin ruido)
    if len(uniq) > 0:
        cent = gmap[gmap['cluster_OPTICS']!=-1].groupby('cluster_OPTICS')[['lng','lat']].mean()
        ax.scatter(
            cent['lng'],
            cent['lat'],
            s=260,
            marker='X',
            color='white',
            edgecolor='black',
            linewidth=1.5,
            zorder=6
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
                zorder=7
            )

    ax.set_title(f"Mapa: OPTICS (métrica={METRIC}) — Provincia de Llanquihue")
    ax.set_axis_off()
    ax.legend(
        loc='lower left',
        frameon=True,
        fontsize=9,
        title='Etiqueta',
        markerscale=1.2
    )
    plt.tight_layout()
    plt.savefig("optics.svg", format ="svg")
    plt.show()

else:
    print("\n[Mapa] No se encontraron columnas 'lat'/'lng' para dibujar el mapa.")