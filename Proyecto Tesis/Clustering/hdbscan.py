# ================== HDBSCAN (variables solicitadas) + DUNN ==================
!pip -q install geopandas shapely pyproj fiona rtree openpyxl scikit-learn hdbscan --upgrade


# ---- Imports ----
import re, unicodedata, warnings
import numpy as np, pandas as pd, geopandas as gpd, matplotlib.pyplot as plt
from shapely.geometry import Point
from shapely.errors import GEOSException
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
from scipy.spatial.distance import pdist, squareform   # <-- para Dunn
import hdbscan
import matplotlib.patheffects as pe
warnings.filterwarnings("ignore")

# ---- Config ----
FILE_PATH = '/content/drive/MyDrive/HPM/Bases de trabajo/BASE FINAL.xlsx'
SHEET = 0
PROVINCIAS_SHP = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Provincias/Provincias.shp'
COMUNAS_SHP    = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Comunas/comunas.shp'
TARGET_PROV = 'LLANQUIHUE'

# Hiperparámetros HDBSCAN (atributos, métrica euclídea)
MIN_CLUSTER_SIZE = 48
MIN_SAMPLES = 9
CLUSTER_SELECTION_METHOD = 'leaf'

RANDOM_STATE = 42

# ---- Helper Dunn ----
def dunn_index_from_square(D_square, labels):
    """
    Dunn = (mínima distancia inter-cluster) / (máximo diámetro intra-cluster).
    D_square: matriz NxN de distancias (euclídea aquí).
    labels: etiquetas de clusters válidos.
    """
    labels = np.asarray(labels)
    uniq = np.unique(labels)
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
    max_diam = np.max(diameters) if len(diameters) else np.nan

    # Mínima distancia inter
    min_inter = np.inf
    ulist = list(uniq)
    for i in range(len(ulist)):
        for j in range(i+1, len(ulist)):
            a = idx_by_c[ulist[i]]
            b = idx_by_c[ulist[j]]
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
    s = ''.join(c for c in unicodedata.normalize('NFD', s)
                if unicodedata.category(c)!='Mn')
    s = re.sub(r'[^0-9a-zA-Z\s]', ' ', s).lower()
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def normalize_col(s):
    t = ''.join(
        ch for ch in unicodedata.normalize('NFD', str(s))
        if unicodedata.category(ch) != 'Mn'
    )
    t = t.lower()
    return re.sub(r'[^0-9a-z]', '', t)

def map_expected_columns(df, expected_list):
    norm_actual = {normalize_col(c): c for c in df.columns}
    mapping = {name: norm_actual.get(normalize_col(name), None)
               for name in expected_list}
    missing = [k for k,v in mapping.items() if v is None]
    if missing:
        print("Aviso: no se encontraron columnas ->", missing)
    return mapping

def detectar_col_provincia(columns):
    candidatos = [
        'PROVINCIA','Provincia','provincia',
        'NOM_PROV','NOM_PROFIN','NOM_PROVIN','NOM_PROVINCIA',
        'PROV_NOM','PROVINC','PROV_NAME','NAME_2','NOM_PROV'
    ]
    # dedupe y recorrer
    vistos = set()
    for c in candidatos:
        if c in columns and c not in vistos:
            return c
        vistos.add(c)
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
        cl = c.lower()
        if 'comun' in cl or 'name' in cl:
            return c

    # fallback final: primera columna
    return list(columns)[0]

# -------- FILTRO ROBUSTO (incluye borde + fallback por cercanía + autodetección lat/lng) --------
def filtrar_llanquihue(df, target_prov=None, tol_m=250):
    """
    Retorna:
      - gdf_ll      : puntos asignados a la provincia objetivo (EPSG:4326)
      - prov_ll     : polígono(s) de la provincia objetivo
      - comunas_ll  : comunas que intersectan la provincia objetivo
      - comuna_col  : nombre de la columna con el nombre de la comuna (para colorear)
    Estrategia:
      - 'intersects' para incluir puntos sobre el borde
      - repara geometrías inválidas con buffer(0)
      - fallback por cercanía con sjoin_nearest a <= tol_m metros (EPSG:3857)
      - autodetección de lat/lng intercambiados si fuese necesario
    """
    if target_prov is None:
        target_prov = TARGET_PROV

    if 'lat' not in df.columns or 'lng' not in df.columns:
        raise ValueError("Faltan columnas 'lat' y/o 'lng'.")

    df = df.copy()
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lng'] = pd.to_numeric(df['lng'], errors='coerce')
    df = df.dropna(subset=['lat','lng'])

    # Autodetección swap lat/lng para coordenadas Chile aprox
    def _pct_ok(series, lo, hi):
        s = pd.to_numeric(series, errors='coerce')
        return float(s.between(lo, hi).mean())

    lat_ok = _pct_ok(df['lat'], -56, -17)
    lng_ok = _pct_ok(df['lng'], -76, -66)
    if (lat_ok < 0.3) or (lng_ok < 0.3):
        lat_ok_sw = _pct_ok(df['lng'], -56, -17)
        lng_ok_sw = _pct_ok(df['lat'], -76, -66)
        if lat_ok_sw > lat_ok and lng_ok_sw > lng_ok:
            df[['lat','lng']] = df[['lng','lat']]

    # Construir GeoDataFrame de puntos
    gdf_pts = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df['lng'], df['lat']),
        crs='EPSG:4326'
    )

    # Provincias
    prov = gpd.read_file(PROVINCIAS_SHP)
    prov = prov.to_crs(4326) if prov.crs else prov.set_crs(4326)

    # Reparar geometrías inválidas en provincias
    try:
        prov['geometry'] = prov.buffer(0)
    except GEOSException:
        pass
    except Exception:
        pass

    colp = detectar_col_provincia(prov.columns)
    if colp is None:
        raise ValueError(f"No se identificó columna de provincia en: {list(prov.columns)}")

    def _norm(s): return canon(s).upper()
    prov['_p_'] = prov[colp].map(_norm)
    target_norm = _norm(target_prov)

    prov_sel = prov[prov['_p_'] == target_norm]
    if prov_sel.empty:
        raise ValueError(f"No se encontró la provincia '{target_prov}' en el shapefile.")

    # Join principal (incluye borde)
    try:
        j1 = gpd.sjoin(gdf_pts, prov[[colp, 'geometry']],
                       how='left', predicate='intersects')
    except TypeError:
        j1 = gpd.sjoin(gdf_pts, prov[[colp, 'geometry']],
                       how='left', op='intersects')

    # Fallback por cercanía en metros
    na_idx = j1[colp].isna()
    if na_idx.any():
        pts_m  = j1.to_crs(3857)
        prov_m = prov.to_crs(3857)
        try:
            jnear = gpd.sjoin_nearest(
                pts_m.loc[na_idx, ['geometry']],
                prov_m[[colp, 'geometry']],
                how='left',
                max_distance=float(tol_m),
                distance_col='_dist_m'
            )
            j1.loc[na_idx, colp] = jnear[colp].values
        except Exception:
            prov_buf = prov_m.copy()
            prov_buf['geometry'] = prov_buf.buffer(float(tol_m))
            try:
                j2 = gpd.sjoin(
                    pts_m.loc[na_idx, ['geometry']],
                    prov_buf[[colp,'geometry']],
                    how='left',
                    predicate='intersects'
                )
            except TypeError:
                j2 = gpd.sjoin(
                    pts_m.loc[na_idx, ['geometry']],
                    prov_buf[[colp,'geometry']],
                    how='left',
                    op='intersects'
                )
            j2 = j2.to_crs(4326)
            j1.loc[na_idx, colp] = j2[colp].values

    # Filtrar provincia objetivo
    j1['__p__'] = j1[colp].map(_norm)
    gdf_ll = j1[j1['__p__'] == target_norm] \
               .drop(columns=['__p__','index_right'], errors='ignore')
    if gdf_ll.empty:
        bbx_pts  = gdf_pts.total_bounds
        bbx_prov = prov_sel.total_bounds
        raise ValueError(
            f"No hay puntos en '{target_prov}' incluso con tol={tol_m}m.\n"
            f"bbox puntos lon[{bbx_pts[0]:.3f},{bbx_pts[2]:.3f}] "
            f"lat[{bbx_pts[1]:.3f},{bbx_pts[3]:.3f}]\n"
            f"bbox prov lon[{bbx_prov[0]:.3f},{bbx_prov[2]:.3f}] "
            f"lat[{bbx_prov[1]:.3f},{bbx_prov[3]:.3f}]"
        )

    # Polígono(s) de la provincia objetivo
    prov_ll = prov_sel.to_crs(4326).drop(columns=['_p_'], errors='ignore')

    # Comunas que intersectan la provincia objetivo
    comunas = gpd.read_file(COMUNAS_SHP)
    comunas = comunas.to_crs(4326) if comunas.crs else comunas.set_crs(4326)
    try:
        comunas['geometry'] = comunas.buffer(0)
    except Exception:
        pass

    try:
        comunas_ll = gpd.sjoin(
            comunas,
            prov_sel[['geometry']],
            how='inner',
            predicate='intersects'
        ).drop(columns=['index_right'], errors='ignore')
    except TypeError:
        comunas_ll = gpd.sjoin(
            comunas,
            prov_sel[['geometry']],
            how='inner',
            op='intersects'
        ).drop(columns=['index_right'], errors='ignore')

    comunas_ll = comunas_ll.to_crs(4326)
    comuna_col = detectar_col_comuna(comunas_ll.columns)

    return gdf_ll, prov_ll, comunas_ll, comuna_col

# ---------------------------------------------------------------------------------

# ---- Load + spatial filter ----
df_raw = pd.read_excel(FILE_PATH, sheet_name=SHEET)
colmap = map_expected_columns(df_raw, EXPECTED_ALL)
df_std = df_raw.rename(columns={colmap[k]: k for k in EXPECTED_ALL if colmap.get(k)})

gdf_ll, prov_ll, comunas_ll, comuna_col = filtrar_llanquihue(df_std)

# ---- Features para clustering ----
present = [v for v in CLUSTER_VARS if v in gdf_ll.columns]
if len(present) < 3:
    raise ValueError(f"Variables disponibles insuficientes para HDBSCAN: {present}")

df_feat = pd.DataFrame(gdf_ll.drop(columns='geometry'))[present].copy()

# Tipos
cat_cols  = ["Sexo (Desc)"] if "Sexo (Desc)" in df_feat.columns else []
cont_cols = [
    c for c in ["tiempo (minutos)", "km", "Numero de registros", "Ultima Edad Registrada"]
    if c in df_feat.columns
]
bin_cols  = [
    c for c in ["Mayor","Menor","Moderada","Total_Amputaciones"]
    if c in df_feat.columns
]

# Preprocesamiento
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

# ---- HDBSCAN ----
hdb = hdbscan.HDBSCAN(
    min_cluster_size=MIN_CLUSTER_SIZE,
    min_samples=MIN_SAMPLES,
    metric='euclidean',
    cluster_selection_method=CLUSTER_SELECTION_METHOD
)
labels = hdb.fit_predict(X)   # -1 = ruido

# ================== SALIDAS ==================
print("\n=== RESULTADOS HDBSCAN (variables solicitadas) ===")

# (1) Número de clusters (excluye ruido -1)
valid_labels = labels[labels != -1]
n_clusters   = len(np.unique(valid_labels)) if valid_labels.size > 0 else 0
n_noise      = np.sum(labels == -1)
print(f"1) Número de clusters encontrados (excluye ruido): {n_clusters}")
print(f"   · Puntos de ruido (-1): {n_noise}")

# (2) Métricas internas (no-ruido, ≥2 clusters)
if valid_labels.size > 1 and n_clusters >= 2:
    X_valid   = X[labels != -1]
    lab_valid = labels[labels != -1]

    sil = silhouette_score(X_valid, lab_valid, metric='euclidean')
    print(f"2) Silhouette (sin ruido): {sil:.3f}")

    dbi = davies_bouldin_score(X_valid, lab_valid)
    print(f"   Davies–Bouldin (↓ mejor): {dbi:.3f}")

    ch  = calinski_harabasz_score(X_valid, lab_valid)
    print(f"   Calinski–Harabasz (↑ mejor): {ch:.1f}")

    # ---- DUNN (euclídea, no-ruido) ----
    Dv   = squareform(pdist(X_valid, metric='euclidean'))
    dunn = dunn_index_from_square(Dv, lab_valid)
    dunn_txt = "inf" if np.isinf(dunn) else (
        f"{dunn:.3f}" if np.isfinite(dunn) else "nan"
    )
    print(f"   Dunn (↑ mejor): {dunn_txt}")

else:
    print("2) Silhouette / DB / CH / Dunn: no aplican (clusters insuficientes tras excluir ruido)")

# (3) Tablas resumen por cluster
df_out = df_feat.copy()
df_out['cluster_hdbscan'] = labels

# ---- anexar "tiempo (minutos)" y "km" SOLO para reportes
extra_cols = [c for c in ["tiempo (minutos)", "km"] if c in gdf_ll.columns]
for c in extra_cols:
    df_out[c] = pd.to_numeric(gdf_ll[c].values, errors='coerce')

print("\n3.a) Tamaño por cluster (incluye ruido):")
sizes_all = df_out['cluster_hdbscan'].value_counts().sort_index()
for k_, v_ in sizes_all.items():
    tag = " (ruido)" if k_ == -1 else ""
    print(f"  - Etiqueta {k_}{tag}: {v_} filas")

df_valid = df_out[df_out['cluster_hdbscan'] != -1].copy()
if df_valid.empty or n_clusters == 0:
    print("\n[Resumen] No hay clusters válidos (todo fue marcado como ruido).")
else:
    num_all = [c for c in df_valid.columns
               if c not in ['cluster_hdbscan', 'Sexo (Desc)']]
    for c in num_all:
        df_valid[c] = pd.to_numeric(df_valid[c], errors='coerce')

    means = df_valid.groupby('cluster_hdbscan')[num_all].mean().round(4)
    print("\n3.b) Promedios por cluster (media, 4 decimales):")
    print(means.to_string())

    def iqr(s):
        q1, q3 = np.nanpercentile(s, [25, 75])
        return q3 - q1
    def cv(s):
        m  = np.nanmean(s)
        sd = np.nanstd(s, ddof=0)
        return np.nan if m==0 or np.isnan(m) else sd/m

    agg_std = df_valid.groupby('cluster_hdbscan')[num_all].std(ddof=0).round(4)
    agg_iqr = df_valid.groupby('cluster_hdbscan')[num_all].agg(iqr).round(4)
    agg_cv  = df_valid.groupby('cluster_hdbscan')[num_all].agg(cv).round(4)

    print("\n3.c) Variabilidad — Desviación estándar por cluster:")
    print(agg_std.to_string())

    print("\n3.c) Variabilidad — IQR por cluster:")
    print(agg_iqr.to_string())

    print("\n3.c) Variabilidad — Coeficiente de Variación (CV) por cluster:")
    print(agg_cv.to_string())

    print("\n3.d) Asociación — Correlación de Spearman (por cluster):")
    for k_ in sorted(df_valid['cluster_hdbscan'].unique()):
        sub = df_valid[df_valid['cluster_hdbscan']==k_][num_all]
        corr = sub.corr(method='spearman')
        print(f"\n  > Cluster {k_} — matriz Spearman:")
        print(corr.round(2).to_string())

    if "Sexo (Desc)" in df_valid.columns:
        print("\n3.d) Asociación — Distribución de Sexo (Desc) por cluster (%):")
        tmp = df_valid.copy()
        tmp["Sexo (Desc)"] = (
            tmp["Sexo (Desc)"].astype(str).str.strip().fillna("missing")
        )
        sex_pct = (
            tmp.pivot_table(
                index='cluster_hdbscan',
                columns='Sexo (Desc)',
                aggfunc=len,
                fill_value=0
            )
            .apply(lambda r: r/r.sum()*100, axis=1)
            .round(1)
        )
        print(sex_pct.to_string())

# (4) MAPA geográfico con comunas coloreadas y puntos translúcidos
if all(c in gdf_ll.columns for c in ['lat','lng']):
    gmap = gdf_ll.copy()
    gmap['cluster_hdbscan'] = labels

    # Colores base para clusters válidos (excluye ruido -1)
    uniq = np.sort(np.unique(labels[labels!=-1]))  # solo clusters válidos
    cmap = plt.cm.get_cmap('tab20', max(len(uniq), 3))
    colors_pts = {c: cmap(i % 20) for i, c in enumerate(uniq)}

    # Paleta fija por comuna (relleno del polígono)
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

    # Normalizar nombre de comuna para mapear color
    def norm_name(x):
        x = '' if x is None else str(x)
        x = ''.join(
            ch for ch in unicodedata.normalize('NFD', x)
            if unicodedata.category(ch) != 'Mn'
        )
        x = x.lower().strip()
        return x

    comunas_ll = comunas_ll.copy()
    comunas_ll["_comuna_norm_"] = comunas_ll[comuna_col].apply(norm_name)
    comunas_ll["__fillcolor__"] = comunas_ll["_comuna_norm_"].map(comuna_colors)

    fig, ax = plt.subplots(1,1, figsize=(10,10))

    # 4.1 Dibujar comunas con su color asignado
    for _, row in comunas_ll.iterrows():
        fc = row["__fillcolor__"] if pd.notna(row["__fillcolor__"]) else "#dddddd"
        gpd.GeoSeries([row.geometry], crs=comunas_ll.crs).plot(
            ax=ax,
            facecolor=fc,
            edgecolor='gray',
            linewidth=0.8,
            alpha=1.0,
            zorder=1
        )

    # 4.2 Borde provincia más grueso encima
    prov_ll.boundary.plot(ax=ax, linewidth=1.5, color='black', zorder=2)

    # 4.3 Puntos de ruido (-1): translúcidos, NO opacos
    if (gmap['cluster_hdbscan']==-1).any():
        gmap[gmap['cluster_hdbscan']==-1].plot(
            ax=ax,
            color='black',
            markersize=22,
            alpha=1,      # <- ruido NO debe ser opaco
            marker='x',
            edgecolor='black',
            linewidth=0.3,
            label='Ruido (-1)',
            zorder=5
        )

    # 4.4 Puntos de clusters válidos: translúcidos para ver sobreposición
    for c in uniq:
        msk = gmap['cluster_hdbscan'] == c
        gmap[msk].plot(
            ax=ax,
            color=colors_pts[c],
            markersize=28,
            alpha=0.75,      # <- igual transparencia que hemos usado
            marker='o',
            edgecolor='black',
            linewidth=0.3,
            label=f'Cluster {c}',
            zorder=4
        )

    # 4.5 Centroides geográficos promedio de cada cluster válido (sin ruido)
    if len(uniq) > 0:
        cent = (
            gmap[gmap['cluster_hdbscan']!=-1]
            .groupby('cluster_hdbscan')[['lng','lat']]
            .mean()
        )
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
                ha='center',
                va='center',
                color='black',
                path_effects=[pe.withStroke(linewidth=2.5, foreground='white')],
                fontsize=11,
                fontweight='bold',
                zorder=7
            )

    ax.set_title('Mapa: HDBSCAN — Provincia de Llanquihue')
    ax.set_axis_off()
    ax.legend(
        loc='lower left',
        frameon=True,
        fontsize=9,
        title='Etiqueta',
        markerscale=1.2
    )
    plt.tight_layout()
    plt.savefig("DBSCAN 2.0.svg", format ="svg")
    plt.show()

else:
    print("\n[Mapa] No se encontraron columnas 'lat'/'lng' para dibujar el mapa.")