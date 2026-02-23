# ================== MAPA DE AMPUTACIONES POR PACIENTE (SOLO PUERTO MONTT) ==================
#!pip -q install geopandas shapely pyproj fiona rtree openpyxl --upgrade

# ---- Config ----
FILE_PATH = '/content/drive/MyDrive/HPM/Bases de trabajo/BASE FINAL.xlsx'
SHEET = 0
PROVINCIAS_SHP = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Provincias/Provincias.shp'
COMUNAS_SHP    = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Comunas/comunas.shp'
TARGET_PROV = 'LLANQUIHUE'
TARGET_COMUNA = 'PUERTO MONTT'   # >>> CAMBIO: objetivo comuna

# Coordenadas aproximadas Hospital de Puerto Montt (HPM)
HPM_LAT = -41.4714
HPM_LNG = -72.9363

# Variables de amputación a mapear
AMP_VARS = [
    'AMP_DEDO_MANO',
    'AMP_PULGAR',
    #'AMP_DEDO_PIE'#,
    #'AMP_A_NIVEL_PIE'#,
    'DESART_TOBILLO',
    'AMP_NIVEL_MALEOLO',
    #'AMP_DEBAJO_RODILLA'#,
    'DESART_RODILLA'#,
    #'AMP_ENCIMA_RODILLA'
]

# Nombres legibles en leyenda
AMP_LABELS = {
    'AMP_DEDO_MANO'        : 'Amputación Dedo Mano',
    'AMP_PULGAR'           : 'Amputación Pulgar',
    'AMP_DEDO_PIE'         : 'Amputación Dedo Pie',
    'AMP_A_NIVEL_PIE'      : 'Amputación a Nivel de Pie',
    'DESART_TOBILLO'       : 'Desarticulación Tobillo',
    'AMP_NIVEL_MALEOLO'    : 'Amputación Nivel Maleolo',
    'AMP_DEBAJO_RODILLA'   : 'Amputación Debajo de Rodilla',
    'DESART_RODILLA'       : 'Desarticulación Rodilla',
    'AMP_ENCIMA_RODILLA'   : 'Amputación Encima de Rodilla'
}

# ---- Imports ----
import re, unicodedata, warnings
import numpy as np, pandas as pd, geopandas as gpd, matplotlib.pyplot as plt
warnings.filterwarnings("ignore")

# ---- Helpers ----
def canon(s):
    s = '' if s is None else str(s)
    s = ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c)!='Mn'
    )
    s = re.sub(r'[^0-9a-zA-Z\s]', ' ', s).lower()
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def normalize_col(s):
    t = ''.join(
        ch for ch in unicodedata.normalize('NFD', str(s))
        if unicodedata.category(ch) != 'Mn'
    )
    return re.sub(r'[^0-9a-z]', '', t.lower())

def map_expected_columns(df, expected_list):
    norm_actual = {normalize_col(c): c for c in df.columns}
    mapping = {
        name: norm_actual.get(normalize_col(name), None)
        for name in expected_list
    }
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
    candidatos = [
        'COMUNA','Comuna','comuna',
        'NOM_COM','NOM_COMUNA','NOMBRE','NOM_COMU',
        'NAME','NAME_3','name','nom_com',
        'CUT_COM','CUT_COMUNA'
    ]
    for c in candidatos:
        if c in columns:
            return c
    for c in columns:
        if ('comun' in c.lower()) or ('name' in c.lower()):
            return c
    return list(columns)[0]

# -------- FILTRO + CLIP SOLO LLANQUIHUE --------
def filtrar_llanquihue(df, target_prov=None, tol_m=250):
    if target_prov is None:
        target_prov = TARGET_PROV

    if 'lat' not in df.columns or 'lng' not in df.columns:
        raise ValueError("Faltan columnas 'lat' y/o 'lng' en la base.")

    df = df.copy()
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lng'] = pd.to_numeric(df['lng'], errors='coerce')
    df = df[df['lat'].notna() & df['lng'].notna()].copy()

    gdf_pts = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df['lng'], df['lat']),
        crs='EPSG:4326'
    )

    prov = gpd.read_file(PROVINCIAS_SHP)
    prov = prov.set_crs('EPSG:4326') if prov.crs is None else prov.to_crs('EPSG:4326')
    try:
        prov['geometry'] = prov.buffer(0)
    except Exception:
        pass

    col_prov = detectar_col_provincia(prov.columns)
    if col_prov is None:
        raise ValueError(f"No se pudo detectar columna de provincia en: {list(prov.columns)}")

    try:
        j1 = gpd.sjoin(
            gdf_pts,
            prov[[col_prov, 'geometry']],
            how='left',
            predicate='intersects'
        )
    except TypeError:
        j1 = gpd.sjoin(
            gdf_pts,
            prov[[col_prov, 'geometry']],
            how='left',
            op='intersects'
        )

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

    norm = lambda s: canon(s).upper()
    j1['__prov_norm__'] = j1[col_prov].map(norm)
    gdf_ll = j1[j1['__prov_norm__'] == norm(target_prov)].drop(
        columns=['__prov_norm__','index_right'],
        errors='ignore'
    )
    if gdf_ll.empty:
        raise ValueError(f"No hay puntos en la provincia '{target_prov}' tras filtrar.")

    prov_ll = prov[prov[col_prov].map(norm) == norm(target_prov)].to_crs('EPSG:4326')

    comunas = gpd.read_file(COMUNAS_SHP)
    comunas = comunas.set_crs('EPSG:4326') if comunas.crs is None else comunas.to_crs('EPSG:4326')
    try:
        comunas['geometry'] = comunas.buffer(0)
    except Exception:
        pass

    comunas_clip = gpd.overlay(
        comunas,
        prov_ll[['geometry']],
        how='intersection'
    ).to_crs('EPSG:4326')

    comuna_col = detectar_col_comuna(comunas_clip.columns)

    return gdf_ll.to_crs('EPSG:4326'), prov_ll.to_crs('EPSG:4326'), comunas_clip.to_crs('EPSG:4326'), comuna_col

# -------------------------------------------------------------------
# ---- Cargar datos y quedarnos con LLANQUIHUE ----
df_raw = pd.read_excel(FILE_PATH, sheet_name=SHEET)

cols_necesarias = ["lat", "lng"] + AMP_VARS
colmap = map_expected_columns(df_raw, cols_necesarias)

df_std = df_raw.rename(columns={
    colmap[k]: k for k in cols_necesarias if colmap.get(k)
})

gdf_ll, prov_ll, comunas_ll, comuna_col = filtrar_llanquihue(
    df_std,
    target_prov=TARGET_PROV,
    tol_m=250
)

# -------------------------------------------------------------------
# ---- Colores por comuna (paleta pastel fija) ----
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

# -------------------------------------------------------------------
# >>> CAMBIO: seleccionar SOLO la comuna de Puerto Montt y clippear puntos a esa comuna
TARGET_COMUNA_NORM = norm_name(TARGET_COMUNA)

comuna_pm = comunas_ll[comunas_ll["_comuna_norm_"] == TARGET_COMUNA_NORM].copy()
if comuna_pm.empty:
    raise ValueError("No se encontró la comuna 'PUERTO MONTT' dentro de la provincia.")

# color de relleno (fallback si no está en el dict)
comuna_pm["__fillcolor__"] = comuna_pm["__fillcolor__"].fillna("#dddddd")

# Mantener solo los puntos que caen DENTRO de la comuna de Puerto Montt
try:
    pts_pm = gpd.sjoin(gdf_ll, comuna_pm[['geometry']], how='inner', predicate='within')
except TypeError:
    pts_pm = gpd.sjoin(gdf_ll, comuna_pm[['geometry']], how='inner', op='within')

pts_pm = pts_pm.drop(columns=['index_right'], errors='ignore')

# -------------------------------------------------------------------
# ---- Paleta para las amputaciones ----
cmap_pts = plt.cm.get_cmap('tab10', len(AMP_VARS))
amp_color_map = {var: cmap_pts(i % 10) for i, var in enumerate(AMP_VARS)}

# -------------------------------------------------------------------
# ---- Plot (solo comuna de Puerto Montt) ----
fig, ax = plt.subplots(1, 1, figsize=(8, 10))

# 1) Pintar SOLO Puerto Montt
for _, row in comuna_pm.iterrows():
    face_c = row["__fillcolor__"] if pd.notna(row["__fillcolor__"]) else "#dddddd"
    gpd.GeoSeries([row.geometry], crs=comuna_pm.crs).plot(
        ax=ax,
        facecolor=face_c,
        edgecolor='black',
        linewidth=1.0,
        alpha=1.0,
        zorder=1
    )

# 2) Puntos por cada tipo de amputación (solo los dentro de Pto. Montt)
handles = []
labels_leg = []

for var in AMP_VARS:
    if var not in pts_pm.columns:
        continue
    submask = pd.to_numeric(pts_pm[var], errors='coerce').fillna(0) > 0
    if not submask.any():
        continue

    pts_pm[submask].plot(
        ax=ax,
        color=amp_color_map[var],
        markersize=28,
        alpha=0.85,
        marker='o',
        edgecolor='black',
        linewidth=0.3,
        zorder=4
    )
    handles.append(
        plt.Line2D(
            [0],[0],
            marker='o',
            color='black',
            markerfacecolor=amp_color_map[var],
            markeredgecolor='black',
            linewidth=0,
            markersize=8,
            alpha=0.85
        )
    )
    labels_leg.append(AMP_LABELS.get(var, var))

# 3) Hospital de Puerto Montt (HPM) con estrella
hpm_handle = None
if HPM_LAT is not None and HPM_LNG is not None:
    ax.scatter(
        HPM_LNG, HPM_LAT,
        s=80,
        marker='*',
        color='gold',
        edgecolor='black',
        linewidth=0.8,
        zorder=5
    )
    hpm_handle = plt.Line2D(
        [0],[0],
        marker='*',
        color='black',
        markerfacecolor='gold',
        markeredgecolor='black',
        linewidth=0,
        markersize=10
    )
    handles.insert(0, hpm_handle)
    labels_leg.insert(0, 'Hospital de Puerto Montt (HPM)')

# 4) Estética final
ax.set_title('Distribución de Pacientes por Tipo de Amputación\nComuna de Puerto Montt')
ax.set_axis_off()

ax.legend(
    handles,
    labels_leg,
    loc='lower left',
    frameon=True,
    fontsize=8,
    title='Tipos de Amputación',
    title_fontsize=9
)

# Enfocar el encuadre a la comuna
minx, miny, maxx, maxy = comuna_pm.total_bounds
padx = (maxx - minx) * 0.05
pady = (maxy - miny) * 0.05
ax.set_xlim(minx - padx, maxx + padx)
ax.set_ylim(miny - pady, maxy + pady)

plt.tight_layout()
plt.savefig("amp21.svg", format="svg")
plt.show()
