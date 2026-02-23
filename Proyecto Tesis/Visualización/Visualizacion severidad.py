# ================== MAPA DE PACIENTES POR NIVEL DE SEVERIDAD (SOLO LLANQUIHUE) ==================
#!pip -q install geopandas shapely pyproj fiona rtree openpyxl --upgrade

# ---- Config ----
FILE_PATH = '/content/drive/MyDrive/HPM/Bases de trabajo/BASE FINAL.xlsx'
SHEET = 0
PROVINCIAS_SHP = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Provincias/Provincias.shp'
COMUNAS_SHP    = '/content/drive/MyDrive/SHAPEFILES NUEVOS/Comunas/comunas.shp'
TARGET_PROV = 'LLANQUIHUE'

# Coordenadas aproximadas Hospital de Puerto Montt (HPM)
HPM_LAT = -41.4714
HPM_LNG = -72.9363

# Variables de severidad a mapear
SEV_VARS = ["Mayor",
            "Menor",
            "Moderada"
            ]

# Etiquetas legibles para la leyenda
SEV_LABELS = {
    "Mayor": "Severidad Mayor",
    "Menor": "Severidad Menor",
    "Moderada": "Severidad Moderada"
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
    """
    Alinea las columnas reales a los nombres esperados
    (por ejemplo 'lat', 'lng', 'Mayor', etc.) aunque tengan variaciones.
    """
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
    """
    Retorna:
      gdf_ll        -> pacientes dentro (o pegados) a la provincia objetivo
      prov_ll       -> polígono de la provincia objetivo
      comunas_clip  -> comunas recortadas EXACTAMENTE a la provincia
      comuna_col    -> nombre de la columna con el nombre de la comuna
    CRS final: EPSG:4326
    """
    if target_prov is None:
        target_prov = TARGET_PROV

    # Validar coords mínimas
    if 'lat' not in df.columns or 'lng' not in df.columns:
        raise ValueError("Faltan columnas 'lat' y/o 'lng' en la base.")

    df = df.copy()
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lng'] = pd.to_numeric(df['lng'], errors='coerce')
    df = df[df['lat'].notna() & df['lng'].notna()].copy()

    # GeoDataFrame de puntos
    gdf_pts = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df['lng'], df['lat']),
        crs='EPSG:4326'
    )

    # Provincias
    prov = gpd.read_file(PROVINCIAS_SHP)
    prov = prov.set_crs('EPSG:4326') if prov.crs is None else prov.to_crs('EPSG:4326')

    # Reparar geometrías si fuese necesario
    try:
        prov['geometry'] = prov.buffer(0)
    except Exception:
        pass

    col_prov = detectar_col_provincia(prov.columns)
    if col_prov is None:
        raise ValueError(f"No se pudo detectar columna de provincia en: {list(prov.columns)}")

    # Asignar provincia a cada punto (incluye puntos justo en el borde)
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

    # Fallback por cercanía hasta tol_m metros para los NaN
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

    # Filtrar SOLO la provincia objetivo
    norm = lambda s: canon(s).upper()
    j1['__prov_norm__'] = j1[col_prov].map(norm)
    gdf_ll = j1[j1['__prov_norm__'] == norm(target_prov)].drop(
        columns=['__prov_norm__','index_right'],
        errors='ignore'
    )
    if gdf_ll.empty:
        raise ValueError(f"No hay puntos en la provincia '{target_prov}' tras filtrar.")

    prov_ll = prov[prov[col_prov].map(norm) == norm(target_prov)].to_crs('EPSG:4326')

    # Comunas y recorte exacto a la provincia
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
# ---- 1. Cargar datos base y quedarnos con la provincia objetivo ----
df_raw = pd.read_excel(FILE_PATH, sheet_name=SHEET)

cols_necesarias = ["lat", "lng"] + SEV_VARS
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
# ---- 2. Colores pastel por comuna ----
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
# ---- 3. Paleta de colores para severidad ----
# Mayor    -> rojo
# Moderada -> celeste (nuevo pedido)
# Menor    -> azul
sev_color_map = {
    "Mayor":     "#e41a1c",  # rojo
    "Moderada":  "#00bfff",  # celeste
    "Menor":     "#008000",  # verde
}

# -------------------------------------------------------------------
# ---- 4. Graficar ----
fig, ax = plt.subplots(1, 1, figsize=(8, 10))

# 4.1 Pintar SOLO comunas recortadas a Llanquihue
for _, row in comunas_ll.iterrows():
    face_c = row["__fillcolor__"] if pd.notna(row["__fillcolor__"]) else "#dddddd"
    gpd.GeoSeries([row.geometry], crs=comunas_ll.crs).plot(
        ax=ax,
        facecolor=face_c,
        edgecolor='gray',
        linewidth=0.8,
        alpha=1.0,
        zorder=1
    )

# 4.2 Borde negro de la provincia
prov_ll.boundary.plot(ax=ax, linewidth=1.5, color='black', zorder=2)

# 4.3 Puntos por cada nivel de severidad
handles = []
labels_leg = []

for sev_var in SEV_VARS:
    if sev_var not in gdf_ll.columns:
        continue

    # Consideramos que está presente si el valor es >0 / True
    submask = pd.to_numeric(gdf_ll[sev_var], errors='coerce').fillna(0) > 0
    if not submask.any():
        continue

    gdf_ll[submask].plot(
        ax=ax,
        color=sev_color_map.get(sev_var, "#000000"),
        markersize=28,
        alpha=0.45,     # opacidad para ver sobreposición
        marker='o',
        edgecolor='black',
        linewidth=0.3,
        zorder=4
    )

    # Handle para la leyenda
    handles.append(
        plt.Line2D(
            [0],[0],
            marker='o',
            color='black',
            markerfacecolor=sev_color_map.get(sev_var, "#000000"),
            markeredgecolor='black',
            linewidth=0,
            markersize=8,
            alpha=0.45
        )
    )
    labels_leg.append(SEV_LABELS.get(sev_var, sev_var))

# 4.4 Hospital de Puerto Montt (estrella)
hpm_handle = None
if HPM_LAT is not None and HPM_LNG is not None:
    ax.scatter(
        HPM_LNG,
        HPM_LAT,
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

# 4.5 Estética final
ax.set_title('Distribución de Pacientes por Nivel de Severidad\nProvincia de Llanquihue')
ax.set_axis_off()

ax.legend(
    handles,
    labels_leg,
    loc='lower left',
    frameon=True,
    fontsize=8,
    title='Niveles de Severidad',
    title_fontsize=9
)

plt.tight_layout()
plt.savefig("severidad13.svg", format="svg")
plt.show()
