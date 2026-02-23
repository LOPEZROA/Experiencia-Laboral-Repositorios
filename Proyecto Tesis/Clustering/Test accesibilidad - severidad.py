
# ===========================================================
# ANÁLISIS DE ACCESIBILIDAD vs "ULTIMA SEVERIDAD"

!pip -q install scikit-posthocs statsmodels --upgrade

import os, glob, unicodedata, re, warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from scipy.stats import (
    shapiro, levene, fligner, f_oneway, kruskal,
    pearsonr, spearmanr, kendalltau
)
import scikit_posthocs as sp
from statsmodels.stats.multicomp import pairwise_tukeyhsd
from IPython.display import display

warnings.filterwarnings("ignore")
pd.set_option("display.max_columns", 200)

# ---------- Configuración ----------
FILE_GLOB = '/content/drive/MyDrive/HPM/Bases de trabajo/BASE FINAL CON GRUPO*.xlsx'
SHEET_INDEX = 0           # hoja a leer
ALPHA = 0.05
SAVE_OUTPUT = False       # guardar base con "ultima severidad"
SAVE_PLOTS  = False       # guardar PNGs de boxplots
OUT_DIR     = "/content"  # carpeta para PNGs si SAVE_PLOTS=True

# Nombres esperados (se buscarán de forma robusta, ignorando acentos y mayúsculas)
EXPECTED_SEVERITY = "ultima registro severidad"
EXPECTED_TIME     = "tiempo (minutos)"
EXPECTED_KM       = "km"

# ---------- Utilidades ----------
def slug(s: str) -> str:
    """Normaliza texto: minúsculas, sin acentos, sin signos raros, espacios simples."""
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[\t\n\r]+", " ", s)
    s = re.sub(r"[^a-z0-9\s\(\)\[\]\-_/.:]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def find_latest_xlsx(pattern: str) -> str:
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No se encontraron archivos con el patrón: {pattern}")
    files.sort(key=os.path.getmtime, reverse=True)
    return files[0]

def map_column(df: pd.DataFrame, target: str, fallbacks_tokens=None) -> str:
    """
    Encuentra en df la columna cuyo 'slug' coincide con 'target' o, en su defecto,
    que contenga todos los tokens de 'fallbacks_tokens'. Devuelve nombre ORIGINAL.
    """
    slugs = {col: slug(col) for col in df.columns}
    target_slug = slug(target)

    # 1) Exacto
    for original, s in slugs.items():
        if s == target_slug:
            return original

    # 2) Por tokens
    if fallbacks_tokens:
        tokens = [slug(t) for t in fallbacks_tokens]
        for original, s in slugs.items():
            if all(tok in s for tok in tokens):
                return original

    # 3) Ignorando paréntesis
    def strip_brackets(x): return re.sub(r"\([^)]*\)", "", x).strip()
    base_target = strip_brackets(target_slug)
    for original, s in slugs.items():
        if strip_brackets(s) == base_target:
            return original

    raise KeyError(f"No se encontró columna para: '{target}'. Revisa nombres en la base.")

def map_ultima_severidad(series: pd.Series) -> pd.Series:
    """
    Mapea 'menor'->1, 'moderada'->2, 'mayor'->3, robusto a acentos/mayúsculas.
    Si ya es 1/2/3 numérico, lo respeta.
    """
    def normalize_val(x):
        if pd.isna(x): return np.nan
        if isinstance(x, (int, float, np.integer, np.floating)):
            if x in (1, 2, 3): return int(x)
        sx = slug(str(x)).replace("ultima","").replace("última","").replace("registro","").strip()
        if "menor" in sx:    return 1
        if "moderada" in sx: return 2
        if "mayor" in sx:    return 3
        try:
            val = int(re.findall(r"\d+", sx)[0])
            return val if val in (1,2,3) else np.nan
        except:
            return np.nan
    return series.apply(normalize_val).astype("float").astype("Int64")

def group_arrays(df, y_col, x_col):
    """Devuelve lista de arrays por nivel y_col=1,2,3 (en ese orden) para x_col."""
    arrs = []
    for sev in [1, 2, 3]:
        arr = df.loc[df[y_col] == sev, x_col].dropna().values
        arrs.append(arr)
    return arrs

def check_normality_by_group(arrs, min_n=3):
    """Shapiro por grupo; devuelve DataFrame con {'n','W','p','normal?'}."""
    res = []
    for i, a in enumerate(arrs, start=1):
        if len(a) >= min_n and len(a) <= 5000:
            W, p = shapiro(a)
            res.append({"grupo": i, "n": len(a), "W": float(W), "p": float(p), "normal?": p > ALPHA})
        else:
            res.append({"grupo": i, "n": len(a), "W": np.nan, "p": np.nan, "normal?": False if len(a) < min_n else None})
    return pd.DataFrame(res)

def homoscedasticity_tests(arrs):
    """Levene (mediana) y Fligner-Killeen globales."""
    valid = [a for a in arrs if len(a) > 1]
    if len(valid) < 2:
        return {"Levene": (np.nan, np.nan), "Fligner": (np.nan, np.nan)}
    lev_F, lev_p = levene(*valid, center='median')
    flig_X2, flig_p = fligner(*valid)
    return {"Levene": (float(lev_F), float(lev_p)), "Fligner": (float(flig_X2), float(flig_p))}

def anova_or_kruskal(arrs):
    """
    Decide entre ANOVA y Kruskal según normalidad y homoscedasticidad.
    Retorna dict con resultados, tamaño de efecto y qué prueba se aplicó.
    """
    norm_df = check_normality_by_group(arrs)
    homo = homoscedasticity_tests(arrs)

    all_normal = norm_df["normal?"].fillna(False).all()
    homo_ok = (pd.notna(homo["Levene"][1]) and homo["Levene"][1] > ALPHA) and \
              (pd.notna(homo["Fligner"][1]) and homo["Fligner"][1] > ALPHA)

    N = sum(len(a) for a in arrs)
    k = sum(1 for a in arrs if len(a) > 0)

    if all_normal and homo_ok and k >= 2 and all(len(a) > 1 for a in arrs):
        F, p = f_oneway(*arrs)
        df1, df2 = k - 1, N - k
        eta2_p = (F * df1) / (F * df1 + df2) if (F * df1 + df2) != 0 else np.nan
        return {
            "test": "ANOVA (paramétrico)", "stat_label": "F", "stat": float(F), "p": float(p),
            "effect_label": "eta^2 parcial", "effect": float(eta2_p) if not np.isnan(eta2_p) else np.nan,
            "supuestos": {"normalidad": norm_df, "homocedasticidad": homo},
            "which": "anova", "N": int(N), "k": int(k)
        }
    else:
        valid = [a for a in arrs if len(a) > 0]
        if len(valid) < 2:
            return {
                "test": "Insuficiente para ANOVA/Kruskal", "stat": np.nan, "p": np.nan,
                "effect_label": None, "effect": np.nan,
                "supuestos": {"normalidad": norm_df, "homocedasticidad": homo}
            }
        H, p = kruskal(*arrs)
        eps2 = (H - (k - 1)) / (N - k) if (N - k) != 0 else np.nan
        return {
            "test": "Kruskal–Wallis (no paramétrico)", "stat_label": "H", "stat": float(H), "p": float(p),
            "effect_label": "epsilon^2", "effect": float(eps2) if not np.isnan(eps2) else np.nan,
            "supuestos": {"normalidad": norm_df, "homocedasticidad": homo},
            "which": "kruskal", "N": int(N), "k": int(k)
        }

def posthoc(df, y_col, x_col, which):
    """Post-hoc: ANOVA -> Tukey HSD; Kruskal -> Dunn (Holm)."""
    if which == "anova":
        sub = df[[y_col, x_col]].dropna()
        try:
            tuk = pairwise_tukeyhsd(endog=sub[x_col].values, groups=sub[y_col].astype(str).values, alpha=ALPHA)
            return pd.DataFrame(data=tuk._results_table.data[1:], columns=tuk._results_table.data[0])
        except Exception as e:
            print(f"[Aviso] No se pudo ejecutar Tukey HSD: {e}")
            return None
    elif which == "kruskal":
        sub = df[[y_col, x_col]].dropna()
        try:
            dunn = sp.posthoc_dunn(sub, val_col=x_col, group_col=y_col, p_adjust='holm')
            dunn.index = dunn.index.map(lambda v: f"Grupo {int(v)}")
            dunn.columns = dunn.columns.map(lambda v: f"Grupo {int(v)}")
            return dunn
        except Exception as e:
            print(f"[Aviso] No se pudo ejecutar Dunn: {e}")
            return None
    else:
        return None

def boxplot_por_severidad(sub_df: pd.DataFrame, var: str, out_dir=OUT_DIR, save=SAVE_PLOTS):
    """
    Boxplot estándar (sin notch, sin relleno personalizado) por niveles de
    'ultima severidad' (1=Menor, 2=Moderada, 3=Mayor). Muestra outliers.
    """
    # Preparar datos (en orden 1-2-3)
    data, labels = [], []
    nombre_map = {1: "Menor", 2: "Moderada", 3: "Mayor"}
    for sev in [1, 2, 3]:
        arr = sub_df.loc[sub_df["ultima severidad"] == sev, var].dropna().values
        data.append(arr)
        labels.append(f"{nombre_map[sev]} ({sev})\n n={len(arr)}")

    if all(len(a) == 0 for a in data):
        print(f"[Aviso] Sin datos para boxplot en variable '{var}'.")
        return

    fig, ax = plt.subplots(figsize=(7.5, 5.2))
    ax.boxplot(
        data,
        labels=labels,
        notch=False,        # sin muesca
        showfliers=True,    # muestra outliers
        patch_artist=False  # estilo estándar
    )
    # --- Título SIN "(1<2<3)" ---
    ax.set_title(f"Boxplot de {var} por nivel de 'ultima severidad'")
    ax.set_ylabel(var)
    ax.grid(axis='y', linestyle='--', alpha=0.25)  # quita esta línea si quieres sin grilla
    plt.tight_layout()

    if save:
        os.makedirs(out_dir, exist_ok=True)
        fname = f"boxplot_{slug(var).replace(' ','_')}_por_ultima_severidad.png"
        path = os.path.join(out_dir, fname)
        plt.savefig(path, dpi=200, bbox_inches="tight")
        print(f"Boxplot guardado: {path}")
    plt.show()

# ---------- Carga de datos ----------
xlsx_path = find_latest_xlsx(FILE_GLOB)
print(f"Archivo seleccionado: {xlsx_path}")
df = pd.read_excel(xlsx_path, sheet_name=SHEET_INDEX)

# ---------- Detección robusta de columnas ----------
sev_col = map_column(df, EXPECTED_SEVERITY, fallbacks_tokens=["ultima", "severidad"])
time_col = map_column(df, EXPECTED_TIME,     fallbacks_tokens=["tiempo", "minutos"])
km_col   = map_column(df, EXPECTED_KM,       fallbacks_tokens=["km", "kilometros"])

print(f"Columna de severidad original: {sev_col}")
print(f"Columna tiempo: {time_col}")
print(f"Columna km: {km_col}")

# ---------- Crear "ultima severidad" ----------
df["ultima severidad"] = map_ultima_severidad(df[sev_col])

# Reporte de mapeo
print("\nTabla de conteo 'ultima severidad' (1=menor, 2=moderada, 3=mayor):")
print(df["ultima severidad"].value_counts(dropna=False).sort_index())

# ---------- Análisis por variable de accesibilidad ----------
vars_acceso = [time_col, km_col]
resultados = {}

for var in vars_acceso:
    print("\n" + "="*70)
    print(f"VARIABLE: {var}  vs  'ultima severidad' (1,2,3)")
    print("="*70)

    # Sólo filas con datos en sev y var
    sub = df[["ultima severidad", var]].dropna()
    sub[var] = pd.to_numeric(sub[var], errors='coerce')
    sub = sub.dropna()

    # Descriptivos por grupo
    desc = sub.groupby("ultima severidad")[var].agg(["count", "mean", "std", "median", "min", "max"])
    print("\nDescriptivos por grupo:")
    display(desc)

    # ---- Pruebas de medias (ANOVA/Kruskal) con supuestos ----
    arrs = group_arrays(sub, "ultima severidad", var)
    test_info = anova_or_kruskal(arrs)

    print("\nChequeo de supuestos:")
    print("- Normalidad (Shapiro–Wilk) por grupo:")
    display(test_info["supuestos"]["normalidad"])
    levF, levp = test_info["supuestos"]["homocedasticidad"]["Levene"]
    flX2, flp = test_info["supuestos"]["homocedasticidad"]["Fligner"]
    print(f"- Levene (mediana): F={levF:.4f}  p={levp:.4f}")
    print(f"- Fligner–Killeen:  X^2={flX2:.4f}  p={flp:.4f}")
    print("(*) La independencia se asume por diseño del estudio/datos.")

    print("\nPrueba principal:")
    print(f"{test_info['test']}: {test_info['stat_label']}={test_info['stat']:.4f}  p={test_info['p']:.4f}  "
          f"({test_info['effect_label']}={test_info['effect']:.4f} ; N={test_info['N']}, k={test_info['k']})")

    if test_info["p"] < ALPHA and test_info["which"] in ("anova", "kruskal"):
        print("\nPost-hoc (comparaciones por pares):")
        ph = posthoc(sub, "ultima severidad", var, test_info["which"])
        if ph is not None:
            display(ph)
        else:
            print("No se pudo calcular el post-hoc.")
    else:
        print("No se aplica post-hoc (p >= α).")

    # ---- Boxplot estándar por nivel ordinal ----
    print("\nGenerando boxplot por nivel ordinal...")
    boxplot_por_severidad(sub, var, out_dir=OUT_DIR, save=SAVE_PLOTS)

    resultados[var] = {"descriptivos": desc, "prueba": test_info}

# ---------- Correlaciones (Pearson, Spearman y Kendall) ----------
print("\n" + "="*70)
print("CORRELACIONES entre 'ultima severidad' y variables de accesibilidad")
print("="*70)

corr_rows = []
for var in vars_acceso:
    sub = df[["ultima severidad", var]].dropna()
    sub[var] = pd.to_numeric(sub[var], errors='coerce')
    sub = sub.dropna()
    if len(sub) >= 3:
        sev = sub["ultima severidad"].astype(float).values
        x   = sub[var].values
        # Pearson
        try:
            r_p, p_p = pearsonr(sev, x)
        except Exception:
            r_p, p_p = (np.nan, np.nan)
        # Spearman
        try:
            r_s, p_s = spearmanr(sev, x)
        except Exception:
            r_s, p_s = (np.nan, np.nan)
        # Kendall tau-b
        try:
            tau_k, p_k = kendalltau(sev, x, nan_policy='omit')
        except Exception:
            tau_k, p_k = (np.nan, np.nan)

        corr_rows.append({
            "Variable": var,
            "N": len(sub),
            "Pearson_r": r_p,   "Pearson_p": p_p,
            "Spearman_rho": r_s, "Spearman_p": p_s,
            "Kendall_tau": tau_k, "Kendall_p": p_k
        })
    else:
        corr_rows.append({
            "Variable": var, "N": len(sub),
            "Pearson_r": np.nan, "Pearson_p": np.nan,
            "Spearman_rho": np.nan, "Spearman_p": np.nan,
            "Kendall_tau": np.nan, "Kendall_p": np.nan
        })

corr_df = pd.DataFrame(corr_rows)
display(corr_df)