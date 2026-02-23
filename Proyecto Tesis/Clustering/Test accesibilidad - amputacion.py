# ===========================================================
# SUPUESTOS + TEST DE MEDIAS (k = número real de clusters)


import os, glob, re, unicodedata, warnings
import numpy as np
import pandas as pd
from scipy.stats import (
    shapiro, kendalltau, spearmanr,
    levene, fligner, f_oneway, kruskal
)

warnings.filterwarnings("ignore")
ALPHA = 0.05
#'/content/drive/MyDrive/HPM/Bases de trabajo/BASE FINAL CON GRUPO*.xlsx'

# ---------- Config ----------
BASE_DIR = '/content/drive/MyDrive/HPM/Bases de trabajo'
PATTERN  = 'BASE FINAL CON GRUPO*.xlsx'
NEW_FILE_PATH = None  # opcional

# ---------- Helpers ----------
def normalize_col(s: str) -> str:
    t = ''.join(ch for ch in unicodedata.normalize('NFD', str(s)) if unicodedata.category(ch) != 'Mn')
    t = t.lower()
    return re.sub(r'[^0-9a-z]', '', t)

def find_col(df: pd.DataFrame, candidates) -> str | None:
    if isinstance(candidates, str): candidates = [candidates]
    norm_map = {normalize_col(c): c for c in df.columns}
    for cand in candidates:
        got = norm_map.get(normalize_col(cand))
        if got is not None: return got
    for cand in candidates:
        key = normalize_col(cand)
        for k, orig in norm_map.items():
            if key in k: return orig
    return None

def load_latest_with_pattern(base_dir: str, pattern: str) -> str:
    files = sorted(glob.glob(os.path.join(base_dir, pattern)), key=os.path.getmtime)
    if not files:
        raise FileNotFoundError(f"No se encontraron archivos con patrón '{pattern}' en {base_dir}.")
    return files[-1]

def eta_squared_anova(F, df_between, df_within):
    ss_between = F * df_between
    ss_within = df_within
    den = ss_between + ss_within
    return np.nan if den == 0 else ss_between / den

def _cluster_num_from_grupo(s: str) -> int:
    """
    Extrae el número del texto 'cluster N'. Si no puede, devuelve un número grande para
    mandarlo al final al ordenar.
    """
    if not isinstance(s, str):
        return 10**9
    m = re.search(r'cluster\s+(-?\d+)', s.strip().lower())
    return int(m.group(1)) if m else 10**9

# ---------- Cargar base ----------
if NEW_FILE_PATH is None:
    NEW_FILE_PATH = load_latest_with_pattern(BASE_DIR, PATTERN)
print(f"[Carga] {NEW_FILE_PATH}")
df = pd.read_excel(NEW_FILE_PATH)

# ---------- Asegurar 'Grupo' ----------
cg = find_col(df, 'Grupo')
if cg is None:
    copt = find_col(df, 'cluster_OPTICS')
    if copt is None: raise ValueError("La base no contiene 'Grupo' ni 'cluster_OPTICS'.")
    tmp = pd.to_numeric(df[copt], errors='coerce').round().astype('Int64')
    df['Grupo'] = np.where(tmp==-1, 'ruido', 'cluster '+tmp.astype(str))
else:
    if cg != 'Grupo': df.rename(columns={cg:'Grupo'}, inplace=True)

# ---------- Columnas métricas ----------
col_tiempo = find_col(df, ['Tiempo (minutos)','tiempo (minutos)','tiempo','tiempo minutos'])
col_km     = find_col(df, ['km','distancia (km)','distancia_km'])
base_vars = [
    'Numero de registros','Mayor','Menor','Moderada',
    'Ultima Edad Registrada','Total_Amputaciones'
]
present_base = [c for c in base_vars if c in df.columns]
if col_tiempo: present_base.append(col_tiempo)
if col_km:     present_base.append(col_km)

# Solo clusters (sin ruido)
d = df[df['Grupo'].astype(str).str.lower()!='ruido'].copy()
for v in present_base:
    d[v] = pd.to_numeric(d[v], errors='coerce')

# ====== USAR TODOS LOS CLUSTERS PRESENTES (orden natural por número) ======
todos = sorted(d['Grupo'].dropna().unique(), key=_cluster_num_from_grupo)
grupos = todos  # no truncar
k_real = len(grupos)
if k_real == 0:
    raise ValueError("No hay clusters válidos (solo 'ruido' o 'Grupo' vacío).")
print(f"Clusters usados (k={k_real}):", grupos)

# =============== Medias (referencia) ===============
tabla_medias = (
    d.loc[d['Grupo'].isin(grupos)].groupby('Grupo')[present_base].mean().round(3)
)
print("\n=== Medias por cluster ===")
print(tabla_medias.to_string())

# Variables a testear
vars_tests = [v for v in [col_tiempo, col_km] if v]

# ======== SUPUESTOS POR CLUSTER + LEVENE GLOBAL =========
normal_rows, indep_rows, lev_rows, flig_rows = [], [], [], []
lev_global_rows = []

for var in vars_tests:
    for g in grupos:
        xg = pd.to_numeric(d.loc[d['Grupo']==g, var], errors='coerce').dropna().values
        n  = len(xg)

        # -------- NORMALIDAD (Shapiro–Wilk) --------
        if n >= 3:
            W, p = shapiro(xg if n<=5000 else np.random.default_rng(42).choice(xg, 5000, replace=False))
            normal_rows.append({'variable':var,'grupo':g,'n':n,'test':'Shapiro-Wilk','W':float(W),'pvalue':float(p),
                                'nota':'No rechaza normalidad si p>=0.05'})
        else:
            normal_rows.append({'variable':var,'grupo':g,'n':n,'test':'Shapiro-Wilk','W':np.nan,'pvalue':np.nan,'nota':'n<3'})

        # -------- INDEPENDENCIA --------
        if n >= 3:
            idx = np.arange(n)
            # (a) Kendall
            tau, p_tau = kendalltau(idx, xg)
            # (b) Spearman
            rho, p_rho = spearmanr(idx, xg)
            indep_rows.append({
                'variable': var, 'grupo': g,
                'kendall_tau': float(tau), 'pvalue_kendall': float(p_tau),
                'spearman_rho': float(rho), 'pvalue_spearman': float(p_rho),
                'nota': 'No dependencia si p>=0.05 en ambos tests'
            })
        else:
            indep_rows.append({'variable':var,'grupo':g,'kendall_tau':np.nan,'pvalue_kendall':np.nan,
                               'spearman_rho':np.nan,'pvalue_spearman':np.nan,'nota':'n<3'})

        # -------- HOMOSCEDASTICIDAD UNO-VS-RESTO --------
        xrest = pd.to_numeric(d.loc[(d['Grupo']!=g) & (d['Grupo'].isin(grupos)), var], errors='coerce').dropna().values
        if len(xg)>=2 and len(xrest)>=2:
            Wl, pl = levene(xg, xrest, center='median')
            lev_rows.append({'variable':var,'grupo':g,'W':float(Wl),'pvalue':float(pl),
                             'nota':'No difiere de la varianza del resto si p>=0.05'})
            Wf, pf = fligner(xg, xrest)
            flig_rows.append({'variable':var,'grupo':g,'X2':float(Wf),'pvalue':float(pf),
                              'nota':'Fligner-Killeen uno-vs-resto; no difiere si p>=0.05'})
        else:
            lev_rows.append({'variable':var,'grupo':g,'W':np.nan,'pvalue':np.nan,'nota':'n<2 en grupo o resto'})
            flig_rows.append({'variable':var,'grupo':g,'X2':np.nan,'pvalue':np.nan,'nota':'n<2 en grupo o resto'})

    # -------- LEVENE GLOBAL (k dinámico) --------
    series_full = [pd.to_numeric(d.loc[d['Grupo']==g, var], errors='coerce').dropna().values for g in grupos]
    series_full = [s for s in series_full if len(s)>=2]
    if len(series_full) >= 2:
        Wg, pg = levene(*series_full, center='median')
        lev_global_rows.append({'variable':var, 'k': len(series_full), 'W':float(Wg), 'pvalue':float(pg),
                                'nota':'Levene GLOBAL; no rechaza si p>=0.05'})
    else:
        lev_global_rows.append({'variable':var, 'k': len(series_full), 'W':np.nan, 'pvalue':np.nan,
                                'nota':'Levene GLOBAL no aplicable (n<2 en algún grupo)'})

# ---- Mostrar resultados de supuestos ----
print("\n=== Normalidad por cluster (Shapiro-Wilk) ===")
print(pd.DataFrame(normal_rows).to_string(index=False))

print("\n=== Independencia (Kendall + Spearman) por cluster ===")
print(pd.DataFrame(indep_rows).to_string(index=False))

print("\n=== Homoscedasticidad UNO-VS-RESTO (Levene) ===")
print(pd.DataFrame(lev_rows).to_string(index=False))

print("\n=== Homoscedasticidad (Fligner UNO-VS-RESTO) ===")
print(pd.DataFrame(flig_rows).to_string(index=False))

print(f"\n=== Homoscedasticidad (Levene GLOBAL k={k_real}) ===")
print(pd.DataFrame(lev_global_rows).to_string(index=False))

# ===================== TEST DE MEDIAS (k = dinámico) =======================
res_anova, res_kw, decision = [], [], []

for var in vars_tests:
    subN = pd.DataFrame(normal_rows)
    subI = pd.DataFrame(indep_rows)
    subL = pd.DataFrame(lev_rows)

    flags_norm, flags_ind, flags_lev = [], [], []

    for g in grupos:
        pN = subN[(subN.variable==var) & (subN.grupo==g)]['pvalue'].values
        pK = subI[(subI.variable==var) & (subI.grupo==g)]['pvalue_kendall'].values
        pS = subI[(subI.variable==var) & (subI.grupo==g)]['pvalue_spearman'].values
        pL = subL[(subL.variable==var) & (subL.grupo==g)]['pvalue'].values

        flags_norm.append(len(pN)>0 and pd.notna(pN[0]) and pN[0]>=ALPHA)
        ok_ind = True
        if len(pK)>0 and pd.notna(pK[0]): ok_ind &= pK[0]>=ALPHA
        if len(pS)>0 and pd.notna(pS[0]): ok_ind &= pS[0]>=ALPHA
        flags_ind.append(ok_ind)
        flags_lev.append(len(pL)>0 and pd.notna(pL[0]) and pL[0]>=ALPHA)

    series = [pd.to_numeric(d.loc[d['Grupo']==g, var], errors='coerce').dropna().values for g in grupos]
    series = [x for x in series if len(x)>=2]

    if all(flags_norm) and all(flags_ind) and all(flags_lev) and len(series)>=2:
        F, pA = f_oneway(*series)
        k = len(series); N = sum(len(x) for x in series)
        eta2 = eta_squared_anova(F, k-1, N-k)
        res_anova.append({'variable':var,'k':k,'F':float(F),'pvalue':float(pA),'eta2':float(eta2)})
        decision.append({'variable':var,'ruta':'ANOVA (supuestos OK)','pvalue':float(pA)})
        print(f"\n[MEDIAS] {var}: ANOVA (k={k}) F={F:.3f}, p={pA:.4f}, eta²={eta2:.3f}")
    else:
        if len(series)>=2:
            H, pK = kruskal(*series)
            res_kw.append({'variable':var,'k':len(series),'H':float(H),'pvalue':float(pK)})
            decision.append({'variable':var,'ruta':'Kruskal–Wallis (no se cumplen todos los supuestos)','pvalue':float(pK)})
            print(f"\n[MEDIAS] {var}: Kruskal–Wallis (k={len(series)}) H={H:.3f}, p={pK:.4f}")
        else:
            decision.append({'variable':var,'ruta':'Sin prueba (n insuficiente)','pvalue':np.nan})
            print(f"\n[MEDIAS] {var}: Insuficiente n para prueba global.")

# ---- Resúmenes finales --------
if res_anova:
    print("\n=== Test de medias: ANOVA (cuando procede) ===")
    print(pd.DataFrame(res_anova).to_string(index=False))
if res_kw:
    print("\n=== Test de medias: Kruskal–Wallis (alternativa) ===")
    print(pd.DataFrame(res_kw).to_string(index=False))

print("\n=== Decisión por variable (ruta del test de medias) ===")
print(pd.DataFrame(decision).to_string(index=False))
