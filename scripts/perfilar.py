import glob, os, sys
import pandas as pd

# Uso: python perfilar.py <carpeta_con_los_24_csv> <archivo_salida.csv>
base, salida = sys.argv[1], sys.argv[2]

RANGOS = {"air-temperature": (-10, 50), "relative-humidity": (0, 100),
          "rainfall": (0, None), "soil-moisture": (0, 100)}

filas = []
for f in sorted(glob.glob(os.path.join(base, "**", "*.csv"), recursive=True)):
    nombre = os.path.basename(f)
    partes = nombre.split("_")
    estacion, variable = partes[0], partes[1]
    df = pd.read_csv(f, header=None, usecols=[0, 1], names=["datetime", "value"], dtype=str)
    ts = pd.to_datetime(df["datetime"], format="%Y-%m-%d %H:%M", errors="coerce")
    val = pd.to_numeric(df["value"], errors="coerce")

    base_var = "soil-moisture" if variable.startswith("soil-moisture") else variable
    lo, hi = RANGOS.get(base_var, (None, None))
    aplicar = not (estacion == "kampi" and base_var == "soil-moisture")  # Kampi viene en mm
    fuera = 0
    if aplicar:
        if lo is not None: fuera += int((val < lo).sum())
        if hi is not None: fuera += int((val > hi).sum())

    t = ts.dropna().sort_values()
    d = t.diff().dropna()
    v = val.dropna()
    filas.append({
        "archivo": nombre, "estacion": estacion, "variable": variable,
        "filas": len(df),
        "fecha_invalida": int(ts.isna().sum()),
        "valor_vacio": int(df["value"].isna().sum()),
        "valor_no_numerico": int((df["value"].notna() & val.isna()).sum()),
        "fecha_min": t.min(), "fecha_max": t.max(),
        "fechas_duplicadas": int(ts.dropna().duplicated().sum()),
        "pct_intervalo_10min": round(float((d == pd.Timedelta("10min")).mean()) * 100, 1) if len(d) else None,
        "intervalo_max_horas": round(d.max().total_seconds() / 3600, 1) if len(d) else None,
        "val_min": v.min(), "p05": v.quantile(.05), "mediana": v.median(),
        "p95": v.quantile(.95), "val_max": v.max(),
        "fuera_de_rango": fuera if aplicar else "n/a (mm)",
    })

res = pd.DataFrame(filas)
res.to_csv(salida, index=False, encoding="utf-8-sig")
pd.set_option("display.width", 250); pd.set_option("display.max_columns", 30)
print(res.to_string(index=False))
print("\nTotal de filas:", res["filas"].sum())