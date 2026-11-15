import glob, os, sys
import pandas as pd

# Uso: python huecos.py <carpeta_csv> <inicio> <fin> <salida.csv>
base, ini, fin, salida = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
ini, fin = pd.Timestamp(ini), pd.Timestamp(fin) + pd.Timedelta("1D")
UMBRAL = pd.Timedelta("6h")

res, huecos = [], []
for f in sorted(glob.glob(os.path.join(base, "**", "*.csv"), recursive=True)):
    n = os.path.basename(f); est, var = n.split("_")[0], n.split("_")[1]
    df = pd.read_csv(f, header=None, usecols=[0, 1], names=["datetime", "value"], dtype=str)
    df["ts"] = pd.to_datetime(df["datetime"], format="%Y-%m-%d %H:%M", errors="coerce")
    df["v"] = pd.to_numeric(df["value"], errors="coerce")
    w = df[(df["ts"] >= ini) & (df["ts"] < fin)].sort_values("ts")
    validos = w.dropna(subset=["v"])
    esperados = int((fin - ini) / pd.Timedelta("10min"))
    res.append({"estacion": est, "variable": var, "filas_en_ventana": len(w),
                "vacios": int(w["v"].isna().sum()),
                "pct_validos_vs_esperado": round(len(validos) / esperados * 100, 1)})
    d = validos["ts"].diff()
    for i in d[d > UMBRAL].index:
        huecos.append({"estacion": est, "variable": var,
                       "desde": validos.loc[validos.index[validos.index.get_loc(i) - 1], "ts"],
                       "hasta": validos.loc[i, "ts"], "horas": round(d[i].total_seconds() / 3600, 1)})
r = pd.DataFrame(res); h = pd.DataFrame(huecos)
pd.set_option("display.width", 220); pd.set_option("display.max_rows", 200)
print(r.to_string(index=False))
print("\nHuecos > 6 h entre valores válidos:", len(h))
if len(h): print(h.sort_values("horas", ascending=False).head(40).to_string(index=False))
r.to_csv(salida, index=False, encoding="utf-8-sig")
h.to_csv(salida.replace(".csv", "_detalle.csv"), index=False, encoding="utf-8-sig")