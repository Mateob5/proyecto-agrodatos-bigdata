"""
Capa curated: agregacion diaria por lote y alertas por umbral (trusted -> curated).

Uso:
  # ventana de desarrollo (calibracion hecha con esta ventana)
  python scripts/curated_alertas.py --salida salida \
      --excluir-humedad-lotes "kampi,kostakioi" \
      --umbrales-lote "kompoti=7.0,vigla=8.0" \
      --umbral-temp 35

  # corrida oficial (mismo umbral, ahora sobre todo el periodo)
  python scripts/curated_alertas.py --salida salida_completa \
      --excluir-humedad-lotes "kampi,kostakioi" \
      --umbrales-lote "kompoti=7.0,vigla=8.0" \
      --umbral-temp 35

Umbrales de humedad calibrados con la ventana 2024-01-01 -> 2025-08-26
(p05 de humedad_suelo_30cm por lote):
  kompoti   : 7.0 %
  vigla     : 8.0 %
  kostakioi : sensor plano (101 valores unicos en 87k lecturas, racha
              maxima 6595 lecturas = 45.8 dias, rango diario mediano
              0.10 %) -> EXCLUIDO de las alertas de humedad
  kampi     : humedad en mm, no comparable -> EXCLUIDO

Entrada : <salida>/trusted   (Parquet generado por limpieza_trusted.py)
Salidas : <salida>/curated/humedad_diaria_lote      una fila por (lote_id, fecha)
          <salida>/curated/alertas                   una fila por lectura de 10 min en alerta
          <salida>/curated/alertas_diarias           una fila por (lote_id, fecha, tipo_alerta)
          <salida>/curated/percentiles_por_lote.csv  calibracion (para el informe)
          <salida>/curated/metricas_curated          conteos para la Tabla 7
"""
import argparse
import os
import sys
import time

import pandas as pd
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from limpieza_trusted import crear_spark  # noqa: E402

PROFUNDIDADES = ["10cm", "30cm", "50cm"]
LECTURAS_POR_DIA = 144  # 24 h x 6 lecturas/hora


def _parse_csv_set(s):
    return set(x.strip() for x in (s or "").split(",") if x.strip())


def _parse_umbrales(s):
    out = {}
    for par in (s or "").split(","):
        par = par.strip()
        if not par:
            continue
        lote, val = par.split("=")
        out[lote.strip()] = float(val.strip())
    return out


def _in(colname, values):
    return F.lit(False) if not values else F.col(colname).isin(sorted(values))


def _not_in(colname, values):
    return F.lit(True) if not values else ~F.col(colname).isin(sorted(values))


def _umbral_col(lotes_umbrales, default):
    """Columna con el umbral por lote (constantes) y fallback escalar."""
    if not lotes_umbrales:
        return F.lit(default).cast("double")
    expr = None
    for lote, u in lotes_umbrales.items():
        cond = F.col("lote_id") == lote
        expr = F.when(cond, F.lit(u)) if expr is None else expr.when(cond, F.lit(u))
    return expr.otherwise(F.lit(default).cast("double")).cast("double")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--salida", required=True)
    ap.add_argument("--umbral-humedad", type=float, default=10.0,
                    help="umbral de respaldo si un lote no tiene valor en --umbrales-lote (puntos porcentuales)")
    ap.add_argument("--umbrales-lote", default="",
                    help="umbrales por lote, formato 'kompoti=7.0,vigla=8.0' (constantes calibradas)")
    ap.add_argument("--excluir-humedad-lotes", default="kampi,kostakioi",
                    help="lotes fuera de las alertas de humedad (Kampi por mm, Kostakioi por sensor plano)")
    ap.add_argument("--lotes-sensor-plano", default="kostakioi",
                    help="lotes con sensor de humedad plano (se marcan en calidad_humedad)")
    ap.add_argument("--umbral-temp", type=float, default=35.0)
    ap.add_argument("--profundidad", default="30cm", choices=PROFUNDIDADES)
    ap.add_argument("--memoria", default="4g")
    ap.add_argument("--nucleos", default="4")
    a = ap.parse_args()
    t0 = time.time()

    umbrales_lote = _parse_umbrales(a.umbrales_lote)
    excluidos = _parse_csv_set(a.excluir_humedad_lotes)
    sensor_plano = _parse_csv_set(a.lotes_sensor_plano)

    spark = crear_spark(a.memoria, a.nucleos)
    spark.sparkContext.setLogLevel("ERROR")
    col_alerta = f"humedad_suelo_{a.profundidad}"

    t = spark.read.parquet(os.path.join(a.salida, "trusted")).withColumn("fecha", F.to_date("timestamp"))
    es_pct = F.col("suelo_unidad") == "%"
    umbral_col = _umbral_col(umbrales_lote, a.umbral_humedad)
    t = t.withColumn("umbral_humedad_lote", umbral_col).persist()

    # calidad del canal de humedad (para dashboard e informe)
    calidad = (F.when(~es_pct, F.lit("unidad_mm"))
               .when(_in("lote_id", sensor_plano), F.lit("sensor_plano"))
               .otherwise(F.lit("ok")))

    # ---- 1) agregacion diaria (se mantiene para TODOS los lotes) ----
    aggs = [F.count("*").alias("n_lecturas"),
            F.sum(F.col("valores_imputados")).alias("n_valores_imputados"),
            F.first("suelo_unidad").alias("suelo_unidad"),
            F.first(calidad).alias("calidad_humedad")]
    for d in PROFUNDIDADES:
        c = f"humedad_suelo_{d}"
        aggs += [F.avg(F.when(es_pct, F.col(c))).alias(f"{c}_prom"),
                 F.min(F.when(es_pct, F.col(c))).alias(f"{c}_min"),
                 F.max(F.when(es_pct, F.col(c))).alias(f"{c}_max")]
    aggs += [F.avg("temperatura").alias("temperatura_prom"),
             F.min("temperatura").alias("temperatura_min"),
             F.max("temperatura").alias("temperatura_max"),
             F.avg("humedad_aire").alias("humedad_aire_prom"),
             F.sum("precipitacion").alias("precipitacion_acum_mm"),
             F.count("precipitacion").alias("n_lecturas_lluvia")]
    diaria = (t.groupBy("lote_id", "fecha").agg(*aggs)
              .withColumn("pct_dia_cubierto", F.round(F.col("n_lecturas") / LECTURAS_POR_DIA * 100, 1))
              .orderBy("lote_id", "fecha"))

    # ---- 2) alertas por lectura ----
    base_cols = ["lote_id", "timestamp", "fecha", "imputado"]
    a_hum = (t.filter(es_pct
                      & _not_in("lote_id", excluidos)
                      & F.col(col_alerta).isNotNull()
                      & (F.col(col_alerta) < F.col("umbral_humedad_lote")))
             .select(*base_cols, F.lit("humedad_suelo_baja").alias("tipo_alerta"),
                     F.lit(col_alerta).alias("variable"),
                     F.col(col_alerta).alias("valor"),
                     F.col("umbral_humedad_lote").alias("umbral")))
    a_tmp = (t.filter(F.col("temperatura") > a.umbral_temp)
             .select(*base_cols, F.lit("temperatura_alta").alias("tipo_alerta"),
                     F.lit("temperatura").alias("variable"),
                     F.col("temperatura").alias("valor"),
                     F.lit(a.umbral_temp).alias("umbral")))
    alertas = a_hum.unionByName(a_tmp)

    # ---- 3) resumen diario de alertas (dashboard) ----
    alertas_dia = (alertas.groupBy("lote_id", "fecha", "tipo_alerta", "variable", "umbral")
                   .agg(F.count("*").alias("n_lecturas_en_alerta"),
                        F.round(F.min("valor"), 2).alias("valor_min"),
                        F.round(F.max("valor"), 2).alias("valor_max"),
                        F.min("timestamp").alias("primera_lectura"),
                        F.max("timestamp").alias("ultima_lectura"))
                   .orderBy("fecha", "lote_id"))

    # ---- 4) escritura ----
    out = os.path.join(a.salida, "curated")
    diaria.write.mode("overwrite").parquet(os.path.join(out, "humedad_diaria_lote"))
    alertas.write.mode("overwrite").parquet(os.path.join(out, "alertas"))
    alertas_dia.write.mode("overwrite").parquet(os.path.join(out, "alertas_diarias"))

    # ---- 5) percentiles por lote (incluye excluidos, para el informe) ----
    cand = [6, 8, 10, 12, 15, 20]
    pct_df = (t.filter(es_pct).groupBy("lote_id")
              .agg(F.count(F.col(col_alerta)).alias("n"),
                   F.round(F.expr(f"percentile_approx({col_alerta}, 0.05)"), 2).alias("p05"),
                   F.round(F.expr(f"percentile_approx({col_alerta}, 0.10)"), 2).alias("p10"),
                   F.round(F.expr(f"percentile_approx({col_alerta}, 0.50)"), 2).alias("mediana"),
                   F.round(F.expr(f"percentile_approx({col_alerta}, 0.95)"), 2).alias("p95"),
                   *[F.round(100 * F.avg((F.col(col_alerta) < u).cast("int")), 2).alias(f"bajo_{u}") for u in cand])
              .orderBy("lote_id"))

    if umbrales_lote:
        u_expr = None
        for lote, u in umbrales_lote.items():
            cond = F.col("lote_id") == lote
            u_expr = F.when(cond, F.lit(u)) if u_expr is None else u_expr.when(cond, F.lit(u))
        u_expr = u_expr.otherwise(F.lit(None).cast("double"))
    else:
        u_expr = F.lit(a.umbral_humedad).cast("double")

    pct_df = (pct_df
              .withColumn("umbral_usado", u_expr.cast("double"))
              .withColumn("excluido_de_alertas", _in("lote_id", excluidos).cast("int")))

    # Export CSV (Row.asDict -> pandas; sin createDataFrame ni toPandas, seguro en 3.14)
    pct_rows = pct_df.collect()
    pct_pd = pd.DataFrame([r.asDict() for r in pct_rows])
    pct_pd.to_csv(os.path.join(out, "percentiles_por_lote.csv"), index=False, encoding="utf-8-sig")

    # ---- 6) metricas ----
    n_trusted = t.count()
    n_lote_dia = diaria.count()
    n_alertas = alertas.count()
    n_alertas_dia = alertas_dia.count()
    por_tipo = {r["tipo_alerta"]: r["n"] for r in alertas.groupBy("tipo_alerta").agg(F.count("*").alias("n")).collect()}
    n_h = por_tipo.get("humedad_suelo_baja", 0)
    n_t = por_tipo.get("temperatura_alta", 0)
    n_lect_pct = t.filter(es_pct & _not_in("lote_id", excluidos) & F.col(col_alerta).isNotNull()).count()
    m = {
        "filas_trusted_leidas": n_trusted,
        "filas_lote_dia_curated": n_lote_dia,
        "alertas_por_lectura": n_alertas,
        "alertas_humedad_suelo_baja": n_h,
        "alertas_temperatura_alta": n_t,
        "alertas_diarias": n_alertas_dia,
        "pct_lecturas_humedad_en_alerta": round(100.0 * n_h / max(n_lect_pct, 1), 3),
        "pct_lecturas_temp_en_alerta": round(100.0 * n_t / max(n_trusted, 1), 3),
        "umbrales_lote": str(umbrales_lote) if umbrales_lote else f"global={a.umbral_humedad}",
        "umbral_temperatura": a.umbral_temp,
        "profundidad": a.profundidad,
        "excluidos_humedad": ",".join(sorted(excluidos)) or "ninguno",
        "sensor_plano": ",".join(sorted(sensor_plano)) or "ninguno",
        "tiempo_total_s": round(time.time() - t0, 1),
    }
    print("\n=== METRICAS CURATED ===")
    for k, v in m.items():
        print(f"{k:34s} {v}")

    print("\n=== % DE LECTURAS EN ALERTA DE HUMEDAD POR LOTE (no excluidos) ===")
    (t.filter(es_pct & _not_in("lote_id", excluidos) & F.col(col_alerta).isNotNull())
     .groupBy("lote_id")
     .agg(F.count("*").alias("n_lecturas"),
          F.round(100 * F.avg((F.col(col_alerta) < F.col("umbral_humedad_lote")).cast("int")), 2).alias("pct_en_alerta"),
          F.first("umbral_humedad_lote").alias("umbral"))
     .orderBy("lote_id").show(truncate=False))

    print("=== CALIDAD DEL CANAL DE HUMEDAD POR LOTE ===")
    (t.groupBy("lote_id").agg(F.first(calidad).alias("calidad_humedad"))
     .orderBy("lote_id").show(truncate=False))

    print(f"\n=== PERCENTILES DE {col_alerta} POR LOTE (guardado en curated/percentiles_por_lote.csv) ===")
    print(pct_pd.to_string(index=False))

    print("\n=== ALERTAS DIARIAS POR LOTE Y TIPO ===")
    (alertas_dia.groupBy("lote_id", "tipo_alerta")
     .agg(F.count("*").alias("dias_con_alerta"), F.sum("n_lecturas_en_alerta").alias("lecturas"))
     .orderBy("lote_id", "tipo_alerta").show(40, truncate=False))

    spark.range(1).select(*[F.lit(str(v)).alias(k) for k, v in m.items()]) \
        .coalesce(1).write.mode("overwrite").json(os.path.join(out, "metricas_curated"))
    spark.stop()


if __name__ == "__main__":
    main()