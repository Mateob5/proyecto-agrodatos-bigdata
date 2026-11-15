"""
Limpieza raw -> trusted para el dataset Arta (4 estaciones x 6 variables, CSV sin encabezado).

Uso (ejemplo):
  py limpieza_trusted.py --entrada .\\extraido --salida .\\salida --inicio 2024-01-01 --fin 2025-08-26

Salidas (Parquet) dentro de --salida:
  trusted/      una fila por (lote_id, timestamp) con las 6 variables en columnas (formato ancho)
  rechazados/   registros descartados (largo) con la columna motivo_rechazo
  metricas/     JSON con los conteos para la Tabla 7 y la prueba de conservacion de registros
"""
import argparse
import os
import time

from pyspark.sql import SparkSession, Window, functions as F, types as T

# ---------------------------------------------------------------- configuracion
VARIABLES = {  # nombre en el archivo -> columna en la tabla ancha
    "air-temperature": "temperatura",
    "relative-humidity": "humedad_aire",
    "rainfall": "precipitacion",
    "soil-moisture-10cm": "humedad_suelo_10cm",
    "soil-moisture-30cm": "humedad_suelo_30cm",
    "soil-moisture-50cm": "humedad_suelo_50cm",
}
RANGOS = {  # (minimo, maximo); None = sin limite
    "air-temperature": (-10.0, 50.0),
    "relative-humidity": (0.0, 100.0),
    "rainfall": (0.0, None),
    "soil-moisture-10cm": (0.0, 100.0),
    "soil-moisture-30cm": (0.0, 100.0),
    "soil-moisture-50cm": (0.0, 100.0),
}
COLS_IMPUTABLES = [c for v, c in VARIABLES.items() if v != "rainfall"]  # la lluvia NUNCA se interpola
MAX_VACIO = 3  # lecturas consecutivas faltantes que se pueden interpolar (3 x 10 min = 30 min)

# Periodo que el readme declara invalido para la humedad del suelo de Vigla (traslado de la estacion).
# El readme no da fechas exactas: se asume julio de 2017. CONFIRMAR en OpenHI.
VIGLA_INVALIDO = ("2017-07-01", "2017-08-01")


def crear_spark(memoria="4g", nucleos="4"):
    # En modo local el limite por defecto es 1 GB de memoria y TODOS los nucleos: con muchos nucleos
    # se lanzan decenas de tareas a la vez y Spark se queda sin memoria. Aqui se fijan ambos.
    b = (SparkSession.builder.appName("limpieza_trusted_arta")
         .master(f"local[{nucleos}]")
         .config("spark.driver.memory", memoria)
         .config("spark.sql.session.timeZone", "UTC")  # las fechas se tratan como hora local, sin conversion
         .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
         .config("spark.sql.shuffle.partitions", "8"))
    # Opcional: leer/escribir en MinIO (rutas s3a://). NO PROBADO en este entorno.
    if os.environ.get("MINIO_ENDPOINT"):
        b = (b.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,"
                                             "com.amazonaws:aws-java-sdk-bundle:1.12.262")
             .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT"])
             .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ACCESS_KEY"])
             .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_SECRET_KEY"])
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false"))
    return b.getOrCreate()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--entrada", required=True, help="carpeta con los 24 CSV (o s3a://raw/...)")
    ap.add_argument("--salida", required=True, help="carpeta de salida")
    ap.add_argument("--inicio", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--fin", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--memoria", default="4g", help="memoria del driver de Spark (ej. 4g, 6g)")
    ap.add_argument("--nucleos", default="4", help="nucleos locales de Spark (ej. 2, 4)")
    a = ap.parse_args()
    t0 = time.time()

    spark = crear_spark(a.memoria, a.nucleos)
    spark.sparkContext.setLogLevel("ERROR")

    # ------------------------------------------------------------ 1) lectura (sin encabezado, 3 columnas)
    esquema = T.StructType([
        T.StructField("datetime", T.StringType()),
        T.StructField("value", T.StringType()),
        T.StructField("extra", T.StringType()),  # la coma final genera una tercera columna vacia
    ])
    raw = (spark.read.schema(esquema).option("header", "false")
           .option("recursiveFileLookup", "true").option("pathGlobFilter", "*.csv")
           .csv(a.entrada)
           .withColumn("archivo", F.regexp_extract(F.input_file_name(), r"([^/\\]+)$", 1))
           .drop("extra"))
    raw = (raw.withColumn("estacion", F.regexp_extract("archivo", r"^([a-z]+)_", 1))
              .withColumn("variable", F.regexp_extract(
                  "archivo", r"^[a-z]+_(air-temperature|relative-humidity|rainfall|soil-moisture-\d+cm)_", 1)))

    # ------------------------------------------------------------ 2) tipos
    base = (raw.withColumn("ts", F.to_timestamp("datetime", "yyyy-MM-dd HH:mm"))
               .withColumn("valor", F.col("value").cast("double"))
               # marcas fuera de la malla de 10 min se llevan al intervalo de 10 min que las contiene
               .withColumn("ts10", F.timestamp_seconds(F.floor(F.unix_timestamp("ts") / 600) * 600)))
    base = base.persist()
    n_raw = base.count()

    ini = F.lit(a.inicio).cast("timestamp")
    fin = F.lit(a.fin).cast("timestamp") + F.expr("interval 1 day")

    invalidas = base.filter(F.col("ts").isNull())
    con_fecha = base.filter(F.col("ts").isNotNull())
    en_ventana = con_fecha.filter((F.col("ts10") >= ini) & (F.col("ts10") < fin))
    n_fuera_ventana = con_fecha.count() - en_ventana.count()

    # ------------------------------------------------------------ 3) deduplicacion (estacion, variable, intervalo)
    w_dup = Window.partitionBy("estacion", "variable", "ts10").orderBy(
        F.col("valor").isNotNull().desc(), F.col("ts").desc())
    numerado = en_ventana.withColumn("rn", F.row_number().over(w_dup))
    duplicados = numerado.filter(F.col("rn") > 1)
    unicos = numerado.filter(F.col("rn") == 1).drop("rn")

    # ------------------------------------------------------------ 4) clasificacion y validacion de rangos
    fuera = F.lit(False)
    for var, (lo, hi) in RANGOS.items():
        c = F.col("variable") == var
        if lo is not None:
            fuera = fuera | (c & (F.col("valor") < lo))
        if hi is not None:
            fuera = fuera | (c & (F.col("valor") > hi))
    periodo_inv = ((F.col("estacion") == "vigla") & F.col("variable").startswith("soil-moisture")
                   & (F.col("ts10") >= F.lit(VIGLA_INVALIDO[0]).cast("timestamp"))
                   & (F.col("ts10") < F.lit(VIGLA_INVALIDO[1]).cast("timestamp")))
    vacio = F.col("value").isNull() | (F.trim(F.col("value")) == "")

    clasif = unicos.withColumn(
        "estado",
        F.when(vacio, "vacio")
         .when(F.col("valor").isNull() | F.isnan("valor"), "no_numerico")
         .when(periodo_inv, "periodo_invalido")
         .when(fuera, "fuera_de_rango")
         .otherwise("valido")).persist()

    conteos = {r["estado"]: r["n"] for r in clasif.groupBy("estado").agg(F.count("*").alias("n")).collect()}
    n_vacio = conteos.get("vacio", 0)
    n_nonum = conteos.get("no_numerico", 0)
    n_percinv = conteos.get("periodo_invalido", 0)
    n_rango = conteos.get("fuera_de_rango", 0)
    n_valido = conteos.get("valido", 0)
    n_invalidas = invalidas.count()
    n_dup = duplicados.count()

    # ------------------------------------------------------------ 5) tabla de rechazados (trazabilidad)
    cols_rech = ["archivo", "estacion", "variable", "datetime", "value"]
    rechazados = (
        invalidas.select(*cols_rech, F.lit("fecha_invalida").alias("motivo_rechazo"))
        .unionByName(duplicados.select(*cols_rech, F.lit("duplicado").alias("motivo_rechazo")))
        .unionByName(clasif.filter(F.col("estado").isin("no_numerico", "periodo_invalido", "fuera_de_rango"))
                     .select(*cols_rech, F.col("estado").alias("motivo_rechazo"))))

    # ------------------------------------------------------------ 6) pivote a formato ancho
    validos = clasif.filter(F.col("estado") == "valido")
    ancho = (validos.groupBy("estacion", "ts10")
             .pivot("variable", list(VARIABLES)).agg(F.first("valor")))
    for var, col in VARIABLES.items():
        ancho = ancho.withColumnRenamed(var, col)

    # ------------------------------------------------------------ 7) malla regular de 10 min por estacion
    estaciones = ancho.select("estacion").distinct()
    malla = (estaciones
             .withColumn("ts10", F.explode(F.expr(
                 f"sequence(timestamp'{a.inicio} 00:00:00', timestamp'{a.fin} 23:50:00', interval 10 minutes)"))))
    t = malla.join(ancho, ["estacion", "ts10"], "left").withColumn("ts_s", F.unix_timestamp("ts10"))

    # ------------------------------------------------------------ 8) imputacion controlada (vacios <= MAX_VACIO)
    w = Window.partitionBy("estacion").orderBy("ts10")
    # Ambas ventanas son acumulativas "hacia adelante" (lineales). Para el siguiente valor valido se recorre
    # la serie en orden inverso: una ventana "currentRow..unboundedFollowing" es cuadratica en Spark.
    wp = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    wn = (Window.partitionBy("estacion").orderBy(F.col("ts10").desc())
          .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    flags = []
    for c in COLS_IMPUTABLES:
        prev_v = F.last(F.col(c), ignorenulls=True).over(wp)
        prev_t = F.last(F.when(F.col(c).isNotNull(), F.col("ts_s")), ignorenulls=True).over(wp)
        next_v = F.last(F.col(c), ignorenulls=True).over(wn)
        next_t = F.last(F.when(F.col(c).isNotNull(), F.col("ts_s")), ignorenulls=True).over(wn)
        faltan = (next_t - prev_t) / 600 - 1  # lecturas faltantes consecutivas alrededor de esta fila
        cond = (F.col(c).isNull() & prev_v.isNotNull() & next_v.isNotNull() & (faltan <= MAX_VACIO))
        interp = F.round(prev_v + (next_v - prev_v) * (F.col("ts_s") - prev_t) / (next_t - prev_t), 2)
        t = t.withColumn(f"_imp_{c}", cond.cast("int")).withColumn(c, F.when(cond, interp).otherwise(F.col(c)))
        flags.append(f"_imp_{c}")

    todas = list(VARIABLES.values())
    t = t.withColumn("valores_imputados", sum(F.col(f) for f in flags))
    t = t.withColumn("imputado", F.col("valores_imputados") > 0)
    sin_dato = F.lit(True)
    for c in todas:
        sin_dato = sin_dato & F.col(c).isNull()
    trusted = (t.filter(~sin_dato)
               .withColumnRenamed("estacion", "lote_id")
               .withColumnRenamed("ts10", "timestamp")
               .withColumn("suelo_unidad", F.when(F.col("lote_id") == "kampi", "mm").otherwise("%"))
               .select("lote_id", "timestamp", *todas, "imputado", "valores_imputados", "suelo_unidad")
               .persist())

    # ------------------------------------------------------------ 9) escritura
    trusted.write.mode("overwrite").partitionBy("lote_id").parquet(os.path.join(a.salida, "trusted"))
    rechazados.write.mode("overwrite").parquet(os.path.join(a.salida, "rechazados"))

    # ------------------------------------------------------------ 10) metricas (Tabla 7) y conservacion
    n_trusted = trusted.count()
    n_imputados = trusted.agg(F.sum("valores_imputados")).collect()[0][0] or 0
    suma = (n_invalidas + n_fuera_ventana + n_dup + n_vacio + n_nonum + n_percinv + n_rango + n_valido)
    en_vent_total = n_raw - n_invalidas - n_fuera_ventana
    m = {
        "filas_raw_leidas": n_raw,
        "fecha_invalida": n_invalidas,
        "fuera_de_ventana": n_fuera_ventana,
        "duplicados_eliminados": n_dup,
        "valores_vacios": n_vacio,
        "rechazados_no_numerico": n_nonum,
        "rechazados_periodo_invalido": n_percinv,
        "rechazados_fuera_de_rango": n_rango,
        "registros_validos": n_valido,
        "pct_validos_sobre_en_ventana": round(100.0 * n_valido / max(en_vent_total - n_dup, 1), 2),
        "conservacion_ok": bool(suma == n_raw),
        "suma_categorias": suma,
        "filas_trusted_ancho": n_trusted,
        "valores_imputados": int(n_imputados),
        "ventana": [a.inicio, a.fin],
        "tiempo_total_s": round(time.time() - t0, 1),
    }
    por_motivo = (rechazados.groupBy("estacion", "variable", "motivo_rechazo")
                  .agg(F.count("*").alias("n")).orderBy(F.desc("n")))
    print("\n=== METRICAS ===")
    for k, v in m.items():
        print(f"{k:34s} {v}")
    print("\n=== RECHAZOS POR ESTACION / VARIABLE / MOTIVO ===")
    por_motivo.show(60, truncate=False)

    # Se arma el DataFrame solo con literales de Spark (sin serializar objetos de Python): createDataFrame
    # desde una lista de Python falla en PySpark 3.5 con Python 3.14 (RecursionError al serializar).
    spark.range(1).select(*[F.lit(str(v)).alias(k) for k, v in m.items()]) \
        .coalesce(1).write.mode("overwrite").json(os.path.join(a.salida, "metricas"))
    por_motivo.coalesce(1).write.mode("overwrite").option("header", "true") \
        .csv(os.path.join(a.salida, "rechazos_por_motivo"))
    spark.stop()


if __name__ == "__main__":
    main()