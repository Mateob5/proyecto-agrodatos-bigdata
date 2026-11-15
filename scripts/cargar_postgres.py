"""
Carga curated (Parquet) a PostgreSQL via JDBC.
No usa createDataFrame/toPandas/UDF -> seguro en cualquier version de Python.

Uso:
  PYSPARK_SUBMIT_ARGS="--packages org.postgresql:postgresql:42.7.4 pyspark-shell" \
  python scripts/cargar_postgres.py --salida salida_completa \
      --host localhost --db agro --user agro --password agro --modo overwrite

Si la tabla ya existe y --modo=overwrite: se hace TRUNCATE+INSERT (no se recrea la tabla).
El DDL previo (sql/01_schema.sql) es requisito.
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from limpieza_trusted import crear_spark  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--salida", required=True, help="carpeta con curated/ (misma que --salida de curated_alertas.py)")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=5432)
    ap.add_argument("--db", default="agro")
    ap.add_argument("--user", default="agro")
    ap.add_argument("--password", default="agro")
    ap.add_argument("--modo", default="overwrite", choices=["overwrite", "append"])
    ap.add_argument("--memoria", default="4g")
    ap.add_argument("--nucleos", default="4")
    a = ap.parse_args()
    t0 = time.time()

    spark = crear_spark(a.memoria, a.nucleos)
    spark.sparkContext.setLogLevel("ERROR")

    jdbc_url = f"jdbc:postgresql://{a.host}:{a.port}/{a.db}"
    props = {
        "user": a.user,
        "password": a.password,
        "driver": "org.postgresql.Driver",
        "batchsize": "5000",
        "truncate": "true" if a.modo == "overwrite" else "false",
    }

    cur = os.path.join(a.salida, "curated")
    tabla = [
        ("humedad_diaria_lote", "humedad_diaria_lote"),
        ("alertas",             "alertas"),
        ("alertas_diarias",     "alertas_diarias"),
    ]

    print(f"\n=== CARGA JDBC -> {jdbc_url} ===")
    resumen = []
    for carpeta, tabla_pg in tabla:
        ruta = os.path.join(cur, carpeta)
        df = spark.read.parquet(ruta)
        n = df.count()
        # Reescribir columnas en orden estable (Postgres no depende del orden,
        # pero ayuda a depurar y evita problemas de schema con arrays)
        df.write.jdbc(url=jdbc_url, table=tabla_pg, mode=a.modo, properties=props)
        resumen.append((tabla_pg, n))
        print(f"  {tabla_pg:22s}  filas_enviadas={n:>9,d}")

    print("\n=== RESUMEN ===")
    for t, n in resumen:
        print(f"  {t:22s}  {n:>9,d}")
    print(f"\ntiempo_total_s {round(time.time() - t0, 1)}")
    spark.stop()


if __name__ == "__main__":
    main()