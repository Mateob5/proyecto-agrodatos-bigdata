# Proyecto Big Data: Monitoreo Agroclimático y Humedad del Suelo

Pipeline de datos híbrido (batch + streaming simulado) para el monitoreo de 4 estaciones agroclimáticas, desarrollado como proyecto final del curso de Big Data de la Universidad Industrial de Santander (UIS).

## Descripción del Proyecto

El sistema procesa 13,77 millones de registros de telemetría agroclimática (frecuencia de 10 minutos, 2015-2026) de 4 estaciones. Limpia y valida los datos, calcula agregados diarios por lote, identifica tendencias de humedad del suelo, genera alertas por umbral y las expone en un dashboard interactivo.

Los datos son **reales y provienen de Grecia** (región de Arta), no de Colombia. Ver la sección "Fuente de Datos".

## Integrantes y Roles

| Integrante | Código | Rol |
|---|---|---|
| Juan Diego Pinzón | 2243604 | Coordinación y arquitectura |
| Juan José Serrano | 2243567 | Ingeniería de datos e ingesta (batch y streaming) |
| Jerónimo Andrés Bazán Rojas | 2243590 | Ingeniería de procesamiento Big Data |
| Diego Javier Quintero | 2243593 | Analítica y visualización |

## Arquitectura del Pipeline

El sistema implementa un Data Lake por capas y un flujo de eventos simulado (batch + streaming):

1. **Ingesta:** lectura batch de 24 archivos CSV (sin encabezado) y simulación de eventos en tiempo real con Apache Kafka.
2. **Almacenamiento (MinIO):** capas `raw` (cruda), `trusted` (validada, en Parquet) y `curated` (agregada).
3. **Procesamiento (PySpark):** limpieza, validación de rangos, imputación controlada, agregación diaria y alertas.
4. **Servido (PostgreSQL):** carga vía JDBC.
5. **Visualización (Metabase):** dashboard con vistas de evolución, comparación, alertas y resumen.

## Stack Tecnológico

- **Procesamiento:** PySpark 3.5.3 (Python 3.10 a 3.12 recomendado)
- **Streaming:** Apache Kafka y Zookeeper
- **Data Lake:** MinIO (compatible con S3)
- **Base de datos:** PostgreSQL 15
- **Dashboard:** Metabase
- **Infraestructura:** Docker y Docker Compose

## Estructura del Repositorio

```
.
├── docker-compose.yml
├── requirements.txt
├── scripts/
│   ├── limpieza_trusted.py      # Job 1: raw -> trusted
│   ├── curated_alertas.py       # Job 2: trusted -> curated (agregación y alertas)
│   ├── cargar_postgres.py       # Job 3: curated -> PostgreSQL (JDBC)
│   ├── kafka_simulador.py       # productor de eventos simulados
│   ├── consumidor_streaming.py  # consumidor del flujo simulado
│   ├── perfilar.py              # perfil de calidad por archivo (análisis previo)
│   └── huecos.py                # análisis de huecos por ventana (análisis previo)
└── data_samples/                # muestras pequeñas del formato original
```

## Fuente de Datos

**Ten-Minute Atmospheric and Soil-Moisture Time Series for the Arta Pilot, Greece (2015-2026).** Salmas, D., Kapsala, O., Giavis, I., Pachoulas, G. y Stylios, C. (2026). Zenodo. DOI: [10.5281/zenodo.20667128](https://doi.org/10.5281/zenodo.20667128). La licencia está indicada en la ficha del dataset en Zenodo.

- 24 archivos CSV = 4 estaciones (`kampi`, `kompoti`, `kostakioi`, `vigla`) x 6 variables (temperatura del aire, humedad relativa, lluvia y humedad del suelo a 10, 30 y 50 cm).
- Los archivos **no tienen encabezado**: cada línea es `YYYY-MM-DD HH:MM,valor,`. Un valor vacío es un dato faltante, nunca cero. No incluyen zona horaria.

---

## Guía de Instalación y Ejecución (Paso a Paso)

### 1. Requisitos Previos del Sistema

Asegúrate de tener instalado:

- **Docker y Docker Compose** (para la infraestructura).
- **Java 17 (JRE)** (requerido por PySpark).
- **Python 3.10 a 3.12** (recomendado). Con Python 3.14 apareció un error de serialización de PySpark 3.5 al crear DataFrames desde listas de Python; los scripts ya lo evitan, pero se recomienda una versión anterior.

### 2. Configuración del Entorno (Python)

Clona el repositorio, entra a la carpeta y crea un entorno virtual aislado:

```bash
git clone https://github.com/TU_USUARIO/TU_REPOSITORIO.git
cd TU_REPOSITORIO

# Crear y activar el entorno virtual
python3 -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt
```

### 3. Preparación de los Datos (para evaluadores)

Por su tamaño, los 24 archivos CSV originales (13,77 millones de filas) **no están en el repositorio**. Para replicar la ejecución desde cero:

1. Descarga el dataset desde [Zenodo (DOI: 10.5281/zenodo.20667128)](https://doi.org/10.5281/zenodo.20667128).
2. Crea una carpeta llamada `extraido/` en la raíz del proyecto.
3. Extrae los 24 archivos CSV dentro de esa carpeta (el job los busca de forma recursiva).

En la carpeta `data_samples/` hay muestras pequeñas con el formato original.

### 4. Levantar la Infraestructura

```bash
docker compose up -d
docker compose ps
```

Espera a que todos los servicios estén activos. Servicios y puertos (credenciales de **desarrollo local**, no usar en producción):

| Servicio | Acceso |
|---|---|
| PostgreSQL | `localhost:5432` (usuario `agro`, contraseña `agro`, base `agro`) |
| MinIO | API `localhost:9000`, consola `http://localhost:9001` (usuario `minioadmin`) |
| Metabase | `http://localhost:3000` |
| Kafka | `localhost:9092` |

### 5. Ejecutar el Pipeline Batch (Procesamiento Histórico)

Se ejecutan los tres jobs en orden. La corrida oficial usa todo el periodo disponible (2015-02-19 a 2026-07-27):

```bash
# Job 1: limpieza y validación (raw -> trusted)
python scripts/limpieza_trusted.py --entrada extraido --salida salida_completa \
  --inicio 2015-02-19 --fin 2026-07-27 --memoria 4g --nucleos 4

# Job 2: agregación diaria y alertas (trusted -> curated)
python scripts/curated_alertas.py --salida salida_completa \
  --excluir-humedad-lotes "kampi,kostakioi" \
  --umbrales-lote "kompoti=7.0,vigla=8.0" --umbral-temp 35

# Job 3: carga a PostgreSQL (JDBC)
python scripts/cargar_postgres.py --salida salida_completa \
  --host localhost --db agro --user agro --password agro
```

Para una prueba rápida de desarrollo se puede usar una ventana corta (por ejemplo `--inicio 2024-01-01 --fin 2025-08-26 --salida salida`). Los resultados reportados en el informe corresponden a la corrida completa.

Cada job imprime sus métricas (`=== METRICAS ===`). El Job 1 verifica la conservación de registros: raw = fecha inválida + fuera de ventana + duplicados + vacíos + rechazados + válidos.

### 6. Simular el Streaming

Abre **dos terminales separadas** (ambas con el entorno virtual activado):

```bash
# Terminal 1: consumidor
python scripts/consumidor_streaming.py

# Terminal 2: productor de eventos simulados
python scripts/kafka_simulador.py
```

El flujo es **simulado**: reproduce lecturas del mismo dataset histórico como si llegaran en vivo. No hay sensores físicos conectados.

### 7. Visualización en el Dashboard

1. Abre `http://localhost:3000` en tu navegador.
2. Completa el asistente de configuración inicial de Metabase.
3. Cuando te pida conectar una base de datos, elige **PostgreSQL** e ingresa los datos:
   - Host: `postgres` (importante: usa el nombre del servicio de Docker, no `localhost`)
   - Puerto: `5432`
   - Database: `agro`
   - Usuario: `agro`
   - Contraseña: `agro`
4. Crea las cuatro vistas del proyecto: evolución de la humedad por lote, comparación entre lotes, tabla de alertas y resumen diario o semanal. En las comparaciones de humedad, filtra `calidad_humedad = 'ok'`.

---

## Decisiones Técnicas de Calidad de Datos

- **Reglas de validación:** temperatura de -10 a 50 °C; humedad relativa y del suelo de 0 a 100 %; lluvia mayor o igual a 0. Los valores fuera de rango se envían a una tabla de rechazados con su motivo.
- **Imputación controlada:** interpolación lineal solo si faltan 3 lecturas consecutivas o menos (30 min), y solo para temperatura, humedad del aire y humedad del suelo. **La lluvia nunca se interpola.** Un valor rechazado por rango deja un hueco que también puede imputarse si cumple esa regla.
- **Kampi:** su humedad del suelo está en milímetros, no en porcentaje, por lo que no es comparable con las demás estaciones. Queda fuera de las comparaciones y de las alertas de humedad.
- **Kostakioi:** el sensor de humedad del suelo a 30 cm resultó plano en 2024-2025 (101 valores únicos en unas 87.000 lecturas; racha máxima de 6.595 lecturas idénticas). Se excluye de las alertas de humedad y se mantiene para temperatura.
- **Umbrales por lote:** el umbral global del 30 % no es aplicable (la mediana real de humedad está entre 11 % y 21 %). Se usa un umbral por lote igual al percentil 5 de la humedad a 30 cm en la ventana 2024-2025 (Kompoti 7,0 %, Vigla 8,0 %). Es una alerta de **anomalía relativa** a la línea base de cada lote, no un punto de marchitez; la tasa de alertas en el periodo completo es la que reporta el job.
- **Lluvia:** cada lectura se interpreta como los milímetros caídos en el intervalo de 10 minutos y se suma para el acumulado diario.
- **Hora:** las marcas de tiempo se tratan como hora local del sitio, sin conversión de zona horaria.
- **Vigla, julio de 2017:** el readme del dataset indica que la humedad del suelo fue inválida durante el traslado de la estación; el job rechaza ese mes completo (el mes exacto es un supuesto).

## Limitaciones

- Spark se ejecuta en un solo nodo local, no en un clúster.
- El streaming es simulado a partir del mismo dataset.
- Con Kampi y Kostakioi fuera, las comparaciones y alertas de humedad cubren solo Kompoti y Vigla.
- El dataset es de Grecia; la integración de datos colombianos (por ejemplo IDEAM) queda como trabajo futuro.

## Solución de Problemas

- **Puerto 5432 en uso:** hay un PostgreSQL local activo. Detenlo con `sudo systemctl stop postgresql` antes de levantar Docker.
- **MinIO no descarga la imagen:** revisa la línea `image:` de MinIO en `docker-compose.yml` y usa la versión fija con la que se probó el proyecto.
- **Error de Java en PySpark:** comprueba `java -version`. Si falta, instala `openjdk-17-jre-headless` (`sudo apt install openjdk-17-jre-headless`).
- **Falta de memoria en Spark:** sube `--memoria` (por ejemplo `6g`) o baja `--nucleos`.