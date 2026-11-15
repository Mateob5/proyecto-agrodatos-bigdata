Casi está perfecto, pero **tiene un error de formato** que va a romper el README cuando lo veas en GitHub.

En el **Paso 2**, te faltó cerrar las tres comillas invertidas (```) del bloque de código. Por eso, todo el texto del Paso 3 y lo que sigue se va a ver como si fuera código de programación. Además, veo que se cortó el resto de la guía (faltan los pasos 4 al 7, las decisiones técnicas y el troubleshooting).

Para que no te compliques copiando y pegando pedazos, **aquí tienes el archivo README.md completo, corregido y listo para pegar**. 

Haz esto:
1. Abre tu archivo en VS Code (`README.md`).
2. Selecciona todo (`Ctrl + A`) y bórralo.
3. Copia y pega esto:

```markdown
# Proyecto Big Data: Monitoreo Agroclimático y Humedad del Suelo

Pipeline de datos híbrido (Batch + Streaming) para el monitoreo de 4 estaciones agroclimáticas, desarrollado como proyecto final para el curso de Big Data de la Universidad Industrial de Santander (UIS).

## 📋 Descripción del Proyecto
El sistema procesa 13.7 millones de registros de telemetría agroclimática (frecuencia de 10 minutos, 2015-2026) para identificar tendencias de humedad del suelo, generar alertas tempranas de estrés hídrico y visualizar el estado de los cultivos en un dashboard interactivo.

## 👥 Integrantes y Roles
- **Juan Diego Pinzón** (2243604) - Coordinación de proyecto y arquitectura.
- **Juan José Serrano** (2243567) - Ingeniería de datos e ingesta (Batch/Streaming).
- **Jerónimo Andrés Bazán Rojas** (2243590) - Ingeniería de procesamiento Big Data.
- **Diego Javier Quintero** (2243593) - Analítica y visualización.

## 🏗️ Arquitectura del Pipeline
El sistema implementa un patrón de Data Lake por capas y un enfoque Lambda (Batch + Streaming):
1. **Ingesta:** Lectura batch de 24 archivos CSV (sin encabezado) y simulación de eventos en tiempo real vía Apache Kafka.
2. **Almacenamiento (MinIO):** Capa `raw` (cruda), `trusted` (limpia en Parquet) y `curated` (agregada).
3. **Procesamiento (PySpark):** Limpieza, validación de rangos, imputación controlada y agregación diaria.
4. **Servido (PostgreSQL):** Carga vía JDBC con índices optimizados.
5. **Visualización (Metabase):** Dashboard con vistas de evolución, comparación, alertas y resumen.

## 🛠️ Stack Tecnológico
- **Procesamiento:** PySpark 3.5.3 (Python 3.12)
- **Streaming:** Apache Kafka & Zookeeper
- **Data Lake:** MinIO (S3A)
- **Base de Datos:** PostgreSQL 15
- **Dashboard:** Metabase
- **Infraestructura:** Docker & Docker Compose

---

## 🚀 Guía de Instalación y Ejecución (Paso a Paso)

### 1. Requisitos Previos del Sistema
Asegúrate de tener instalado:
- **Docker y Docker Compose** (para la infraestructura).
- **Java 17 (JRE)** (requerido por PySpark).
- **Python 3.12** (recomendado; versiones más nuevas pueden dar problemas con PySpark).

### 2. Configuración del Entorno (Python)
Clona el repositorio, entra a la carpeta y crea un entorno virtual aislado:
```bash
git clone https://github.com/TU_USUARIO/proyecto-agrodatos-uis.git
cd proyecto-agrodatos-uis

# Crear y activar el entorno virtual
python3.12 -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt
```

### 3. Preparación de los Datos (Para evaluadores)
Por razones de peso y buenas prácticas de la industria, los 24 archivos CSV originales (13.7M de filas) **no están en el repositorio**. 
Si deseas replicar la ejecución desde cero:
1. Descarga el dataset desde [Zenodo (DOI: 10.5281/zenodo.20667128)](https://doi.org/10.5281/zenodo.20667128).
2. Crea una carpeta llamada `extraido/` en la raíz del proyecto.
3. Extrae los 24 archivos CSV dentro de esa carpeta.
*(Nota: En la carpeta `data_samples/` dejamos un archivo de 50 registros como muestra del formato original).*

### 4. Levantar la Infraestructura
```bash
docker compose up -d
```
*Esto levantará los siguientes servicios en segundo plano:*
- **PostgreSQL:** `localhost:5432` (user: `agro`, pass: `agro`, db: `agro`)
- **MinIO:** `localhost:9000` (API) y `http://localhost:9001` (Consola Web, user: `minioadmin`)
- **Metabase:** `http://localhost:3000`
- **Kafka:** `localhost:9092`

*Nota: Espera unos 30 segundos a que todos los servicios inicialicen correctamente. Puedes verificar con `docker compose ps`.*

### 5. Ejecutar el Pipeline Batch (Procesamiento Histórico)
Ejecuta los jobs de PySpark en orden:
```bash
# 1. Limpieza y validación (Raw -> Trusted)
python scripts/limpieza_trusted.py --entrada extraido --salida salida --inicio 2024-01-01 --fin 2025-08-26

# 2. Agregación diaria y alertas (Trusted -> Curated)
python scripts/curated_alertas.py --salida salida --excluir-humedad-lotes "kampi,kostakioi" --umbrales-lote "kompoti=7.0,vigla=8.0"

# 3. Carga a PostgreSQL (JDBC)
python scripts/cargar_postgres.py --salida salida --host localhost --db agro --user agro --password agro
```

### 6. Simular el Streaming (Tiempo Real)
Abre **dos terminales separadas** (ambas con el entorno virtual activado):
```bash
# Terminal 1: Iniciar el consumidor (PySpark escuchando a Kafka y escribiendo en Postgres)
python scripts/consumidor_streaming.py

# Terminal 2: Enviar los datos (Productor)
python scripts/kafka_simulador.py
```
*Verás en la Terminal 1 cómo los micro-lotes se procesan y guardan exitosamente.*

### 7. Visualización en el Dashboard
1. Abre `http://localhost:3000` en tu navegador.
2. Completa el asistente de configuración inicial de Metabase.
3. Cuando te pida conectar una base de datos, elige **PostgreSQL** e ingresa los datos:
   - Host: `postgres` (¡importante, no uses localhost!)
   - Puerto: `5432`
   - Database: `agro`
   - Usuario: `agro`
   - Contraseña: `agro`
4. Explora las tablas cargadas y crea tus gráficos.

---

## 🧠 Decisiones Técnicas de Calidad de Datos
- **Exclusión de Kampi:** Su sensor mide humedad del suelo en milímetros (mm), no en porcentaje (%), haciéndolo incomparable con las demás estaciones.
- **Exclusión de Kostakioi:** Se detectó una degradación progresiva del sensor (rachas de hasta 6.595 lecturas idénticas). Se excluye de las alertas de humedad pero se mantiene para análisis de temperatura.
- **Umbrales Dinámicos:** En lugar de un umbral fijo (que generaba falsas alarmas), se utilizó el percentil 5 (p05) de cada lote como umbral de anomalía relativa (Kompoti 7.0%, Vigla 8.0%).
- **S3 Local:** Se utilizó MinIO para emular Amazon S3 localmente, garantizando que el código sea agnóstico a la nube y reproducible sin costos.

## 🐛 Solución de Problemas (Troubleshooting)
- **Error "port 5432 is already in use":** Tienes un PostgreSQL local corriendo. Deténlo con `sudo systemctl stop postgresql` antes de levantar Docker.
- **Error de imagen de MinIO:** Si Docker no descarga MinIO, verifica que el `docker-compose.yml` apunte a `cgr.dev/chainguard/minio:latest` (la imagen oficial cambió de registro recientemente).
- **Error de Java en PySpark:** Asegúrate de tener Java 17 (`java -version`). Si no, instálalo con `sudo apt install openjdk-17-jre-headless`.
```
