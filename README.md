# Batch Processing Pipeline con Apache Spark en AWS EMR

## Descripción del Proyecto
Este proyecto implementa un pipeline ETL distribuido y de análisis de datos utilizando **Apache Spark**. [cite_start]El objetivo principal es explorar la microestructura del mercado y la eficiencia de ejecución de **Polymarket**, un mercado de predicciones descentralizado[cite: 5]. 

[cite_start]Dado el gran volumen y la complejidad relacional de los datos (un conjunto de datos de 30 GB que se traduce en más de 5.6 mil millones de registros), las herramientas de procesamiento de un solo nodo resultan insuficientes[cite: 6, 70, 71]. [cite_start]Por ello, se diseñó una arquitectura de Big Data robusta para extraer métricas financieras mediante cruces temporales complejos[cite: 6, 7].

## Arquitectura
* [cite_start]**Procesamiento:** Apache Spark (PySpark)[cite: 7, 780].
* [cite_start]**Despliegue:** AWS Elastic MapReduce (EMR) para producción y clúster local basado en Docker para desarrollo[cite: 7, 606, 608].
* **Almacenamiento:** Amazon S3. [cite_start]Los datos se ingieren, se limpian y se persisten utilizando el formato columnar **Parquet**[cite: 8]. [cite_start]Se utiliza una estrategia de particionamiento temporal (por año y mes) para optimizar consultas futuras y evitar problemas de archivos pequeños[cite: 713, 715, 716].

## Conjunto de Datos (Dataset)
[cite_start]El análisis utiliza el **Polymarket Tick-Level Orderbook Dataset**, el cual está normalizado en 4 tablas principales[cite: 11, 16]:
1.  [cite_start]**Orderbook:** Captura cada actualización a nivel de tick de los precios y volúmenes (más de 5.5 mil millones de registros)[cite: 17, 71].
2.  [cite_start]**Snapshots:** Instantáneas de profundidad de liquidez con arrays anidados de ofertas y demandas (bids/asks)[cite: 49, 50, 72].
3.  [cite_start]**Trades:** Registro histórico de todas las transacciones ejecutadas en los mercados[cite: 32, 73].
4.  [cite_start]**Targets:** Catálogo dimensional que provee el contexto legible por humanos (preguntas, fechas de cierre, estatus) a los hashes criptográficos[cite: 40, 74].

## Estructura del Repositorio
* [cite_start]`/infra/spark-clusterProject/`: Contiene los Dockerfiles y el archivo `docker-compose.yml` necesarios para levantar un clúster local de Spark (Master, Worker y Jupyter Notebook)[cite: 606, 607, 608].
* `/spark/src/`: Módulos de utilidad en Python (`emr_spark_utils.py` y `spark_utils.py`) para configurar la sesión de Spark, afinar la memoria y generar esquemas de datos estructurados dinámicamente.
* `/spark/notebooks/`: Directorio principal de análisis. Contiene `emr.py` (el script principal para enviar a AWS EMR) y `try.ipynb` (notebook de Jupyter para exploración local).

## Entorno de Desarrollo Local (Docker)
Puedes levantar un entorno de Spark completo localmente para pruebas. Se requieren Docker y Docker Compose instalados.

1.  Navega al directorio de infraestructura:
    ```bash
    cd infra/spark-clusterProject/
    ```
2.  Construye las imágenes base de Spark ejecutando el script proporcionado:
    ```bash
    chmod +x build-images.sh
    ./build-images.sh
    ```
3.  Levanta el clúster con Docker Compose:
    ```bash
    docker compose up --scale spark-worker=1 -d
    ```
4.  Valida la ejecución accediendo a la Interfaz Web de Spark Master en `http://localhost:9090` o al entorno de Jupyter en `http://localhost:8888`.

## Despliegue en Producción (AWS EMR)
Para procesar el conjunto de datos completo de 30 GB, el script principal se envía a un clúster de Amazon EMR asegurando que se incluyan las dependencias y utilidades.

**Comando de ejecución (`spark-submit`):**
```bash
spark-submit --deploy-mode cluster --py-files s3://<tu-bucket>/spark/emr_spark_utils.py s3://<tu-bucket>/spark/emr.py
