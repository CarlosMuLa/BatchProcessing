# Batch Processing Pipeline con Apache Spark en AWS EMR

## Descripción del Proyecto
Este proyecto implementa un pipeline ETL distribuido y de análisis de datos utilizando **Apache Spark**. El objetivo principal es explorar la microestructura del mercado y la eficiencia de ejecución de **Polymarket**, un mercado de predicciones descentralizado. 

Dado el gran volumen y la complejidad relacional de los datos (un conjunto de datos de 30 GB que se traduce en más de 5.6 mil millones de registros), las herramientas de procesamiento de un solo nodo resultan insuficientes. Por ello, se diseñó una arquitectura de Big Data robusta para extraer métricas financieras mediante cruces temporales complejos.

## Arquitectura
* **Procesamiento:** Apache Spark (PySpark).
* **Despliegue:** AWS Elastic MapReduce (EMR) para producción y clúster local basado en Docker para desarrollo.
* **Almacenamiento:** Amazon S3. Los datos se ingieren, se limpian y se persisten utilizando el formato columnar **Parquet**. Se utiliza una estrategia de particionamiento temporal (por año y mes) para optimizar consultas futuras y evitar problemas de archivos pequeños.

## Conjunto de Datos (Dataset)
El análisis utiliza el **Polymarket Tick-Level Orderbook Dataset**, el cual está normalizado en 4 tablas principales:
1.  **Orderbook:** Captura cada actualización a nivel de tick de los precios y volúmenes (más de 5.5 mil millones de registros).
2.  **Snapshots:** Instantáneas de profundidad de liquidez con arrays anidados de ofertas y demandas (bids/asks).
3.  **Trades:** Registro histórico de todas las transacciones ejecutadas en los mercados.
4.  **Targets:** Catálogo dimensional que provee el contexto legible por humanos (preguntas, fechas de cierre, estatus) a los hashes criptográficos.

## Estructura del Repositorio
* `/infra/spark-clusterProject/`: Contiene los Dockerfiles y el archivo `docker-compose.yml` necesarios para levantar un clúster local de Spark (Master, Worker y Jupyter Notebook).
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

