from spark_utils import SparkUtils
from pyspark.sql import functions as F
from pyspark.sql.window import Window
su = SparkUtils()

column_types = [("timestamp_received", "long"),
                ("timestamp_created_at", "long"),
                ("market_id", "string"),
                ("best_bid", "double"),
                ("best_ask", "float"),
                ("change_price", "float"),
                ("change_size", "float"),
                ("change_side", "string"),
                ("token_id", "string"),
                ("spread", "float"),
                ("mid_price", "float")
                ]


order_book = SparkUtils.generate_schema(column_types)
order_book_df = su._spark \
                .read \
                .schema(order_book) \
                .parquet("s3://batch-processingcml/orderbook/")

from pyspark.sql import functions as F
column_types = [("timestamp_received", "long"),
                ("timestamp_created_at", "long"),
                ("market_id", "string"),
                ("update_type", "string"),
                ("data", "string")
                ]

json_data = [
                ("token_id","string"),
                ("side", "string"),
                ("best_bid", "string"),
                ("best_ask", "string"),
                ("timestamp", "float"),
                ("bids", "array_string"),
                ("asks", "array_string")
]


snapshots_schema = SparkUtils.generate_schema(column_types)
snapshots= su._spark.read.schema(snapshots_schema).parquet("s3://batch-processingcml/snapshots/")

json_data = SparkUtils.generate_schema(json_data)

snapshots = (
    snapshots
    .withColumn("data_parsed", F.from_json(F.col("data"), json_data))
    .withColumn("token_id", F.col("data_parsed.token_id"))
    .withColumn("side", F.col("data_parsed.side"))
    .withColumn("best_bid", F.col("data_parsed.best_bid").cast("double"))
    .withColumn("best_ask", F.col("data_parsed.best_ask").cast("double"))
    .withColumn("book_timestamp", F.col("data_parsed.timestamp"))
    .withColumn("bids", F.col("data_parsed.bids"))
    .withColumn("asks", F.col("data_parsed.asks"))
    .drop("data", "data_parsed")
)
#show data column
column_types = [("condition_id", "string"),
                ("question", "string"),
                ("end_date", "string"),
                ("closed", "boolean"),
                ("uma_status", "string"),
                ("liquidity","double"),
                ("clob_token_id_yes", "string"),
                ("clob_token_id_no", "string")]

targets_schema = SparkUtils.generate_schema(column_types)

targets = (
    su._spark.read
    .schema(targets_schema)
    .parquet("s3://batch-processingcml/labels/targets/")
    .drop("category", "target")
    .withColumn(
        "uma_status",
        F.when(F.trim(F.col("uma_status")) == "", F.lit("proposed"))
        .otherwise(F.col("uma_status"))
    )       
)
column_types = [("condition_id", "string"),
                ("side", "string"),
                ("outcome", "string"),
                ("price", "double"),
                ("size", "double"),
                ("timestamp", "long"),
                ("asset", "string")]

trades_schema = SparkUtils.generate_schema(column_types)

trades = su._spark.read.schema(trades_schema).parquet("s3://batch-processingcml/labels/trades/")

#print number of records in each dataframe
print(f"Order Book records: {order_book_df.count()}")
print(f"Snapshots records: {snapshots.count()}")
print(f"Targets records: {targets.count()}")
print(f"Trades records: {trades.count()}")

# 1) Expandimos targets a nivel token para unir con trades.asset
labels_by_token = (
    targets.select(
        F.col("condition_id").alias("label_condition_id"),
        "question",
        "end_date",
        "closed",
        "uma_status",
        "liquidity",
        F.col("clob_token_id_yes").alias("token_id"),
        F.lit("Yes").alias("label_outcome")
    )
    .unionByName(
        targets.select(
            F.col("condition_id").alias("label_condition_id"),
            "question",
            "end_date",
            "closed",
            "uma_status",
            "liquidity",
            F.col("clob_token_id_no").alias("token_id"),
            F.lit("No").alias("label_outcome")
        )
    )
)

# 2) Normalizamos timestamp de trade a ms (algunos vienen en segundos)
tr = (
    trades
    .withColumn(
        "trade_ts_ms",
        F.when(F.length(F.col("timestamp").cast("string")) <= 10, F.col("timestamp") * 1000)
         .otherwise(F.col("timestamp"))
    )
    .withColumn("trade_id", F.monotonically_increasing_id())
    .alias("tr")
)

lb = labels_by_token.alias("lb")

trades_enriched = (
    tr.join(lb, F.col("tr.asset") == F.col("lb.token_id"), "left")
      .select(
          F.col("tr.*"),
          F.col("lb.label_condition_id"),
          F.col("lb.question"),
          F.col("lb.end_date"),
          F.col("lb.closed"),
          F.col("lb.uma_status"),
          F.col("lb.liquidity"),
          F.col("lb.label_outcome")
      )
)

# 3) Join temporal con order_book_df: último estado de libro antes del trade (hasta 10 min hacia atrás)
ob = order_book_df.alias("ob")
t = trades_enriched.alias("t")

joined = (
    t.join(
        ob,
        (F.col("t.asset") == F.col("ob.token_id")) &
        (F.col("ob.timestamp_received") <= F.col("t.trade_ts_ms")) &
        (F.col("ob.timestamp_received") >= F.col("t.trade_ts_ms") - F.lit(10 * 60 * 1000)),
        "left"
    )
)

w = Window.partitionBy("t.trade_id").orderBy(F.col("ob.timestamp_received").desc_nulls_last())

joined_latest = (
    joined
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# 4) Features de microestructura por trade
with_features = (
    joined_latest
    .withColumn(
        "ref_price",
        F.when(F.col("t.side") == "BUY", F.col("ob.best_ask")).otherwise(F.col("ob.best_bid"))
    )
    .withColumn(
        "slippage_bps",
        F.when(
            F.col("ref_price").isNotNull() & (F.col("ref_price") > 0),
            (F.col("t.price") - F.col("ref_price")) / F.col("ref_price") * F.lit(10000.0)
        )
    )
    .withColumn(
        "edge_vs_mid",
        F.when(F.col("t.side") == "BUY", F.col("ob.mid_price") - F.col("t.price"))
         .otherwise(F.col("t.price") - F.col("ob.mid_price"))
    )
    .withColumn("trade_notional", F.col("t.price") * F.col("t.size"))
    .withColumn("trade_time", F.to_timestamp(F.from_unixtime(F.col("t.trade_ts_ms") / 1000)))
)

# 5) Vista agregada: mercados con peor/mejor ejecución
summary = (
    with_features
    .groupBy("label_condition_id", "question", "label_outcome")
    .agg(
        F.count("*").alias("n_trades"),
        F.round(F.sum("trade_notional"), 2).alias("notional"),
        F.round(F.avg("ob.spread"), 6).alias("avg_spread"),
        F.round(F.avg("slippage_bps"), 2).alias("avg_slippage_bps"),
        F.round(F.avg("edge_vs_mid"), 6).alias("avg_edge_vs_mid")
    )
    .orderBy(F.col("notional").desc_nulls_last())
)

summary.show(30, truncate=False)

# Inspección de trades concretos
with_features.select(
    "trade_time",
    "label_condition_id",
    "question",
    "label_outcome",
    F.col("t.side").alias("trade_side"),
    F.col("t.price").alias("trade_price"),
    F.col("t.size").alias("trade_size"),
    "ob.best_bid",
    "ob.best_ask",
    "ob.mid_price",
    "ob.spread",
    "slippage_bps",
    "edge_vs_mid"
).orderBy(F.col("trade_time").desc()).show(20, truncate=False)


trades_clean = (
    trades
    .withColumn(
        "trade_ts_ms",
        F.when(F.length(F.col("timestamp").cast("string")) <= 10, F.col("timestamp") * 1000)
         .otherwise(F.col("timestamp"))
    )
    .withColumn("trade_notional", F.col("price") * F.col("size"))
    .withColumn("trade_id", F.monotonically_increasing_id())
)

# Filtrar ballenas (mayor a $5,000)
whale_trades = trades_clean.filter(F.col("trade_notional") >= 5000.0).alias("wt")

# 2. Agregar contexto usando targets (solo la pregunta)
targets_min = targets.select("condition_id", "question").dropDuplicates().alias("tgt")
whales_with_q = whale_trades.join(targets_min, F.col("wt.condition_id") == F.col("tgt.condition_id"), "left").alias("wq")

# 3. FOTO ANTES: Precio justo ANTES del trade
ob_before = order_book_df.alias("obb")
join_before = whales_with_q.join(
    ob_before,
    (F.col("wq.asset") == F.col("obb.token_id")) &
    (F.col("obb.timestamp_received") <= F.col("wq.trade_ts_ms")),
    "left"
)

# Quedarnos solo con el registro más reciente antes del trade y ponerle alias "wa"
w_before = Window.partitionBy("wq.trade_id").orderBy(F.col("obb.timestamp_received").desc_nulls_last())
whales_antes = (
    join_before
    .withColumn("rn", F.row_number().over(w_before))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .alias("wa")
)

# 4. FOTO DESPUÉS: Ventana de 10s DESPUÉS del trade
ob_after = order_book_df.alias("oba")
join_after = whales_antes.join(
    ob_after,
    (F.col("wa.asset") == F.col("oba.token_id")) &
    (F.col("oba.timestamp_received") > F.col("wa.trade_ts_ms")) &
    (F.col("oba.timestamp_received") <= F.col("wa.trade_ts_ms") + F.lit(10 * 1000)),
    "left"
)

w_impact = Window.partitionBy("wa.trade_id").orderBy(F.col("oba.timestamp_received").asc())
w_recovery = Window.partitionBy("wa.trade_id").orderBy(F.col("oba.timestamp_received").desc())

impact_analysis = (
    join_after
    .withColumn("rn_inmediato", F.row_number().over(w_impact))
    .withColumn("rn_10s", F.row_number().over(w_recovery))
)

impacto_inmediato = impact_analysis.filter(F.col("rn_inmediato") == 1).select(
    F.col("wa.trade_id"), F.col("oba.mid_price").alias("mid_price_post")
)

recuperacion_10s = impact_analysis.filter(F.col("rn_10s") == 1).select(
    F.col("wa.trade_id"), F.col("oba.mid_price").alias("mid_price_10s")
)

reporte_final = (
    whales_antes
    .join(impacto_inmediato, "trade_id", "left")
    .join(recuperacion_10s, "trade_id", "left")
    .withColumn(
        "impacto_bps",
        F.abs(F.col("mid_price_post") - F.col("wa.mid_price")) / F.col("wa.mid_price") * 10000
    )
    .withColumn("trade_time", F.to_timestamp(F.from_unixtime(F.col("wa.trade_ts_ms") / 1000)))
)

reportefinal = reporte_final.select(
    "trade_time",
    F.substring("wa.question", 1, 40).alias("question"), 
    "wa.side",
    F.round("wa.trade_notional", 2).alias("volumen_$"),
    F.round("wa.mid_price", 4).alias("mid_ANTES"),
    F.round("mid_price_post", 4).alias("mid_DESPUES"),
    F.round("mid_price_10s", 4).alias("mid_10_SEGS"),
    F.round("impacto_bps", 2).alias("impacto_bps")
).orderBy(F.col("wa.trade_notional").desc())

reportefinal.show(20, truncate=False)

reportes3 = reportefinal.withColumn("year", F.year("trade_time")) \
                        .withColumn("month", F.month("trade_time"))

(
    reportes3
    .write
    .mode("overwrite")
    .partitionBy("year", "month") 
    .parquet("s3a://batch-processingcml/reports/bigfish/") 
)