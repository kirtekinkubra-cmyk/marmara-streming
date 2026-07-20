"""
Job 2 — risk_score_streaming.py

`earthquake.events.scored`'ı okur ve `foreachBatch` ile bir Delta tabloya
(district_risk_live) MERGE (upsert) yapar. Böylece:
  - Restart'ta kaldığı yerden devam eder (Spark'ın kendi state store'una değil,
    Delta'nın ACID garantisine güveniyoruz — operasyonel olarak daha sağlam).
  - Sadece gerçekten değişen ilçeler `district.risk.updated` topic'ine yazılır
    (Job 3'ü gereksiz tetiklememek için).

Risk seviyesi eşikleri (LOW/MEDIUM/HIGH/VERY_HIGH), batch pipeline'ınızdaki
05_risk_score.py'nin pd.qcut(q=4) çıktısından türetilen SABİT eşiklerdir — streaming'de
her mikro-batch üzerinden yeniden çeyreklik hesaplamak anlamsız olur (örneklem küçük
ve kararsız olur), bu yüzden eşikleri periyodik bir batch job ile güncelleyip
buraya sabit olarak taşımanızı öneririm (aşağıdaki RISK_LEVEL_THRESHOLDS).

Çalıştırma: event_impact_streaming.py ile aynı spark-submit deseni.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
from delta.tables import DeltaTable

KAFKA_BOOTSTRAP = "kafka:29092"
SOURCE_TOPIC = "earthquake.events.scored"
SINK_TOPIC = "district.risk.updated"
DELTA_TABLE_PATH = "data/reference/district_risk_live"

# 05_risk_score.py'nin son batch çalışmasındaki qcut sınırlarından türetilmiştir.
# Bu değerleri her batch job (05) çalıştığında yeniden hesaplayıp güncelleyin.
RISK_LEVEL_THRESHOLDS = {"low_max": 30.0, "medium_max": 55.0, "high_max": 75.0}

# Canlı bir depremin risk skoruna etkisi ne kadar süre "sıcak" kalsın (sonra
# baseline'a döner). Gerçek bir merge-based decay yerine basitlik için event
# impact'i doğrudan mevcut skora karıştırıyoruz; üretimde bir TTL/decay job'u
# (örn. saatlik ayrı bir Spark job) ile eskiyen boost'ları sıfırlamanız gerekir.
LIVE_IMPACT_WEIGHT = 0.30

SCORED_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_name", StringType()),
    StructField("event_time", StringType()),
    StructField("severity", StringType()),
    StructField("magnitude", DoubleType()),
    StructField("depth_km", DoubleType()),
    StructField("event_latitude", DoubleType()),
    StructField("event_longitude", DoubleType()),
    StructField("province", StringType()),
    StructField("district", StringType()),
    StructField("province_key", StringType()),
    StructField("district_key", StringType()),
    StructField("district_latitude", DoubleType()),
    StructField("district_longitude", DoubleType()),
    StructField("distance_to_epicenter_km", DoubleType()),
    StructField("event_impact_score", DoubleType()),
    StructField("final_risk_score", DoubleType()),
    StructField("dynamic_priority_score", DoubleType()),
])


def classify_risk_level(score_col):
    t = RISK_LEVEL_THRESHOLDS
    return (
        F.when(score_col <= t["low_max"], "Low")
        .when(score_col <= t["medium_max"], "Medium")
        .when(score_col <= t["high_max"], "High")
        .otherwise("Very High")
    )


def process_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    spark = batch_df.sparkSession

    # Bu mikro-batch'te her ilçe için en yüksek event etkisini al
    # (birden fazla eşzamanlı deprem varsa en kötü senaryo baskın olsun).
    district_updates = (
        batch_df.groupBy("province_key", "district_key", "province", "district")
        .agg(
            F.max("event_impact_score").alias("live_event_impact_score"),
            F.first("final_risk_score").alias("baseline_risk_score"),
            F.max("dynamic_priority_score").alias("dynamic_priority_score"),
            F.max("event_id").alias("triggering_event_id"),
        )
        .withColumn(
            "live_risk_score",
            F.col("baseline_risk_score") * (1 - LIVE_IMPACT_WEIGHT)
            + F.col("live_event_impact_score") * LIVE_IMPACT_WEIGHT,
        )
        .withColumn("risk_level", classify_risk_level(F.col("live_risk_score")))
        .withColumn("updated_at", F.current_timestamp())
    )

    if not DeltaTable.isDeltaTable(spark, DELTA_TABLE_PATH):
        district_updates.write.format("delta").mode("overwrite").save(DELTA_TABLE_PATH)
        changed = district_updates
    else:
        delta_table = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
        existing = delta_table.toDF().select("district_key", "live_risk_score").withColumnRenamed(
            "live_risk_score", "previous_risk_score"
        )
        changed = (
            district_updates.join(existing, "district_key", "left")
            .filter(
                F.col("previous_risk_score").isNull()
                | (F.abs(F.col("live_risk_score") - F.col("previous_risk_score")) > 1.0)
            )
            .drop("previous_risk_score")
        )

        (
            delta_table.alias("t")
            .merge(district_updates.alias("s"), "t.district_key = s.district_key")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    if changed.rdd.isEmpty():
        return

    (
        changed.select(
            F.col("district_key").alias("key"),
            F.to_json(
                F.struct(
                    "province_key", "district_key", "province", "district",
                    "live_risk_score", "risk_level", "triggering_event_id",
                    "dynamic_priority_score", "updated_at",
                )
            ).alias("value"),
        )
        .write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", SINK_TOPIC)
        .save()
    )


def build_query(spark: SparkSession):
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    scored_events = raw_stream.select(
        F.from_json(F.col("value").cast("string"), SCORED_SCHEMA).alias("e")
    ).select("e.*")

    query = (
        scored_events.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", "/tmp/checkpoints/risk_score_streaming")
        .trigger(processingTime="15 seconds")
        .start()
    )
    return query


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("risk_score_streaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    query = build_query(spark)
    query.awaitTermination()
