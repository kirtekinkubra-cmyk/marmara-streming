"""
Job 1 — event_impact_streaming.py

`earthquake.events.raw`'daki her deprem event'ini okur, statik ilçe referans
tablosuyla (broadcast) çarpraz birleştirir ve 08_event_impact.py'deki
magnitude/depth/distance formülünü uygulayarak HER İLÇE İÇİN etki skoru üretir.

Çıktı: `earthquake.events.scored` topic'i (event_id x district_key satırları)

Çalıştırma:
  spark-submit --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0 \
    streaming/event_impact_streaming.py
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

from common.reference_data import load_static_reference

KAFKA_BOOTSTRAP = "kafka:29092"
SOURCE_TOPIC = "earthquake.events.raw"
SINK_TOPIC = "earthquake.events.scored"

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_name", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("magnitude", DoubleType()),
    StructField("depth_km", DoubleType()),
    StructField("event_time", StringType()),
    StructField("source", StringType()),
    StructField("ingested_at", StringType()),
])


def haversine_km(lat1, lon1, lat2, lon2):
    """08_event_impact.py'deki haversine_distance'ın Spark sütun ifadesi hali."""
    earth_radius_km = 6371.0
    lat1_rad, lon1_rad = F.radians(lat1), F.radians(lon1)
    lat2_rad, lon2_rad = F.radians(lat2), F.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = (
        F.sin(dlat / 2) ** 2
        + F.cos(lat1_rad) * F.cos(lat2_rad) * F.sin(dlon / 2) ** 2
    )
    c = 2 * F.asin(F.sqrt(a))
    return earth_radius_km * c


def magnitude_score(magnitude):
    return F.least(F.greatest(((magnitude - 5.0) / (7.5 - 5.0)) * 100, F.lit(0.0)), F.lit(100.0))


def depth_score(depth_km):
    return F.least(F.greatest(100 - (depth_km / 40.0 * 100), F.lit(0.0)), F.lit(100.0))


def distance_score(distance_km):
    return 100 * F.exp(-distance_km / 75.0)


def classify_severity(magnitude_col):
    return (
        F.when(magnitude_col >= 7.0, "Extreme")
        .when(magnitude_col >= 6.5, "Major")
        .when(magnitude_col >= 6.0, "Moderate")
        .otherwise("Minor")
    )


def build_query(spark: SparkSession):
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    events = (
        raw_stream.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_time_ts", F.to_timestamp("event_time"))
        # geç gelen/aşırı eski event'leri ele: 10 dakikadan eski event'i state'e alma
        .withWatermark("event_time_ts", "10 minutes")
        .withColumn("severity", classify_severity(F.col("magnitude")))
    )

    # Çakışan sütun adlarını önlemek için event ve ilçe koordinatlarını ayrıştır
    # (09/10'daki facility ranking mantığı district_latitude/district_longitude ve
    # event_latitude/event_longitude adlarını bekliyor — 08_event_impact.py ile tutarlı).
    events_renamed = events.withColumnRenamed("latitude", "event_latitude").withColumnRenamed(
        "longitude", "event_longitude"
    )

    districts = load_static_reference(spark).withColumnRenamed(
        "latitude", "district_latitude"
    ).withColumnRenamed("longitude", "district_longitude")
    districts_bc = F.broadcast(districts)

    paired = events_renamed.crossJoin(districts_bc)

    # Not: event_priority_rank (08'deki .rank(method="dense")) burada HESAPLANMAZ.
    # Structured Streaming'in append mode'u sınırsız pencere fonksiyonlarını
    # desteklemez. Sıralama, foreachBatch içinde statik bir DataFrame üzerinde
    # çalışan Job 3'te (facility_logistics_batch.py) yapılır.
    scored = (
        paired
        .withColumn(
            "distance_to_epicenter_km",
            haversine_km(F.col("event_latitude"), F.col("event_longitude"),
                         F.col("district_latitude"), F.col("district_longitude")),
        )
        .withColumn("magnitude_score", magnitude_score(F.col("magnitude")))
        .withColumn("depth_score_val", depth_score(F.col("depth_km")))
        .withColumn("distance_score_val", distance_score(F.col("distance_to_epicenter_km")))
        .withColumn(
            "event_strength",
            F.col("magnitude_score") * 0.70 + F.col("depth_score_val") * 0.30,
        )
        .withColumn(
            "event_impact_score",
            F.least(
                F.greatest(F.col("event_strength") * F.col("distance_score_val") / 100, F.lit(0.0)),
                F.lit(100.0),
            ),
        )
        .withColumn(
            "dynamic_priority_score",
            F.col("event_impact_score") * 0.60 + F.col("final_risk_score") * 0.40,
        )
        .select(
            "event_id", "event_name", "event_time", "severity", "depth_km",
            "magnitude", "event_latitude", "event_longitude",
            "province", "district", "province_key", "district_key",
            "district_latitude", "district_longitude",
            "distance_to_epicenter_km", "event_impact_score",
            "final_risk_score", "dynamic_priority_score",
        )
    )

    output = scored.select(
        F.col("district_key").alias("key"),
        F.to_json(F.struct(*scored.columns)).alias("value"),
    )

    query = (
        output.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", SINK_TOPIC)
        .option("checkpointLocation", "/tmp/checkpoints/event_impact_streaming")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )
    return query


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("event_impact_streaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    query = build_query(spark)
    query.awaitTermination()
