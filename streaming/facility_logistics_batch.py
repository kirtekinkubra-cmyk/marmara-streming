"""
Job 3 — facility_logistics_batch.py

`earthquake.events.scored`'ı (Job 1'in çıktısı) okur. Her mikro-batch'te, o batch'te
görülen event_id'ler için:
  1. event_district_priorities'i yeniden kurar (08'deki event_priority_rank +
     is_priority_district mantığı — burada normal bir pandas DataFrame üzerinde,
     çünkü tek bir event'in ilçe sayısı küçük, Spark'a gerek yok)
  2. common/facility_ranking.py'deki rank_facilities_for_event() fonksiyonunu
     DEĞİŞTİRMEDEN çağırır
  3. common/logistics_recommendation.py'deki atama fonksiyonlarını çağırır
  4. Sonuçları `aid.center.recommendations` topic'ine ve
     `logistics_recommendations_live` Delta tablosuna yazar

Önemli: OSM aday tesisler (havalimanı/liman/otogar/iskele) ve statik risk tablosu
sadece BİR KEZ pandas'a yüklenip cache'lenir (prepare_facilities / calculate_local_support /
calculate_multimodal_score event'ten bağımsızdır — her mikro-batch'te tekrar
hesaplanmaz, bu da Job 3'ün en pahalı kısmı olurdu).
"""

import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import pandas as pd
from confluent_kafka import Producer
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

from common.facility_ranking import (
    prepare_facilities,
    calculate_local_support,
    calculate_multimodal_score,
    rank_facilities_for_event,
)
from common.logistics_recommendation import (
    get_top_priority_districts,
    get_top_facilities,
    assign_districts_to_facilities,
)

KAFKA_BOOTSTRAP = "kafka:29092"
SOURCE_TOPIC = "earthquake.events.scored"
SINK_TOPIC = "aid.center.recommendations"
DELTA_TABLE_PATH = "data/reference/logistics_recommendations_live"

# Batch pipeline çıktılarınız (04_feature_engineering / OSM temizliği sonrası)
OSM_CLEAN_PATH = "data/processed/osm_critical_points_clean.csv"
RISK_STATIC_PATH = "data/processed/district_risk_scores.csv"

TOP_PRIORITY_DISTRICTS = 10

SCORED_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_name", StringType()),
    StructField("event_time", StringType()),
    StructField("severity", StringType()),
    StructField("depth_km", DoubleType()),
    StructField("magnitude", DoubleType()),
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

# --- Süreç genelinde bir kez yüklenip cache'lenen aday tesis havuzu ---
_CANDIDATE_FACILITIES_CACHE = None


def get_candidate_facilities() -> pd.DataFrame:
    """
    09_facility_ranking.py'deki hazırlık zincirini (prepare_facilities →
    calculate_local_support → calculate_multimodal_score) BİR KEZ çalıştırır ve
    süreç belleğinde tutar. Bu adımlar event'ten bağımsızdır; her mikro-batch'te
    tekrarlamak Job 3'ü gereksiz yavaşlatır.
    """
    global _CANDIDATE_FACILITIES_CACHE
    if _CANDIDATE_FACILITIES_CACHE is not None:
        return _CANDIDATE_FACILITIES_CACHE

    osm = pd.read_csv(OSM_CLEAN_PATH)
    risk = pd.read_csv(RISK_STATIC_PATH)

    candidates = prepare_facilities(osm=osm, risk=risk)
    candidates = calculate_local_support(candidates=candidates, osm=osm)
    candidates = calculate_multimodal_score(candidates=candidates)

    _CANDIDATE_FACILITIES_CACHE = candidates
    return candidates


def build_event_district_priorities(event_pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Job 1'in ürettiği ham skorlardan, 08_event_impact.py'nin çıktı şemasına eşdeğer
    event_district_priorities DataFrame'ini kurar (event_priority_rank ve
    is_priority_district dahil — bunlar Structured Streaming'de değil, burada
    statik bir pandas DataFrame üzerinde hesaplanır).
    """
    df = event_pdf.copy()
    df["event_priority_rank"] = (
        df.groupby("event_id")["dynamic_priority_score"]
        .rank(method="dense", ascending=False)
        .astype(int)
    )
    df["is_priority_district"] = df["event_priority_rank"] <= TOP_PRIORITY_DISTRICTS
    df = df.rename(columns={"final_risk_score": "base_risk_score"})
    return df


def build_recommendations_for_event(event_id: str, event_priorities: pd.DataFrame) -> pd.DataFrame:
    candidates = get_candidate_facilities()

    ranked_facilities = rank_facilities_for_event(
        event_id=event_id,
        event_priorities=event_priorities,
        candidates=candidates,
    )
    if ranked_facilities.empty:
        return pd.DataFrame()

    priority_districts = get_top_priority_districts(event_priorities, event_id)
    top_facilities = get_top_facilities(ranked_facilities, event_id)

    return assign_districts_to_facilities(priority_districts, top_facilities)


def publish_recommendations(recommendations: pd.DataFrame, producer: Producer):
    for _, row in recommendations.iterrows():
        payload = row.to_dict()
        key = f"{payload['event_id']}:{payload['facility_id']}"
        producer.produce(SINK_TOPIC, key=key, value=json.dumps(payload, default=str))
    producer.flush(timeout=5)


def upsert_delta(recommendations: pd.DataFrame, spark: SparkSession):
    spark_df = spark.createDataFrame(recommendations.astype(str))
    if not DeltaTable.isDeltaTable(spark, DELTA_TABLE_PATH):
        spark_df.write.format("delta").mode("overwrite").save(DELTA_TABLE_PATH)
        return

    delta_table = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
    (
        delta_table.alias("t")
        .merge(
            spark_df.alias("s"),
            "t.event_id = s.event_id AND t.district_key = s.district_key",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def process_batch(batch_df: DataFrame, batch_id: int, producer: Producer):
    if batch_df.rdd.isEmpty():
        return

    spark = batch_df.sparkSession
    event_pdf = batch_df.toPandas()

    event_priorities = build_event_district_priorities(event_pdf)

    all_recommendations = []
    for event_id in event_priorities["event_id"].unique():
        recs = build_recommendations_for_event(event_id, event_priorities)
        if not recs.empty:
            all_recommendations.append(recs)

    if not all_recommendations:
        return

    combined = pd.concat(all_recommendations, ignore_index=True)
    publish_recommendations(combined, producer)
    upsert_delta(combined, spark)


def build_query(spark: SparkSession):
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "client.id": "facility-logistics-job"})

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
        scored_events.writeStream.foreachBatch(
            lambda batch_df, batch_id: process_batch(batch_df, batch_id, producer)
        )
        .option("checkpointLocation", "/tmp/checkpoints/facility_logistics_batch")
        .trigger(processingTime="20 seconds")
        .start()
    )
    return query


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("facility_logistics_batch")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    query = build_query(spark)
    query.awaitTermination()
