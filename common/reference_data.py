"""
Statik ilçe referans verisini (nüfus, OSM altyapı, deniz erişimi, farkındalık ve
bunlardan türetilen hazard/exposure/infrastructure/preparedness/response bileşenleri)
yükler.

Bu veri mevcut 04_feature_engineering.py + 05_risk_score.py batch pipeline'ınızın
çıktısıdır ve günlük/haftalık bir Airflow/cron job ile tazelenir. Streaming job'ları
bu veriyi HER MİKRO-BATCH'TE YENİDEN HESAPLAMAZ — sadece broadcast join ile okur.
"""

from pyspark.sql import DataFrame, SparkSession

# Batch pipeline'ınızın (05_risk_score.py) ürettiği dosyanın Delta/Parquet karşılığı.
# Prod'da bunu S3/MinIO üzerinde bir Delta tablosuna işaret edecek şekilde değiştirin.
REFERENCE_TABLE_PATH = "data/reference/district_risk_static"

STATIC_COLUMNS = [
    "province", "district", "province_key", "district_key",
    "latitude", "longitude",  # 06_export_kepler.py'deki OSM-ortalama ilçe merkezi
    "population",
    "hazard_score", "exposure_score", "infrastructure_score",
    "preparedness_score", "response_capacity_score",
    "final_risk_score",  # canlı event gelmeden önceki temel (baseline) risk
    "avg_criticality", "avg_hazard_score",
]


def load_static_reference(spark: SparkSession, path: str = REFERENCE_TABLE_PATH) -> DataFrame:
    """
    district_risk_scores.csv (05'in çıktısı) tabanlı statik referans tablosunu okur.

    Not: Bu fonksiyon her streaming job başlatıldığında BİR KEZ çağrılır,
    sonucu broadcast() ile sarmalayıp join'lerde kullanın:

        from pyspark.sql.functions import broadcast
        scored = events_df.join(broadcast(ref_df), on=["province_key", "district_key"])
    """
    return spark.read.format("delta").load(path).select(*STATIC_COLUMNS)


def refresh_reference_table(spark: SparkSession, source_csv: str, output_path: str = REFERENCE_TABLE_PATH):
    """
    05_risk_score.py'nin ürettiği district_risk_scores.csv'yi Delta tabloya yazar.
    Bu fonksiyonu batch job'ınızın (05'in) son adımına ekleyin ya da ayrı bir
    Airflow task'ı olarak günlük çalıştırın.
    """
    df = spark.read.option("header", True).option("encoding", "UTF-8").csv(source_csv)
    (
        df.write.format("delta")
        .mode("overwrite")
        .save(output_path)
    )
    return df
