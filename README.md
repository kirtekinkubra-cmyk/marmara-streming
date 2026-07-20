# Marmara Deprem Risk & Yardım Merkezi — Streaming

Mevcut batch pipeline'ınızı (03-10) canlı Kafka + Spark mimarisine taşıyan proje.
Detaylı mimari için `ARCHITECTURE.md`'ye bakın.

## Yerel hızlı başlangıç

```bash
# 1) Kafka + Spark cluster'ı ayağa kaldır
docker-compose up -d
bash deploy/create_topics.sh

pip install -r requirements.txt

# 2) Statik referans verisini hazırla (mevcut batch scriptleriniz + Delta'ya yazma)
#    03/04/05'i normal şekilde çalıştırıp çıktısını common/reference_data.py'deki
#    refresh_reference_table() ile Delta'ya yazdırın. Ayrıca 09'un ihtiyaç duyduğu
#    osm_critical_points_clean.csv ve district_risk_scores.csv dosyalarının
#    data/processed/ altında olduğundan emin olun (facility_logistics_batch.py bunları okur).

# 3) Producer'ı test/demo modunda başlat (gerçek AFAD yerine 3 örnek senaryo yayınlar)
python producers/afad_event_producer.py --simulate --poll-interval 20

# 4) Üç streaming job'u ayrı terminallerde başlat
spark-submit --master spark://localhost:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0 \
  streaming/event_impact_streaming.py

spark-submit --master spark://localhost:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0 \
  streaming/risk_score_streaming.py

spark-submit --master spark://localhost:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0 \
  streaming/facility_logistics_batch.py

# 5) Sonuçları izle
#    - Kafka UI: http://localhost:8080 (aid.center.recommendations topic'ini izleyin)
#    - Spark UI: http://localhost:8081
#    - data/reference/district_risk_live ve logistics_recommendations_live Delta tabloları
```

## Prodüksiyona geçiş için yapılacaklar

1. `--simulate` yerine gerçek AFAD polling'i doğrulayın (endpoint şeması zamanla değişebilir).
2. `RISK_LEVEL_THRESHOLDS` (risk_score_streaming.py) ve OSM/risk statik dosya yollarını
   (facility_logistics_batch.py) periyodik batch job çıktılarınızla senkron tutacak bir
   Airflow DAG'i kurun.
3. Docker Compose yerine Strimzi (Kafka) + Spark Operator ile Kubernetes/OpenShift'e taşıyın
   — bkz. `ARCHITECTURE.md` → Deployment bölümü.
4. `LIVE_IMPACT_WEIGHT`'in zamanla "sönmesi" (decay) için ayrı bir saatlik job ekleyin;
   şu anki haliyle bir deprem etkisi sonsuza kadar risk skorunu etkilemeye devam eder.
5. Schema Registry + Avro'ya geçin (şu an ham JSON kullanılıyor, prod'da şema doğrulaması önemli).
