from __future__ import print_function
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import json
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, from_json, udf, lit, datediff, current_date, to_date, when, size
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, ArrayType, BooleanType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline, PipelineModel
import time
from datetime import datetime

sc = SparkContext(appName="PythonStructuredStreamsKafka")
spark = SparkSession(sc)
print(spark.version)
sc.setLogLevel("WARN")

kafkaServer = "kafka:39092"
topic = "gameStatsPx"

# Schema per i dati IGDB
game_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("rating", DoubleType(), True),
    StructField("rating_count", IntegerType(), True),
    StructField("hypes", IntegerType(), True),
    StructField("follows", IntegerType(), True),
    StructField("release_date", LongType(), True),
    StructField("first_release_date", LongType(), True),
    StructField("category", IntegerType(), True),
    StructField("status", IntegerType(), True),
    StructField("total_rating", DoubleType(), True),
    StructField("total_rating_count", IntegerType(), True),
    StructField("aggregated_rating", DoubleType(), True),
    StructField("aggregated_rating_count", IntegerType(), True),
    StructField("franchises", ArrayType(IntegerType()), True),
    StructField("genres", ArrayType(IntegerType()), True),
    StructField("platforms", ArrayType(IntegerType()), True)
])

# Schema per dati da Elasticsearch (include campi calcolati)
es_schema = StructType([
    StructField("game_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("rating", DoubleType(), True),
    StructField("rating_count", IntegerType(), True),
    StructField("total_rating", DoubleType(), True),
    StructField("aggregated_rating", DoubleType(), True),
    StructField("hypes", IntegerType(), True),
    StructField("follows", IntegerType(), True),
    StructField("release_date", StringType(), True),
    StructField("days_to_release", IntegerType(), True),
    StructField("days_since_release", IntegerType(), True),
    StructField("is_released", BooleanType(), True),
    StructField("is_upcoming", BooleanType(), True),
    StructField("franchise_count", IntegerType(), True),
    StructField("platform_count", IntegerType(), True),
    StructField("genre_count", IntegerType(), True),
    StructField("hype_score", DoubleType(), True),
    StructField("longevity_score", DoubleType(), True),
    StructField("success_level", IntegerType(), True),
])

# Variabili globali per i modelli
GLOBAL_MODELS = {}
MODELS_LAST_UPDATE = 0
MODEL_UPDATE_INTERVAL = 3600  # Riallena ogni ora (3600 secondi)

def check_elasticsearch_connection():
    es = Elasticsearch(
        hosts=["http://elasticsearch:9200"],
        http_auth=("kibana_system_user", "kibanapass123"),
        max_retries=10,
        retry_on_timeout=True
    )
    try:
        return es.ping()
    except Exception as e:
        print(f"Errore durante il ping a Elasticsearch: {e}")
        return False

def load_all_historical_data_from_elasticsearch():
    """Carica TUTTI i record da Elasticsearch per training robusto"""
    es = Elasticsearch(
        hosts=["http://elasticsearch:9200"],
        http_auth=("kibana_system_user", "kibanapass123"),
        max_retries=10,
        retry_on_timeout=True
    )
    
    print("Caricamento di TUTTI i dati storici da Elasticsearch...")
    
    try:
        query = {
            "query": {"match_all": {}},
            "_source": True
        }
        
        all_docs = []
        for doc in scan(es, index="games", query=query, scroll='5m', size=1000):
            all_docs.append(doc['_source'])
        
        if not all_docs:
            print("Nessun dato storico trovato in Elasticsearch")
            return None
        
        print(f"Caricati {len(all_docs)} record da Elasticsearch")
        
        # Converti in DataFrame Spark
        historical_df = spark.createDataFrame(all_docs, schema=es_schema)
        
        # Riempimento valori nulli
        historical_df = historical_df.fillna({
            "hypes": 0,
            "follows": 0,
            "rating_count": 0,
            "franchise_count": 0,
            "platform_count": 0,
            "genre_count": 0,
            "total_rating": 0,
            "aggregated_rating": 0,
            "days_to_release": 0,
            "days_since_release": 0,
            "hype_score": 0,
            "longevity_score": 0,
            "success_level": 0
        })
        
        return historical_df
        
    except Exception as e:
        print(f"Errore nel caricamento dati storici: {e}")
        return None

def calculate_hype_score(hypes, follows, rating_count, days_to_release):
    """Calcola un punteggio di hype normalizzato"""
    if days_to_release is None or days_to_release < 0:
        return 0.0
    
    hype_component = (hypes or 0) * 2.0
    follow_component = (follows or 0) * 1.5
    anticipation_component = (rating_count or 0) * 0.5
    
    time_factor = max(0, 1 - (days_to_release / 365.0)) if days_to_release <= 365 else 0.3
    
    raw_score = (hype_component + follow_component + anticipation_component) * time_factor
    
    return min(100.0, raw_score / 10.0)

def calculate_longevity_score(rating, rating_count, franchise_count, platform_count, days_since_release):
    """Calcola un punteggio di longevità basato su metriche storiche"""
    if days_since_release is None or days_since_release <= 0:
        return None
    
    quality_factor = (rating or 0) / 10.0
    popularity_factor = min(1.0, (rating_count or 0) / 1000.0)
    franchise_factor = min(1.0, (franchise_count or 0) / 5.0)
    platform_factor = min(1.0, (platform_count or 0) / 10.0)
    
    age_years = days_since_release / 365.0
    age_factor = min(1.0, age_years / 10.0) if age_years > 1 else 0.5
    
    raw_score = (quality_factor * 40 + popularity_factor * 30 + 
                 franchise_factor * 15 + platform_factor * 10 + age_factor * 5)
    
    return round(raw_score, 2)

def classify_success_level(rating, rating_count):
    """Classifica il livello di successo del gioco (0=flop, 1=medio, 2=hit)"""
    if rating is None or rating_count is None:
        return None
    
    if rating >= 80 and rating_count >= 500:
        return 2  # Hit
    elif rating >= 60 and rating_count >= 100:
        return 1  # Medio
    else:
        return 0  # Flop

hype_udf = udf(calculate_hype_score, DoubleType())
longevity_udf = udf(calculate_longevity_score, DoubleType())
success_udf = udf(classify_success_level, IntegerType())

def train_all_models_with_historical_data(historical_df):
    """Train tutti i modelli usando TUTTI i dati storici da Elasticsearch"""
    
    print("\n" + "="*80)
    print("TRAINING MODELLI CON DATI STORICI COMPLETI")
    print("="*80)
    
    models = {}
    feature_cols = ["hypes", "follows", "rating_count", "franchise_count", 
                    "platform_count", "genre_count"]
    feature_cols_extended = feature_cols + ["total_rating", "aggregated_rating"]
    
    total_records = historical_df.count()
    print(f"\n Dataset totale: {total_records} record")
    
    # 1. MODELLO RATING (usando Gradient Boosted Trees per migliore accuracy)
    training_rating = historical_df.filter(col("rating").isNotNull())
    count_rating = training_rating.count()
    print(f"\n Training Rating Model: {count_rating} record con rating")
    
    if count_rating > 20:
        try:
            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_rating", handleInvalid="skip")
            
            # Usa GBT invece di Linear Regression per migliore accuracy
            gbt = GBTRegressor(
                featuresCol="features_rating", 
                labelCol="rating",
                predictionCol="predicted_rating",
                maxIter=30,
                maxDepth=5,
                stepSize=0.1
            )
            
            pipeline = Pipeline(stages=[assembler, gbt])
            models['rating'] = pipeline.fit(training_rating)
            print(f"Rating model trained successfully (GBT)")
        except Exception as e:
            print(f"Errore: {e}")
    else:
        print(f"Dati insufficienti (minimo 20)")
    
    # 2. MODELLO RATING COUNT
    training_count = historical_df.filter((col("rating_count").isNotNull()) & (col("rating_count") > 0))
    count_rating_count = training_count.count()
    print(f"\n Training Rating Count Model: {count_rating_count} record")
    
    if count_rating_count > 20:
        try:
            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_count", handleInvalid="skip")
            lr = LinearRegression(featuresCol="features_count", labelCol="rating_count",
                                 predictionCol="predicted_rating_count", maxIter=20)
            pipeline = Pipeline(stages=[assembler, lr])
            models['rating_count'] = pipeline.fit(training_count)
            print(f"Rating Count model trained successfully")
        except Exception as e:
            print(f"Errore: {e}")
    else:
        print(f"Dati insufficienti (minimo 20)")
    
    # 3. MODELLO HYPE GROWTH
    training_hype = historical_df.filter((col("hypes").isNotNull()) & (col("hypes") > 0))
    count_hype = training_hype.count()
    print(f"\n Training Hype Growth Model: {count_hype} record")
    
    if count_hype > 20:
        try:
            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_hype", handleInvalid="skip")
            lr = LinearRegression(featuresCol="features_hype", labelCol="hypes",
                                 predictionCol="predicted_hype_growth", maxIter=20)
            pipeline = Pipeline(stages=[assembler, lr])
            models['hype'] = pipeline.fit(training_hype)
            print(f"Hype Growth model trained successfully")
        except Exception as e:
            print(f"Errore: {e}")
    else:
        print(f"Dati insufficienti (minimo 20)")
    
    # 4. MODELLO FOLLOWS GROWTH
    training_follows = historical_df.filter((col("follows").isNotNull()) & (col("follows") > 0))
    count_follows = training_follows.count()
    print(f"\n Training Follows Growth Model: {count_follows} record")
    
    if count_follows > 20:
        try:
            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_follows", handleInvalid="skip")
            lr = LinearRegression(featuresCol="features_follows", labelCol="follows",
                                 predictionCol="predicted_follows_growth", maxIter=20)
            pipeline = Pipeline(stages=[assembler, lr])
            models['follows'] = pipeline.fit(training_follows)
            print(f"Follows Growth model trained successfully")
        except Exception as e:
            print(f"Errore: {e}")
    else:
        print(f"Dati insufficienti (minimo 20)")
    
    # 5. MODELLO LONGEVITY (Random Forest per relazioni complesse)
    training_longevity = historical_df.filter(col("longevity_score").isNotNull())
    count_longevity = training_longevity.count()
    print(f"\n Training Longevity Model: {count_longevity} record")
    
    if count_longevity > 20:
        try:
            assembler = VectorAssembler(inputCols=feature_cols_extended, outputCol="features_longevity", handleInvalid="skip")
            rf = RandomForestRegressor(featuresCol="features_longevity", labelCol="longevity_score",
                                      predictionCol="predicted_longevity", numTrees=30, maxDepth=5)
            pipeline = Pipeline(stages=[assembler, rf])
            models['longevity'] = pipeline.fit(training_longevity)
            print(f"Longevity model trained successfully (Random Forest)")
        except Exception as e:
            print(f"Errore: {e}")
    else:
        print(f"Dati insufficienti (minimo 20)")
    
    # 6. CLASSIFICATORE SUCCESSO
    training_success = historical_df.filter(col("success_level").isNotNull())
    count_success = training_success.count()
    print(f"\n Training Success Classifier: {count_success} record")
    
    if count_success > 30:
        try:
            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_success", handleInvalid="skip")
            lr_classifier = LogisticRegression(featuresCol="features_success", labelCol="success_level",
                                              predictionCol="predicted_success_class",
                                              probabilityCol="success_probability", maxIter=20)
            pipeline = Pipeline(stages=[assembler, lr_classifier])
            models['success'] = pipeline.fit(training_success)
            print(f"Success Classifier trained successfully")
        except Exception as e:
            print(f"Errore: {e}")
    else:
        print(f"Dati insufficienti (minimo 30)")
    
    print("\n" + "="*80)
    print(f"Training completato: {len(models)}/6 modelli trainati con successo")
    print("="*80 + "\n")
    
    return models

def update_global_models_if_needed():
    """Aggiorna i modelli globali se è passato abbastanza tempo"""
    global GLOBAL_MODELS, MODELS_LAST_UPDATE
    
    current_time = time.time()
    
    # Riallena solo se non ci sono modelli O è passata almeno un'ora
    if not GLOBAL_MODELS or (current_time - MODELS_LAST_UPDATE) > MODEL_UPDATE_INTERVAL:
        print(f"\n Aggiornamento modelli globali (ultimo update: {int((current_time - MODELS_LAST_UPDATE) / 60)} minuti fa)")
        
        # Carica TUTTI i dati storici
        historical_df = load_all_historical_data_from_elasticsearch()
        
        if historical_df and historical_df.count() > 50:
            # Train tutti i modelli
            GLOBAL_MODELS = train_all_models_with_historical_data(historical_df)
            MODELS_LAST_UPDATE = current_time
            print(f"Modelli aggiornati con {historical_df.count()} record storici totali\n")
        else:
            print("Dati storici insufficienti, mantengo modelli esistenti\n")

def enrich_with_predictions(df: DataFrame) -> DataFrame:
    """Arricchisce il DataFrame con predizioni usando modelli globali"""
    
    # Aggiorna modelli se necessario
    update_global_models_if_needed()
    
    # Parsing del JSON da Kafka
    parsed_df = df.select(
        col("timestamp"),
        from_json(col("value"), game_schema).alias("game")
    ).where(col("game").isNotNull()).select("timestamp", "game.*")
    
    # Conversione timestamp Unix a date
    enriched_df = parsed_df \
        .withColumn("release_date_formatted", 
                    to_date((col("first_release_date") / 1000).cast("timestamp"))) \
        .withColumn("current_date", current_date()) \
        .withColumn("days_to_release", 
                    datediff(col("release_date_formatted"), col("current_date"))) \
        .withColumn("days_since_release", 
                    datediff(col("current_date"), col("release_date_formatted"))) \
        .withColumn("is_released", 
                    when(col("days_since_release") >= 0, lit(True)).otherwise(lit(False))) \
        .withColumn("is_upcoming", 
                    when(col("days_to_release") >= 0, lit(True)).otherwise(lit(False)))
    
    # Calcolo metriche personalizzate
    enriched_df = enriched_df \
        .withColumn("franchise_count", 
                    when(col("franchises").isNotNull(), size(col("franchises")))
                    .otherwise(lit(0))) \
        .withColumn("platform_count", 
                    when(col("platforms").isNotNull(), size(col("platforms")))
                    .otherwise(lit(0))) \
        .withColumn("genre_count", 
                    when(col("genres").isNotNull(), size(col("genres")))
                    .otherwise(lit(0)))
    
    # Calcolo hype score
    enriched_df = enriched_df \
        .withColumn("hype_score", 
                    when(col("is_upcoming"), 
                         hype_udf(col("hypes"), col("follows"), 
                                 col("rating_count"), col("days_to_release")))
                    .otherwise(lit(None)))
    
    # Calcolo longevity score
    enriched_df = enriched_df \
        .withColumn("longevity_score", 
                    when(col("is_released"), 
                         longevity_udf(col("rating"), col("rating_count"), 
                                      col("franchise_count"), col("platform_count"), 
                                      col("days_since_release")))
                    .otherwise(lit(None)))
    
    # Classificazione successo
    enriched_df = enriched_df \
        .withColumn("success_level",
                    success_udf(col("rating"), col("rating_count")))
    
    # Riempimento valori nulli
    enriched_df = enriched_df.fillna({
        "hypes": 0,
        "follows": 0,
        "rating_count": 0,
        "franchise_count": 0,
        "platform_count": 0,
        "genre_count": 0,
        "total_rating": 0,
        "aggregated_rating": 0
    })
    
    # Applica predizioni usando i modelli globali trainati su TUTTI i dati
    if 'rating' in GLOBAL_MODELS:
        enriched_df = GLOBAL_MODELS['rating'].transform(enriched_df)
        enriched_df = enriched_df.drop("features_rating")
    else:
        enriched_df = enriched_df.withColumn("predicted_rating", lit(None))
    
    if 'rating_count' in GLOBAL_MODELS:
        enriched_df = GLOBAL_MODELS['rating_count'].transform(enriched_df)
        enriched_df = enriched_df.drop("features_count")
    else:
        enriched_df = enriched_df.withColumn("predicted_rating_count", lit(None))
    
    if 'hype' in GLOBAL_MODELS:
        enriched_df = GLOBAL_MODELS['hype'].transform(enriched_df)
        enriched_df = enriched_df.drop("features_hype")
    else:
        enriched_df = enriched_df.withColumn("predicted_hype_growth", lit(None))
    
    if 'follows' in GLOBAL_MODELS:
        enriched_df = GLOBAL_MODELS['follows'].transform(enriched_df)
        enriched_df = enriched_df.drop("features_follows")
    else:
        enriched_df = enriched_df.withColumn("predicted_follows_growth", lit(None))
    
    if 'longevity' in GLOBAL_MODELS:
        enriched_df = GLOBAL_MODELS['longevity'].transform(enriched_df)
        enriched_df = enriched_df.drop("features_longevity")
    else:
        enriched_df = enriched_df.withColumn("predicted_longevity", lit(None))
    
    if 'success' in GLOBAL_MODELS:
        enriched_df = GLOBAL_MODELS['success'].transform(enriched_df)
        enriched_df = enriched_df.drop("features_success")
    else:
        enriched_df = enriched_df.withColumn("predicted_success_class", lit(None))
        enriched_df = enriched_df.withColumn("success_probability", lit(None))
    
    # Success score
    enriched_df = enriched_df \
        .withColumn("predicted_success_score",
                    when(col("predicted_success_class").isNotNull(),
                         col("predicted_success_class") * 50.0)
                    .otherwise(lit(None)))
    
    # Confidence score
    enriched_df = enriched_df \
        .withColumn("prediction_confidence", 
                   ((col("hypes") > 0).cast("int") + 
                    (col("follows") > 0).cast("int") + 
                    (col("rating_count") > 0).cast("int") + 
                    (col("franchise_count") > 0).cast("int") +
                    (col("platform_count") > 0).cast("int") +
                    (col("genre_count") > 0).cast("int")) / 6.0 * 100)
    
    enriched_df = enriched_df.withColumn("processed_timestamp", lit(int(time.time())))
    
    return enriched_df

def send_to_elasticsearch(batch_df: DataFrame, batch_id: int):
    es = Elasticsearch(
        hosts=["http://elasticsearch:9200"],
        http_auth=("kibana_system_user", "kibanapass123"),
        max_retries=10,
        retry_on_timeout=True
    )

    if not es.ping():
        raise ValueError("Error into Elasticsearch connection")

    # Arricchimento con predizioni
    enriched_df = enrich_with_predictions(batch_df)
    
    # Conversione in JSON
    records = enriched_df.toJSON().map(json.loads).collect()

    for record in records:
        doc_id = f"{record.get('id', 0)}_{batch_id}_{int(time.time() * 1000)}"
        
        document = {
            "game_id": record.get("id"),
            "name": record.get("name"),
            "rating": record.get("rating"),
            "rating_count": record.get("rating_count"),
            "total_rating": record.get("total_rating"),
            "aggregated_rating": record.get("aggregated_rating"),
            "hypes": record.get("hypes", 0),
            "follows": record.get("follows", 0),
            "release_date": record.get("release_date_formatted"),
            "days_to_release": record.get("days_to_release"),
            "days_since_release": record.get("days_since_release"),
            "is_released": record.get("is_released"),
            "is_upcoming": record.get("is_upcoming"),
            "franchise_count": record.get("franchise_count"),
            "platform_count": record.get("platform_count"),
            "genre_count": record.get("genre_count"),
            
            # Metriche calcolate
            "hype_score": record.get("hype_score"),
            "longevity_score": record.get("longevity_score"),
            "success_level": record.get("success_level"),
            
            # PREDIZIONI ML
            "predicted_rating": record.get("predicted_rating"),
            "predicted_rating_count": record.get("predicted_rating_count"),
            "predicted_hype_growth": record.get("predicted_hype_growth"),
            "predicted_follows_growth": record.get("predicted_follows_growth"),
            "predicted_longevity": record.get("predicted_longevity"),
            "predicted_success_class": record.get("predicted_success_class"),
            "predicted_success_score": record.get("predicted_success_score"),
            "prediction_confidence": record.get("prediction_confidence"),
            
            # Metadati
            "batch_id": batch_id,
            "processed_timestamp": record.get("processed_timestamp"),
            "ingestion_time": datetime.utcnow().isoformat()
        }
        
        update_body = {
            "doc": document,
            "doc_as_upsert": True
        }

        print(f"{record.get('name', 'Unknown')[:30]:<30} | "
              f"Hype: {document.get('hype_score', 0):>5.1f} | "
              f"Long: {document.get('longevity_score', 0):>5.1f} | "
              f"P.Rating: {document.get('predicted_rating', 0):>5.1f} | "
              f"Success: {document.get('predicted_success_score', 0):>5.1f}")
        
        es.update(index="games", id=doc_id, body=update_body)


while not check_elasticsearch_connection():
    print("Connessione a Elasticsearch...")
    time.sleep(3)

print("Connessione a ElasticSearch stabilita")

# Carica modelli iniziali con TUTTI i dati storici
print("\n Inizializzazione modelli ML con dati storici...")
update_global_models_if_needed()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", topic) \
    .load()

df.selectExpr("CAST(timestamp AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .foreachBatch(send_to_elasticsearch) \
    .start() \
    .awaitTermination()