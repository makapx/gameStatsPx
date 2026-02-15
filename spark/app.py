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
import requests
from bs4 import BeautifulSoup
import re

sc = SparkContext(appName="PythonStructuredStreamsKafka_v2_NoTwitter")
spark = SparkSession(sc)
print(spark.version)
sc.setLogLevel("WARN")

kafkaServer = "kafka:39092"
topic = "gameStatsPx"

# Schema AGGIORNATO per i dati IGDB con campi tradotti
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
    
    # CAMPI TRADOTTI
    StructField("genres", ArrayType(StringType()), True),
    StructField("platforms", ArrayType(StringType()), True),
    StructField("game_modes", ArrayType(StringType()), True),
    StructField("themes", ArrayType(StringType()), True),
    StructField("player_perspectives", ArrayType(StringType()), True),
    
    # IDs originali
    StructField("genre_ids", ArrayType(IntegerType()), True),
    StructField("platform_ids", ArrayType(IntegerType()), True),
    StructField("game_mode_ids", ArrayType(IntegerType()), True),
    StructField("theme_ids", ArrayType(IntegerType()), True),
    StructField("player_perspective_ids", ArrayType(IntegerType()), True),
    
    # Campi già calcolati dal crawler
    StructField("days_since_release", IntegerType(), True),
    StructField("days_to_release", IntegerType(), True),
    StructField("is_released", BooleanType(), True),
    StructField("is_upcoming", BooleanType(), True),
    StructField("franchise_count", IntegerType(), True),
    StructField("platform_count", IntegerType(), True),
    StructField("genre_count", IntegerType(), True),
    StructField("hype_score", DoubleType(), True),
    StructField("longevity_score", DoubleType(), True),
    StructField("success_level", IntegerType(), True),
])

# Schema per dati da Elasticsearch
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
    StructField("genres", ArrayType(StringType()), True),
    StructField("platforms", ArrayType(StringType()), True),
    StructField("game_modes", ArrayType(StringType()), True),
    StructField("themes", ArrayType(StringType()), True),
    StructField("community_sentiment_score", DoubleType(), True),
])

# Variabili globali
GLOBAL_MODELS = {}
MODELS_LAST_UPDATE = 0
MODEL_UPDATE_INTERVAL = 3600

# Cache per sentiment
SENTIMENT_CACHE = {}
SENTIMENT_CACHE_EXPIRY = 3600  # 1 ora

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
        print(f"❌ Errore durante il ping a Elasticsearch: {e}")
        return False

def fetch_reddit_sentiment(game_name):
    """
    Retrive data from Reddit
    """
    try:
        # Search on r/gaming e r/Games. Use old since it's easier to parse
        search_url = f"https://old.reddit.com/r/gaming/search.json?q={game_name.replace(' ', '+')}&restrict_sr=1&sort=relevance&t=month"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(search_url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            return None, None, None
        
        data = response.json()
        posts = data.get('data', {}).get('children', [])
        
        if not posts:
            return 0.0, "neutral", 0
        
        sentiment_scores = []
        total_score = 0
        
        positive_keywords = ['amazing', 'great', 'awesome', 'love', 'excited', 'hyped', 
                           'fantastic', 'incredible', 'best', 'perfect', 'masterpiece',
                           'brilliant', 'stunning', 'epic']
        
        negative_keywords = ['bad', 'terrible', 'awful', 'hate', 'disappointed', 'boring',
                            'worst', 'trash', 'garbage', 'disaster', 'failure',
                            'broken', 'buggy', 'mess']
        
        for post in posts[:50]:
            post_data = post.get('data', {})
            title = post_data.get('title', '').lower()
            score = post_data.get('score', 0)
            
            # Sentiment from keywords
            pos_count = sum(1 for word in positive_keywords if word in title)
            neg_count = sum(1 for word in negative_keywords if word in title)
            
            # Sentiment from Reddit score
            if score > 100:
                score_sentiment = 1.0
            elif score > 50:
                score_sentiment = 0.5
            elif score > 10:
                score_sentiment = 0.2
            elif score < -10:
                score_sentiment = -0.5
            else:
                score_sentiment = 0.0
            
            if pos_count > neg_count:
                keyword_sentiment = min(1.0, (pos_count - neg_count) / 2.0)
            elif neg_count > pos_count:
                keyword_sentiment = max(-1.0, (pos_count - neg_count) / 2.0)
            else:
                keyword_sentiment = 0.0
            
            final_sentiment = keyword_sentiment * 0.6 + score_sentiment * 0.4
            sentiment_scores.append(final_sentiment)
            total_score += score
        
        if not sentiment_scores:
            return 0.0, "neutral", 0
        
        avg_sentiment = sum(sentiment_scores) / len(sentiment_scores)
        sentiment_score = (avg_sentiment + 1.0) * 50.0 
        
        if sentiment_score >= 60:
            sentiment_label = "positive"
        elif sentiment_score >= 40:
            sentiment_label = "neutral"
        else:
            sentiment_label = "negative"
        
        volume = len(posts)
        
        return sentiment_score, sentiment_label, volume
        
    except Exception as e:
        print(f"Error on Reddit sentiment for {game_name}: {e}")
        return None, None, None

def fetch_metacritic_user_sentiment(game_name):
    """
    Fetch sentiment from user reviews Metacritic
    """
    try:
        search_name = game_name.lower().replace(' ', '-').replace(':', '')
        url = f"https://www.metacritic.com/game/{search_name}/"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            return None, None
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        user_score_elem = soup.find('div', {'class': 'c-siteReviewScore_user'})
        
        if user_score_elem:
            score_text = user_score_elem.find('span')
            if score_text:
                try:
                    user_score = float(score_text.text.strip())
                    normalized_score = user_score * 10.0
                    
                    if normalized_score >= 75:
                        label = "positive"
                    elif normalized_score >= 50:
                        label = "neutral"
                    else:
                        label = "negative"
                    
                    return normalized_score, label
                except:
                    pass
        
        return None, None
        
    except Exception as e:
        print(f"Error in Metacritic sentiment for {game_name}: {e}")
        return None, None

def calculate_community_sentiment(game_name, hypes, follows, days_to_release):
    """
    Calculate and combine data for sentiment analysis
    Use: Reddit, Metacritic, and native IGDB metrics
    """
    
    # Skip released and not upcoming games
    if days_to_release is None or days_to_release < 0 or days_to_release > 365:
        return None, None, None, None
    
    # Check cache
    cache_key = f"{game_name}_{days_to_release // 7}"
    current_time = time.time()
    
    if cache_key in SENTIMENT_CACHE:
        cached_data, cached_time = SENTIMENT_CACHE[cache_key]
        if current_time - cached_time < SENTIMENT_CACHE_EXPIRY:
            return (cached_data['sentiment_score'], cached_data['sentiment_label'], 
                   cached_data['volume'], cached_data['source'])
    
    sentiment_scores = []
    sources_used = []
    total_volume = 0
    
    # 1. REDDIT SENTIMENT
    reddit_score, reddit_label, reddit_volume = fetch_reddit_sentiment(game_name)
    if reddit_score is not None:
        sentiment_scores.append(('reddit', reddit_score, 0.4))  # Peso 40%
        sources_used.append('reddit')
        total_volume += reddit_volume or 0
    
    # 2. METACRITIC USER SENTIMENT  
    meta_score, meta_label = fetch_metacritic_user_sentiment(game_name)
    if meta_score is not None:
        sentiment_scores.append(('metacritic', meta_score, 0.3))  # Peso 30%
        sources_used.append('metacritic')
    
    # 3. IGDB NATIVE METRICS (hypes, follows)
    if hypes is not None and follows is not None:
        hype_factor = min(100.0, (hypes / 100.0) * 50.0)
        follow_factor = min(100.0, (follows / 1000.0) * 50.0)
        igdb_score = (hype_factor + follow_factor) / 2.0        
        sentiment_scores.append(('igdb_metrics', igdb_score, 0.3))  # Peso 30%
        sources_used.append('igdb')
    
    if not sentiment_scores:
        return None, None, None, None
    
    weighted_sum = sum(score * weight for _, score, weight in sentiment_scores)
    total_weight = sum(weight for _, _, weight in sentiment_scores)
    final_score = weighted_sum / total_weight if total_weight > 0 else 50.0
    
    if final_score >= 60:
        final_label = "positive"
    elif final_score >= 40:
        final_label = "neutral"
    else:
        final_label = "negative"
    
    sources_str = '+'.join(sources_used)
    
    SENTIMENT_CACHE[cache_key] = (
        {
            'sentiment_score': final_score,
            'sentiment_label': final_label,
            'volume': total_volume,
            'source': sources_str
        },
        current_time
    )
    
    return final_score, final_label, total_volume, sources_str

def load_all_historical_data_from_elasticsearch():
    """Load data from Elasticsearch"""
    es = Elasticsearch(
        hosts=["http://elasticsearch:9200"],
        http_auth=("kibana_system_user", "kibanapass123"),
        max_retries=10,
        retry_on_timeout=True
    )
    
    print("Load Elasticsearch data...")
    try:
        if not es.indices.exists(index="games"):
            print("Index 'games' not exists")
            print("ML models will be trained when data will be available")
            return None
        
        query = {"query": {"match_all": {}}, "_source": True}
        all_docs = []
        
        for doc in scan(es, index="games", query=query, scroll='5m', size=1000):
            all_docs.append(doc['_source'])
        
        if not all_docs:
            print("No data available")
            return None
        
        print(f"Loaded {len(all_docs)} record")
        historical_df = spark.createDataFrame(all_docs, schema=es_schema)
        
        # Add fields if not exists
        existing_columns = historical_df.columns
        
        if "community_sentiment_score" not in existing_columns:
            historical_df = historical_df.withColumn("community_sentiment_score", lit(50.0))
        
        historical_df = historical_df.fillna({
            "hypes": 0, "follows": 0, "rating_count": 0,
            "franchise_count": 0, "platform_count": 0, "genre_count": 0,
            "total_rating": 0, "aggregated_rating": 0,
            "days_to_release": 0, "days_since_release": 0,
            "hype_score": 0, "longevity_score": 0, "success_level": 0,
            "community_sentiment_score": 50.0
        })
        
        return historical_df
        
    except Exception as e:
        print(f"Error in loading Elasticsearch data: {e}")
        return None

def calculate_genre_popularity_score(genres):
    """Calculate score based on popular genres"""
    if not genres:
        return 0.0
    
    popular_genres = {
        'Action': 1.2, 'RPG': 1.15, 'Adventure': 1.1, 'Strategy': 1.0,
        'Shooter': 1.1, 'Fighting': 0.9, 'Racing': 0.85, 'Sports': 0.9,
        'Simulator': 0.8, 'Indie': 0.75, 'Puzzle': 0.7, 'Platform': 0.9
    }
    
    total_weight = sum(popular_genres.get(genre, 0.7) for genre in genres)
    return min(100.0, (total_weight / len(genres)) * 50.0)

def calculate_platform_reach_score(platforms):
    """Calculate platform score"""
    if not platforms:
        return 0.0
    
    platform_weights = {
        'PC (Microsoft Windows)': 1.0, 'PlayStation 5': 1.2, 'Xbox Series X|S': 1.1,
        'Nintendo Switch': 1.15, 'PlayStation 4': 0.9, 'Xbox One': 0.85,
        'iOS': 0.7, 'Android': 0.7, 'Mac': 0.6, 'Linux': 0.5
    }
    
    total_weight = sum(platform_weights.get(platform, 0.5) for platform in platforms)
    return min(100.0, total_weight * 25.0)

genre_popularity_udf = udf(calculate_genre_popularity_score, DoubleType())
platform_reach_udf = udf(calculate_platform_reach_score, DoubleType())

def train_all_models_with_historical_data(historical_df):
    """Training models"""
    
    print("Training models - Sentiment Analysis")
    
    models = {}
    feature_cols = ["hypes", "follows", "rating_count", "franchise_count", 
                    "platform_count", "genre_count"]
    
    feature_cols_extended = feature_cols + ["total_rating", "aggregated_rating"]

    feature_cols_with_sentiment = feature_cols_extended + ["community_sentiment_score"]
    
    total_records = historical_df.count()
    print(f"\nDataset: {total_records} record")
    
    # 1. RATING MODEL (GBT) - SENTIMENT
    training_rating = historical_df.filter(col("rating").isNotNull())
    
    count_rating = training_rating.count()
    print(f"\n Rating Model (with sentiment): {count_rating} record")
    
    if count_rating > 50:
        try:
            assembler = VectorAssembler(inputCols=feature_cols_with_sentiment, 
                                       outputCol="features_rating", handleInvalid="skip")
            gbt = GBTRegressor(featuresCol="features_rating", labelCol="rating",
                              predictionCol="predicted_rating", maxIter=50, maxDepth=6)
            pipeline = Pipeline(stages=[assembler, gbt])
            models['rating'] = pipeline.fit(training_rating)
            print(f"Trained successfully (including sentiment feature)")
        except Exception as e:
            print(f"Error: {e}")
    
    # 2. ALTERNATIVE RATING MODEL (NO SENTIMENT) - fallback
    print(f"\n Rating Model (no sentiment - fallback): {count_rating} record")

    if count_rating > 50:
        try:
            assembler = VectorAssembler(inputCols=feature_cols_extended, 
                                       outputCol="features_rating_base", handleInvalid="skip")
            gbt = GBTRegressor(featuresCol="features_rating_base", labelCol="rating",
                              predictionCol="predicted_rating_base", maxIter=50, maxDepth=6)
            pipeline = Pipeline(stages=[assembler, gbt])
            models['rating_base'] = pipeline.fit(training_rating)
            print(f"Trained successfully (base model)")
        except Exception as e:
            print(f"Error: {e}")
    
    # 2. RATING COUNT MODEL
    training_count = historical_df.filter((col("rating_count").isNotNull()) & 
                                         (col("rating_count") > 0))
    count_rating_count = training_count.count()
    print(f"\nRating Count Model: {count_rating_count} record")
    
    if count_rating_count > 30:
        try:
            assembler = VectorAssembler(inputCols=feature_cols, 
                                       outputCol="features_count", handleInvalid="skip")
            rf = RandomForestRegressor(featuresCol="features_count", labelCol="rating_count",
                                      predictionCol="predicted_rating_count", numTrees=30)
            pipeline = Pipeline(stages=[assembler, rf])
            models['rating_count'] = pipeline.fit(training_count)
            print(f"Trained successfully")
        except Exception as e:
            print(f"Error: {e}")
    
    # 3. LONGEVITY MODEL
    training_longevity = historical_df.filter(col("longevity_score").isNotNull())
    count_longevity = training_longevity.count()
    print(f"\nLongevity Model: {count_longevity} record")
    
    if count_longevity > 30:
        try:
            assembler = VectorAssembler(inputCols=feature_cols_extended, 
                                       outputCol="features_longevity", handleInvalid="skip")
            rf = RandomForestRegressor(featuresCol="features_longevity", 
                                      labelCol="longevity_score",
                                      predictionCol="predicted_longevity", numTrees=40)
            pipeline = Pipeline(stages=[assembler, rf])
            models['longevity'] = pipeline.fit(training_longevity)
            print(f"Trained successfully")
        except Exception as e:
            print(f"Error: {e}")
    
    # 4. SUCCESS CLASSIFIER
    training_success = historical_df.filter(col("success_level").isNotNull())
    count_success = training_success.count()
    print(f"\nSuccess Classifier: {count_success} record")
    
    if count_success > 50:
        try:
            assembler = VectorAssembler(inputCols=feature_cols_extended, 
                                       outputCol="features_success", handleInvalid="skip")
            lr = LogisticRegression(featuresCol="features_success", labelCol="success_level",
                                   predictionCol="predicted_success_class", maxIter=30)
            pipeline = Pipeline(stages=[assembler, lr])
            models['success'] = pipeline.fit(training_success)
            print(f"Trained successfully")
        except Exception as e:
            print(f"Error: {e}")
    
    print(f"Training completed: {len(models)}/4 ML models")
    
    return models

def update_global_models_if_needed():
    """ML models update"""
    global GLOBAL_MODELS, MODELS_LAST_UPDATE
    
    current_time = time.time()
    
    if not GLOBAL_MODELS or (current_time - MODELS_LAST_UPDATE) > MODEL_UPDATE_INTERVAL:
        print(f"\n Update models...")
        
        historical_df = load_all_historical_data_from_elasticsearch()
        
        if historical_df and historical_df.count() > 100:
            GLOBAL_MODELS = train_all_models_with_historical_data(historical_df)
            MODELS_LAST_UPDATE = current_time
            print(f"ML models updated successfully\n")
        else:
            if not historical_df:
                print("'games' index empty or not exists")
                print("ML models will be trained when data will be available")
            else:
                print(f"Only {historical_df.count()} record availabled - required at least 100")
            print("Continue ML prediction...\n")

def enrich_with_predictions(df: DataFrame) -> DataFrame:
    """Enrich DataFrame with ML e sentiment"""
    
    update_global_models_if_needed()
    
    # Parsing JSON from Kafka
    parsed_df = df.select(
        col("timestamp"),
        from_json(col("value"), game_schema).alias("game")
    ).where(col("game").isNotNull()).select("timestamp", "game.*")
    
    enriched_df = parsed_df
    
    enriched_df = enriched_df \
        .withColumn("genre_popularity_score", genre_popularity_udf(col("genres"))) \
        .withColumn("platform_reach_score", platform_reach_udf(col("platforms")))
    
    enriched_df = enriched_df.fillna({
        "hypes": 0, "follows": 0, "rating_count": 0,
        "franchise_count": 0, "platform_count": 0, "genre_count": 0,
        "total_rating": 0, "aggregated_rating": 0
    })
    
    # COMMUNITY SENTIMENT (Reddit + Metacritic + IGDB)
    print(f"\n Calculating community sentiment...")
    records = enriched_df.collect()
    
    if not records:
        print(f"Empty batch, skipping sentiment analysis")
        return enriched_df.withColumn("community_sentiment_score", lit(50.0)) \
                          .withColumn("community_sentiment_label", lit("neutral")) \
                          .withColumn("community_data_volume", lit(None)) \
                          .withColumn("sentiment_source", lit(None)) \
                          .withColumn("predicted_rating", lit(None)) \
                          .withColumn("predicted_rating_count", lit(None)) \
                          .withColumn("predicted_longevity", lit(None)) \
                          .withColumn("predicted_success_class", lit(None)) \
                          .withColumn("predicted_success_score", lit(None)) \
                          .withColumn("prediction_confidence", lit(None)) \
                          .withColumn("processed_timestamp", lit(int(time.time())))
    
    sentiment_data = []
    
    for record in records:
        game_name = record['name']
        days_to_release = record['days_to_release']
        is_upcoming = record['is_upcoming']
        hypes = record['hypes']
        follows = record['follows']
        
        if is_upcoming and days_to_release and 0 < days_to_release <= 365:
            sentiment_score, sentiment_label, volume, source = calculate_community_sentiment(
                game_name, hypes, follows, days_to_release
            )
            sentiment_data.append({
                'id': record['id'],
                'community_sentiment_score': sentiment_score if sentiment_score else 50.0,  # Default neutro
                'community_sentiment_label': sentiment_label,
                'community_data_volume': volume,
                'sentiment_source': source
            })
        else:
            # Neutral score for existing games
            sentiment_data.append({
                'id': record['id'],
                'community_sentiment_score': 50.0,  # Neutro
                'community_sentiment_label': "neutral",
                'community_data_volume': None,
                'sentiment_source': None
            })
    
    # Join sentiment before ML
    from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
    
    # Schema
    sentiment_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("community_sentiment_score", DoubleType(), True),
        StructField("community_sentiment_label", StringType(), True),
        StructField("community_data_volume", IntegerType(), True),
        StructField("sentiment_source", StringType(), True)
    ])
    
    sentiment_df = spark.createDataFrame(sentiment_data, schema=sentiment_schema)
    enriched_df = enriched_df.join(sentiment_df, on='id', how='left')
    
    print(f"Applying ML predictions (sentiment-aware)...")
    
    if 'rating' in GLOBAL_MODELS:
        enriched_df = GLOBAL_MODELS['rating'].transform(enriched_df)
        enriched_df = enriched_df.drop("features_rating")
        print(f"Rating prediction (with sentiment)")
    else:
        enriched_df = enriched_df.withColumn("predicted_rating", lit(None))
        print(f"Rating model not available")
    
    # Fallback: no sentiment-aware
    if 'rating_base' in GLOBAL_MODELS and 'predicted_rating' not in enriched_df.columns:
        enriched_df = GLOBAL_MODELS['rating_base'].transform(enriched_df)
        enriched_df = enriched_df.withColumnRenamed("predicted_rating_base", "predicted_rating")
        enriched_df = enriched_df.drop("features_rating_base")
        print(f"   ✅ Rating prediction (base model fallback)")
    
    if 'rating_count' in GLOBAL_MODELS:
        enriched_df = GLOBAL_MODELS['rating_count'].transform(enriched_df)
        enriched_df = enriched_df.drop("features_count")
    else:
        enriched_df = enriched_df.withColumn("predicted_rating_count", lit(None))
    
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
    
    # predicted_rating NOW ALREADY INCLUDES SENTIMENT as an ML feature
    # Sentiment_adjusted_rating no longer needs to be calculated separately.
    # GBT model has learned to weight sentiment automatically.
    
    # Success score
    enriched_df = enriched_df \
        .withColumn("predicted_success_score",
                    when(col("predicted_success_class").isNotNull(),
                         col("predicted_success_class") * 50.0)
                    .otherwise(lit(None)))
    
    # Enriched confidence
    enriched_df = enriched_df \
        .withColumn("base_confidence",
                   ((col("hypes") > 0).cast("int") + 
                    (col("follows") > 0).cast("int") + 
                    (col("rating_count") > 0).cast("int") + 
                    (col("franchise_count") > 0).cast("int") +
                    (col("platform_count") > 0).cast("int") +
                    (col("genre_count") > 0).cast("int")) / 6.0) \
        .withColumn("sentiment_boost",
                    when(col("community_data_volume") >= 50, lit(0.15))
                    .when(col("community_data_volume") >= 20, lit(0.10))
                    .when(col("community_data_volume") >= 5, lit(0.05))
                    .otherwise(lit(0.0))) \
        .withColumn("prediction_confidence",
                    (col("base_confidence") + col("sentiment_boost")) * 100) \
        .drop("base_confidence", "sentiment_boost")
    
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
        raise ValueError("Elasticsearch connection error")

    print(f"Processing Batch #{batch_id}")
    
    enriched_df = enrich_with_predictions(batch_df)
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
            
            # Human readable fields
            "genres": record.get("genres", []),
            "platforms": record.get("platforms", []),
            "game_modes": record.get("game_modes", []),
            "themes": record.get("themes", []),
            "player_perspectives": record.get("player_perspectives", []),
            
            "genre_popularity_score": record.get("genre_popularity_score"),
            "platform_reach_score": record.get("platform_reach_score"),
            "hype_score": record.get("hype_score"),
            "longevity_score": record.get("longevity_score"),
            "success_level": record.get("success_level"),
            
            # COMMUNITY SENTIMENT
            "community_sentiment_score": record.get("community_sentiment_score"),
            "community_sentiment_label": record.get("community_sentiment_label"),
            "community_data_volume": record.get("community_data_volume"),
            "sentiment_source": record.get("sentiment_source"),
            
            # ML Predictions
            "predicted_rating": record.get("predicted_rating"),
            "predicted_rating_count": record.get("predicted_rating_count"),
            "predicted_longevity": record.get("predicted_longevity"),
            "predicted_success_class": record.get("predicted_success_class"),
            "predicted_success_score": record.get("predicted_success_score"),
            "prediction_confidence": record.get("prediction_confidence"),
            
            # Metadata
            "batch_id": batch_id,
            "processed_timestamp": record.get("processed_timestamp"),
            "ingestion_time": datetime.utcnow().isoformat()
        }
        
        update_body = {"doc": document, "doc_as_upsert": True}

        sentiment_str = ""
        if document.get('community_sentiment_score') and document.get('community_sentiment_score') != 50.0:
            source = document.get('sentiment_source', 'N/A')
            sentiment_str = f"| Sentiment: {document['community_sentiment_label'][:3].upper()} {document['community_sentiment_score']:.0f} ({source})"
        
        print(f"{'🎮' if record.get('is_upcoming') else '✅'} "
              f"{record.get('name', 'Unknown')[:35]:<35} | "
              f"P.Rating: {document.get('predicted_rating', 0):>5.1f} (sentiment-aware) "
              f"{sentiment_str}")
        
        es.update(index="games", id=doc_id, body=update_body)

# Main
print("SPARK STREAMING")
print("Sentiment from: Reddit + Metacritic + IGDB native metrics")

while not check_elasticsearch_connection():
    print("Connessione a Elasticsearch...")
    time.sleep(3)

print("Elasticsearch connected\n")
print("Init ML models...")
update_global_models_if_needed()

print("\nStarting Kafka streaming...")
print(f"Topic: {topic}")
print(f"Server: {kafkaServer}\n")

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