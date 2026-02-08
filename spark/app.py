from __future__ import print_function
from elasticsearch import Elasticsearch
import json
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import time


sc = SparkContext(appName="PythonStructuredStreamsKafka")
spark = SparkSession(sc)
print(spark.version)
sc.setLogLevel("WARN")

kafkaServer="kafka:39092"
topic = "gameStatsPx"

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

def send_to_elasticsearch(batch_df: DataFrame, batch_id: int):
    es = Elasticsearch(hosts=["http://elasticsearch:9200"],http_auth=("kibana_system_user", "kibanapass123"),max_retries=10,retry_on_timeout=True)

    if not es.ping():
        raise ValueError("Error into Elasticsearch connectection")

    records = batch_df.toJSON().map(json.loads).collect()

    for record in records:
        converted_dict = json.loads(record["value"])
        # Use timestamp as ID
        doc_id =  int(str(time.time()).replace(".", ""))
 
        update_body = {
            "doc": converted_dict,
            "doc_as_upsert": True
        }

        print("Sent to Elastichsearch")
        es.update(index="games", id=doc_id, body=update_body)


while not check_elasticsearch_connection():
    print("Connectiong to Elastichsearch..")
    time.sleep(3)

print("Connection to ElasticSearch established")

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafkaServer) \
  .option("subscribe", topic) \
  .load()

df.selectExpr("CAST(timestamp AS STRING)","CAST(value AS STRING)") \
  .writeStream \
  .foreachBatch(send_to_elasticsearch) \
  .start() \
  .awaitTermination()