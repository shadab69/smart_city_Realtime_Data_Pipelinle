import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from confluent_kafka.schema_registry import SchemaRegistryClient

# Load environment variables
def load_config():
    return {
        "kafka": {
            "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{os.getenv("KAFKA_API_KEY")}" password="{os.getenv("KAFKA_API_SECRET")}";'
        },
        "schema_registry": {
            "url": os.getenv("SCHEMA_REGISTRY_URL"),
            "basic.auth.user.info": f"{os.getenv('SCHEMA_REGISTRY_KEY')}:{os.getenv('SCHEMA_REGISTRY_SECRET')}"
        }
    }

# Initialize Spark with Hive support
spark = SparkSession.builder \
    .appName("SmartCityStreaming") \
    .config("hive.metastore.uris", f"thrift://{os.getenv('HIVE_HOST')}:{os.getenv('HIVE_PORT')}") \
    .config("spark.sql.shuffle.partitions", "4") \
    .enableHiveSupport() \
    .getOrCreate()

# Schema Registry Client
schema_registry_conf = {
    'url': os.getenv('SCHEMA_REGISTRY_URL'),
    'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_KEY')}:{os.getenv('SCHEMA_REGISTRY_SECRET')}"
}
schema_client = SchemaRegistryClient(schema_registry_conf)

def get_schema(topic):
    """Fetch latest Avro schema from Schema Registry"""
    schema = schema_client.get_latest_version(f"{topic}-value").schema.schema_str
    return json.loads(schema)

# Convert Avro schema to Spark StructType
def avro_to_spark_schema(avro_schema):
    fields = []
    for field in avro_schema['fields']:
        if field['type'] == 'string':
            data_type = StringType()
        elif field['type'] == 'int':
            data_type = IntegerType()
        elif field['type'] == 'double':
            data_type = DoubleType()
        elif isinstance(field['type'], dict) and field['type']['type'] == 'record':
            data_type = StructType([
                StructType([StructType([
                    StructField(sub_field['name'], 
                    StringType() if sub_field['type'] == 'string' else DoubleType(), 
                    True) for sub_field in field['type']['fields']
                ])])])
        else:
            data_type = StringType()
        fields.append(StructField(field['name'], data_type, True))
    return StructType(fields)

# Get schemas for both topics
traffic_schema = avro_to_spark_schema(get_schema("traffic-data"))
emergency_schema = avro_to_spark_schema(get_schema("emergency-events"))

# Create streaming DataFrames
def create_stream(topic, schema):
    return spark.readStream \
        .format("kafka") \
        .options(**load_config()["kafka"]) \
        .option("subscribe", topic) \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .withWatermark("timestamp", "10 minutes")

# Process traffic stream
traffic_stream = create_stream("traffic-data", traffic_schema) \
    .withColumn("coordinates", struct(col("coordinates.lat"), col("coordinates.lon"))) \
    .withColumnRenamed("timestamp", "traffic_ts")

# Process emergency stream
emergency_stream = create_stream("emergency-events", emergency_schema) \
    .withColumn("coordinates", struct(col("coordinates.lat"), col("coordinates.lon"))) \
    .withColumnRenamed("timestamp", "emergency_ts")

# Geospatial processing functions
@udf(returnType=DoubleType())
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km
    lat_diff = radians(lat2 - lat1)
    lon_diff = radians(lon2 - lon1)
    a = sin(lat_diff/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(lon_diff/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c

# Join streams with geospatial condition
joined_stream = emergency_stream.alias("emergency").join(
    traffic_stream.alias("traffic"),
    expr("""
        haversine(emergency.coordinates.lat, emergency.coordinates.lon, 
                 traffic.coordinates.lat, traffic.coordinates.lon) < 1 AND
        emergency.emergency_ts BETWEEN traffic.traffic_ts - INTERVAL 5 MINUTES AND traffic.traffic_ts + INTERVAL 5 MINUTES
    """),
    "leftOuter"
)

# Calculate risk score
risk_assessment = joined_stream.withColumn(
    "risk_score",
    (col("congestion") * 0.7) + 
    (when(col("severity") == "Critical", 30)
      .when(col("severity") == "High", 20)
      .when(col("severity") == "Medium", 10)
      .otherwise(5))
).withColumn(
    "alert_level",
    when(col("risk_score") >= 85, "SEVERE")
    .when(col("risk_score") >= 65, "HIGH")
    .otherwise("MODERATE")
).withColumn("processing_time", current_timestamp())

# Write to multiple sinks
def write_to_hive(batch_df, batch_id):
    batch_df.select(
        "emergency_id", "type", "location", "severity",
        "congestion", "risk_score", "alert_level",
        col("emergency_ts").alias("event_time"),
        col("coordinates.lat").alias("lat"),
        col("coordinates.lon").alias("lon")
    ).write \
     .format("hive") \
     .mode("append") \
     .saveAsTable("smart_city.emergency_alerts")

# Console output for debugging
console_query = risk_assessment.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Hive output
hive_query = risk_assessment.writeStream \
    .foreachBatch(write_to_hive) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/hive") \
    .start()

# Kafka output for real-time alerts
kafka_query = risk_assessment.select(
    to_json(struct(
        "emergency_id", "type", "severity", 
        "risk_score", "alert_level", 
        col("coordinates.lat").alias("lat"),
        col("coordinates.lon").alias("lon")
    )).alias("value")
).writeStream \
 .format("kafka") \
 .options(**load_config()["kafka"]) \
 .option("topic", "realtime-alerts") \
 .option("checkpointLocation", "/tmp/checkpoints/kafka") \
 .start()

spark.streams.awaitAnyTermination()