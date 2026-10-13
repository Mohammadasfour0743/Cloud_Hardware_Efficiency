from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, to_timestamp, lower, concat, lpad, lit, when, trim
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame
from delta import configure_spark_with_delta_pip
import re
import time
import json

#The second line (with 4g) specifies how much RAM to use. change according to machine
spark = SparkSession.builder \
    .appName("IngestionFramework") \
    .config("spark.driver.memory", "4g") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


TYPE_MAP = {"long": LongType(),
            "double": DoubleType(),
            "string": StringType()}

def build_schema(columns):
    return StructType([StructField(c["name"], TYPE_MAP[c["type"]], True)
                       for c in columns])

def clean(df, cfg):
    if cfg["keep_status"]:
        df = df.filter(F.col("status") == cfg["keep_status"])
    for col in cfg["not_null"]:
        df = df.filter(F.col(col).isNotNull())
    for col in cfg["not_zero"]:
        df = df.filter(F.col(col) != 0)
    return df

#Check README for more info
def check_units():
    inst = spark.read.format("delta").load("delta/batch_instance")
    task = spark.read.format("delta").load("delta/batch_task")

    task.select("plan_cpu", "plan_mem").describe().show()
    inst.select("real_cpu_avg", "real_mem_avg").describe().show()


#Big functions

def load_data(name, cfg):
    schema = build_schema(cfg["columns"])

    raw = spark.read.option("header", "false").schema(schema).csv(cfg["path"])
    before = raw.count()

    cleaned = clean(raw, cfg)

    output_path = f"./delta/{name}"
    cleaned.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").save(output_path)

    after = spark.read.format("delta").load(output_path).count()
    dropped_pct = 100 * (before - after) / before if before else 0
    stats.append((name, before, after, dropped_pct))
    print(f"{name}: {before} -> {after} rows ({dropped_pct:.2f}% dropped)")

def join_tables():
    inst = spark.read.format("delta").load("delta/batch_instance")
    task = spark.read.format("delta").load("delta/batch_task")

    return inst.join(task, on = ["job_id", "task_id"], how = "inner")


# main
with open("config.json") as f:
    config = json.load(f)

stats = []

for name, cfg in config["datasets"].items():
    print(f"Loading {name}...")
    load_data(name, cfg)
    print(f"Finished: {name}...")


