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

    # necessary to rename each table's status column as both of them have one named "status"
    inst = spark.read.format("delta").load("delta/batch_instance")
    inst = inst.withColumnRenamed("status", "status_inst")

    task = spark.read.format("delta").load("delta/batch_task")
    task = task.withColumnRenamed("status", "status_task")

    return inst.join(task, on = ["job_id", "task_id"], how = "inner")

# def normalize_plan_cpu():
#     joined_inst_task = spark.read.format("delta").load("delta/joined_inst_task")
        

def perform_calculations():
    joined_inst_task = spark.read.format("delta").load("delta/joined_inst_task")

    metrics_df = joined_inst_task\
            .withColumn("cpu_waste", F.col("plan_cpu_norm") - F.col("real_cpu_avg"))\
            .withColumn("mem_waste", F.col("plan_mem") - F.col("real_mem_avg"))\
            .withColumn("duration", F.col("end_timestamp") - F.col("start_timestamp"))\
            .withColumn("cpu_waste_core_seconds", F.col("cpu_waste") * F.col("duration"))\
            .withColumn("mem_waste_weighted", F.col("mem_waste") * F.col("duration"))\
            .withColumn("cpu_use_ratio", F.col("real_cpu_avg")/F.col("plan_cpu_norm"))\
            .withColumn("mem_use_ratio", F.col("real_mem_avg")/F.col("plan_mem"))
    metrics_df.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").save("./delta/metrics")
                        
    # metrics_df.select("job_id", "task_id", "cpu_waste", "mem_waste", "duration", "cpu_waste_core_seconds", "mem_waste_weighted", "cpu_use_ratio", "mem_use_ratio").show(3)
    # joined_inst_task.select("job_id", "task_id", "real_cpu_avg", "plan_cpu").show(3)
    

def perform_calculations_grouped():
    metrics_df = spark.read.format("delta").load("delta/metrics")
    # metrics_df.select("job_id", "task_id", "mem_waste", "mem_waste_weighted").show(3)
    
    metrics_machine  = metrics_df.select("machine_id", "cpu_waste", "mem_waste", "cpu_waste_core_seconds", "mem_waste_weighted")\
                .groupBy("machine_id")\
                .agg(F.sum("cpu_waste_core_seconds").alias("total_cpu_waste"),
                     F.sum("mem_waste_weighted").alias("total_mem_waste"),
                    #  F.sum("cpu_waste").alias("total_avg_cpu_waste"),
                    #  F.sum("mem_waste").alias("total_avg_mem_waste")
                     )
    metrics_jobs  = metrics_df.select("job_id", "cpu_waste", "mem_waste", "cpu_waste_core_seconds", "mem_waste_weighted")\
                    .groupBy("job_id")\
                    .agg(F.sum("cpu_waste_core_seconds").alias("total_cpu_waste"),
                         F.sum("mem_waste_weighted").alias("total_mem_waste"),
                        #  F.sum("cpu_waste").alias("total_avg_cpu_waste"),
                        #  F.sum("mem_waste").alias("total_avg_mem_waste")
                         )

    metrics_time = metrics_df.select("start_timestamp", "cpu_waste", "mem_waste", "cpu_waste_core_seconds", "mem_waste_weighted")\
                    .groupBy("start_timestamp")\
                    .agg(F.sum("cpu_waste_core_seconds").alias("total_cpu_waste"),
                         F.sum("mem_waste_weighted").alias("total_mem_waste"),
                        #  F.sum("cpu_waste").alias("total_avg_cpu_waste"),
                        #  F.sum("mem_waste").alias("total_avg_mem_waste")
                        )

    
    # metrics_machine.show(10)
    # metrics_jobs.show(10)
    # metrics_time.show(10)
    metrics_machine.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").save("./delta/metrics_machine")
    
    metrics_jobs.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").save("./delta/metrics_jobs")
    
    metrics_time.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").save("./delta/metrics_time")
            
def server_joins():
    server_usage_df = spark.read.format("delta").load("delta/server_usage")

    server_usage_avg = server_usage_df.select("machine_id","util_cpu","util_mem").groupBy("machine_id")\
        .agg(F.avg("util_cpu").alias("avg_util_cpu"), 
             F.avg("util_mem").alias("avg_util_mem")
            )#\
                # .orderBy(F.col("machine_id").asc()).show(3)
    
    server_event_df = spark.read.format("delta").load("delta/server_event")

    metrics_machine = spark.read.format("delta").load("delta/metrics_machine")
    
    joined_server_machine = metrics_machine.join(server_usage_avg, on = ["machine_id"], how = "inner")\
        .join(server_event_df, on = ["machine_id"], how = "inner")

    joined_server_machine.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").save("./delta/joined_server_machine")

    joined_server_machine.select("machine_id", "avg_util_cpu", "avg_util_mem", "cpu_capacity", "mem_capacity").orderBy(F.col("machine_id")).show(5)

# main
with open("config.json") as f:
    config = json.load(f)

stats = []

# commented out temporarily to not rerun the read/write of datasets
# for name, cfg in config["datasets"].items():
#     print(f"Loading {name}...")
#     load_data(name, cfg)
#     print(f"Finished: {name}...")

# check_units()
# removed temporarily to not rerun the write
# joined_inst_task = join_tables()
# joined_inst_task = joined_inst_task.withColumn("plan_cpu_norm", F.col("plan_cpu")/100)
    
# joined_inst_task.write.format("delta").mode("overwrite") \
#     .option("overwriteSchema", "true").save("./delta/joined_inst_task")
# print(joined_inst_task.columns)
# perform_calculations()

# perform_calculations_grouped()


# server_joins()

# own tests and observations
# joined_inst_task = spark.read.format("delta").load("delta/joined_inst_task")
# joined_inst_task.select("plan_cpu","real_cpu_avg").show(3)
# # total  = joined_inst_task.count()
# count = joined_inst_task.filter(F.col("plan_mem")>F.col("real_mem_avg")).count()
# print(f"all rows:  {total}")
# print(f"times plan_mem is larger than real_mem_avg: {count}")

# print(f"Same thing in percentage: {100*(count/total)}")