CREATE KEYSPACE IF NOT EXISTS cloud_waste
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE cloud_waste;

CREATE TABLE IF NOT EXISTS waste_by_machine (
  machine_id text PRIMARY KEY,
  total_cpu_waste double,
  total_mem_waste double,
  avg_util_cpu double,
  avg_util_mem double,
  cpu_capacity bigint,
  mem_capacity bigint
);

CREATE TABLE IF NOT EXISTS waste_by_job (
  job_id text PRIMARY KEY,
  total_cpu_waste double,
  total_mem_waste double
);

CREATE TABLE IF NOT EXISTS waste_by_hour (
  day bigint,
  hour bigint,
  total_cpu_waste double,
  total_mem_waste double,
  PRIMARY KEY (day, hour)
) WITH CLUSTERING ORDER BY (hour ASC);