from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

import logging
from datetime import datetime

import argparse

def get_level():
    parser = argparse.ArgumentParser(description = 'join script')
    parser.add_argument("-l", dest="level", default=2, type=int)
    args = parser.parse_args()
    return args.level


level = get_level()
your_bucket_name = "result" #Имя вашего бакета
your_access_key = "SKTW7WRLVJ020VTV2XEJ" #Ключ от вашего бакета
your_secret_key = "jJynvObHTG5AeS1nrYoIIVSh815iIaMAZZrjuo2m" #Ключ от вашего бакета

logging.basicConfig(filename=f"/home/alpha/scripts/logs/log_result__{datetime.strftime(datetime.now(), r'%y%m%d_%H%M%S')}.txt",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
log = logging.getLogger("App")

configs = {
    "spark.sql.files.maxPartitionBytes": "1073741824", #1GB
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
    "spark.hadoop.fs.s3a.fast.upload": "true",
    "spark.hadoop.fs.s3a.block.size": "134217728", # 128MB
    "spark.hadoop.fs.s3a.multipart.size": "268435456", # 256MB
    "spark.hadoop.fs.s3a.multipart.threshold": "536870912", # 512MB
    "spark.hadoop.fs.s3a.committer.name": "magic",
    "spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled": "true",
    "spark.hadoop.fs.s3a.threads.max": "64",
    "spark.hadoop.fs.s3a.connection.maximum": "64",
    "spark.hadoop.fs.s3a.fast.upload.buffer": "array",
    "spark.hadoop.fs.s3a.directory.marker.retention": "keep",
    "spark.hadoop.fs.s3a.endpoint": "api.s3.az1.t1.cloud",
    "spark.hadoop.fs.s3a.bucket.source-data.access.key": "P2EGND58XBW5ASXMYLLK",
    "spark.hadoop.fs.s3a.bucket.source-data.secret.key": "IDkOoR8KKmCuXc9eLAnBFYDLLuJ3NcCAkGFghCJI",
    f"spark.hadoop.fs.s3a.bucket.{your_bucket_name}.access.key": your_access_key,
    f"spark.hadoop.fs.s3a.bucket.{your_bucket_name}.secret.key": your_secret_key,
    "spark.sql.parquet.compression.codec": "zstd",
    "spark.sql.catalog.my_catalog": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.my_catalog.warehouse": "s3a://result/iceberg",
    "spark.sql.catalog.my_catalog.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
    "spark.sql.catalog.my_catalog.uri": "jdbc:postgresql://127.0.0.1:5432/postgres",
    "spark.sql.catalog.my_catalog.jdbc.user": "postgres",
    "spark.sql.catalogImplementation": "in-memory"
}
conf = SparkConf()
conf.setAll(configs.items())

spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext

source_bucket = f"s3a://source-data"
target_bucket = f"s3a://{your_bucket_name}"
incr_table = f"{source_bucket}/incr{level}"
source_table = f"{source_bucket}/init{level}"
target_table = f"my_catalog.default.trg_ice_part_tab_{level}"

oldLines = spark.read.parquet(source_table).count()
newLines = spark.read.parquet(incr_table).where(col("id") > oldLines).count()
closedLines = spark.read.parquet(incr_table).where(col("eff_to_dt") != "5999-12-31").count()

newLinesT = spark.table(target_table).where(col("eff_to_dt") == "5999-12-31").count()
closedLinesT = spark.table(target_table).where(col("eff_to_dt") != "5999-12-31").count()

if (oldLines + newLines) == newLinesT:
    log.info(f"Open records match: {newLinesT}")
else:
    log.info(f"ERROR: Expected open records: {oldLines + newLines}, actual: {newLinesT}")

if closedLines == closedLinesT:
    log.info(f"Closed records match: {closedLinesT}")
else:
    log.info(f"ERROR: Expected closed records: {closedLines}, actual: {closedLinesT}")
