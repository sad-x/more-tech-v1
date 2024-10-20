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
your_bucket_name = "<BUCKET_NAME>" # Имя вашего бакета
your_access_key = "<ACCESS_KEY>" # Ключ от вашего бакета
your_secret_key = "<SECRET_KEY>" # Ключ от вашего бакета

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
    "spark.sql.parquet.compression.codec": "zstd"
}
conf = SparkConf()
conf.setAll(configs.items())

spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext

incr_bucket = f"s3a://source-data"
your_bucket = f"s3a://{your_bucket_name}"
incr_table = f"{incr_bucket}/incr{level}"
init_table = f"{incr_bucket}/init{level}"  
your_table = f"{your_bucket}/init{level}"

oldLines = spark.read.parquet(init_table).count()
newLines = spark.read.parquet(incr_table).where(col("id") > oldLines).count()
closedLines = spark.read.parquet(incr_table).where(col("eff_to_dt") != "5999-12-31").count()

newLinesT = spark.read.parquet(your_table).where(col("eff_to_month") == "5999-12-31").count()
closedLinesT = spark.read.parquet(your_table).where(col("eff_to_month") != "5999-12-31").count()

if (oldLines + newLines) == newLinesT:
    log.info(f"Open records match: {newLinesT}")
else:
    log.info(f"ERROR: Expected open records: {oldLines + newLines}, actual: {newLinesT}")

if closedLines == closedLinesT:
    log.info(f"Closed records match: {closedLinesT}")
else:
    log.info(f"ERROR: Expected closed records: {closedLines}, actual: {closedLinesT}")
