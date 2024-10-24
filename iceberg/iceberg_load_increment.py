from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from datetime import datetime
import logging


level = "2" #Какую таблицу тестируем, маленькую, среднюю или большую
your_bucket_name = "result" #Имя вашего бакета
your_access_key = "SKTW7WRLVJ020VTV2XEJ" #Ключ от вашего бакета
your_secret_key = "jJynvObHTG5AeS1nrYoIIVSh815iIaMAZZrjuo2m" #Ключ от вашего бакета

incr_bucket = f"s3a://source-data"
target_bucket = f"s3a://{your_bucket_name}"
incr_table_loc = f"{incr_bucket}/incr{level}"  # таблица с источника ODS , куда мы скопировали инкремент
init_table_loc = f"{target_bucket}/init{level}"  # реплика

iceberg_target_table = f"my_catalog.default.trg_ice_part_tab_{level}"
iceberg_temp_table = f"my_catalog.default.temp_iceberg_{level}_{datetime.strftime(datetime.now(), r'%y%m%d_%H%M%S')}"

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

logging.basicConfig(filename=f"/home/alpha/scripts/logs/log_join_v2__{datetime.strftime(datetime.now(), r'%y%m%d_%H%M%S')}.txt",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
log = logging.getLogger("App")

all_time = datetime.now()
log.info("Application start")

conf = SparkConf()
# conf.set('spark.scheduler.mode', 'FAIR')
conf.setAll(configs.items())
spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext

time_create_temp = datetime.now()
# Запись промежуточной Iceberg таблицы на основе инкрементальных parquet данных в S3
spark.read.parquet(incr_table_loc) \
    .writeTo(iceberg_temp_table).using("iceberg").tableProperty("format-version", "2") \
    .create()
log.info(f"Temp Iceberg table '{iceberg_temp_table}' created in {(datetime.now() - time_create_temp).seconds}s")

time_merge = datetime.now()
# Обновление записей в целевой таблице
# MATCHED -- запись в целевой таблице, которая закрылась в инкременте
# NOT MATCHED -- запись, которая только открылась после закрытия предыдущей версии или ещё не существовала в целевой
spark.sql(f"""
MERGE INTO {iceberg_target_table} tgt
USING {iceberg_temp_table} incr
    ON tgt.id = incr.id AND tgt.eff_from_dt = incr.eff_from_dt
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

log.info(f"Data merged in target table '{iceberg_target_table}' in {(datetime.now() - time_merge).seconds}s")

log.info(f'Process finished in {(datetime.now() - all_time).seconds}s')

# Удаление временной таблицы
log.info(f'Deleting temp table {iceberg_temp_table}')
drop_tmp_time = datetime.now()
spark.sql(f'DROP TABLE {iceberg_temp_table} PURGE')
log.info(f'Temp table deleted in {(datetime.now() - drop_tmp_time).seconds}s')

log.info(f'Application finished in {(datetime.now() - all_time).seconds}s')