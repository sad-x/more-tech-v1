from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging

level = "2" #Какую таблицу тестируем, маленькую, среднюю или большую
your_bucket_name = "result" #Имя вашего бакета
your_access_key = "SKTW7WRLVJ020VTV2XEJ" #Ключ от вашего бакета
your_secret_key = "jJynvObHTG5AeS1nrYoIIVSh815iIaMAZZrjuo2m" #Ключ от вашего бакета

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
conf.set('spark.scheduler.mode', 'FAIR')
conf.setAll(configs.items())

spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext
# log = spark._jvm.org.apache.log4j.LogManager.getLogger(">>> App")

logging.basicConfig(filename=f"/home/alpha/scripts/logs/log_join_v2__{datetime.strftime(datetime.now(), r'%y%m%d_%H%M%S')}.txt",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
log = logging.getLogger("App")

incr_bucket = f"s3a://source-data"
your_bucket = f"s3a://{your_bucket_name}"
incr_table = f"{incr_bucket}/incr{level}"  # таблица с источника ODS , куда мы скопировали инкремент
init_table = f"{your_bucket}/init{level}"  # реплика
temp_table = f"{your_bucket}/temp{level}"

tgt0 = spark.read.parquet(init_table)
columns = tgt0.columns

src0 = spark.read.parquet(incr_table)
src = src0. \
    withColumn("eff_from_month", last_day(col("eff_from_dt")).cast(StringType())). \
    withColumn("eff_to_month", last_day(col("eff_to_dt")).cast(StringType())). \
    repartition("eff_to_month", "eff_from_month"). \
    selectExpr(columns)
src.cache()

log.info(f"SRC STARTED")
time_0 = datetime.now()

# Записываем инкремент и оставляем только нужные нам закрытые записи и минимальные даты
src.write.mode("overwrite").partitionBy("eff_to_month", "eff_from_month").parquet(temp_table)
log.info(f"SRC written, defining closed data")
src_closed = src.filter(col("eff_to_month") != lit("5999-12-31")).select("id", "eff_from_month", "eff_from_dt").cache()
src_closed.isEmpty()
src.unpersist()

log.info(f"SRC FINISHED")

def run_task(from_dt):
    log.info(f"Partition '{from_dt}' STARTED in thread {threading.current_thread()}")
    t0 = datetime.now()

    # Фильтруем до джойна, так как мы джойним ровно одну субпартицию
    tgt = tgt0.filter(col("eff_to_month") == lit("5999-12-31")).filter(col("eff_from_month") == from_dt)
    src_closed_single_part = src_closed.filter(col("eff_from_month") == from_dt)

    # Убираем все записи, по которым пришли апдейты
    tgt_no_match = tgt.join(src_closed_single_part, on=["id", "eff_from_dt"], how="left_anti")
    
    tmp_part = f'{temp_table}/eff_to_month=5999-12-31/eff_from_month={from_dt}'
    log.info(f'path to temp part is {tmp_part}')

    # Дозаписываем данные в таблицу
    # tgt_no_match.write.mode("append").partitionBy("eff_to_month", "eff_from_month").parquet(tmp_part)
    tgt_no_match.write.mode("append").parquet(tmp_part)
    
    log.info(f"Partition '{from_dt}' FINISHED in thread {threading.current_thread()} by {(datetime.now() - t0).seconds}s")

def run_multiple_jobs(part_list: [str]):
    # q = Queue()
    log.info('START Threads')
    # for _ in range(4):
    #     t = threading.Thread(target = run_task)
    #     t.start()

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for part in part_list:
            futures.append(executor.submit(run_task, from_dt=part))
        for future in as_completed(futures):
            future.result()
        # executor.map(run_task, part_list)
    log.info('END Threads')

# Получаем список субпартиций в 5999
rows = tgt0.filter(col("eff_to_month") == lit("5999-12-31")).select(col("eff_from_month").cast(StringType())).distinct().orderBy("eff_from_month").collect()
partitions = [str(row[0]) for row in rows]

log.info(f'There are {len(partitions)} partitions: {partitions}')

part_t0 = datetime.now()
run_multiple_jobs(partitions)
log.info(f'\nFinished tmp partition insert in {(datetime.now() - part_t0).seconds}s\n')

# # Получаем список субпартиций в 5999
# rows = tgt0.filter(col("eff_to_month") == lit("5999-12-31")).select(col("eff_from_month").cast(StringType())).distinct().orderBy("eff_from_month").collect()
# partitions = [str(row[0]) for row in rows]

# # Обрабатываем каждую партицию отдельно
# for from_dt in partitions:
#     log.info(f"{from_dt} STARTED")
    
#     # Фильтруем до джойна, так как мы джойним ровно одну субпартицию
#     tgt = tgt0.filter(col("eff_to_month") == lit("5999-12-31")).filter(col("eff_from_month") == from_dt)
#     src_closed_single_part = src_closed.filter(col("eff_from_month") == from_dt)

#     # Убираем все записи, по которым пришли апдейты
#     tgt_no_match = tgt.join(src_closed_single_part, on=["id", "eff_from_dt"], how="left_anti")
  
#     # Дозаписываем данные в таблицу
#     tgt_no_match.write.mode("append").partitionBy("eff_to_month", "eff_from_month").parquet(temp_table)
    
#     log.info(f"{from_dt} FINISHED")

log.info("Moving temp data")

# Теперь надо перенести данные из темповой в основную таблицу
# Закрытые партиции мы переносим просто так, а 5999-12-31 надо перетереть

init_insert_t0 = datetime.now()
hadoop_conf = sc._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(your_bucket), hadoop_conf)
path = spark._jvm.org.apache.hadoop.fs.Path(f"{init_table}/eff_to_month=5999-12-31/")
fs.delete(path, True)
log.info(f'deleted open records in target finished by {(datetime.now() - init_insert_t0).seconds}s')

tgt_insert_t0 = datetime.now()
spark.read.parquet(temp_table).write.mode("append").partitionBy("eff_to_month", "eff_from_month").parquet(init_table)
log.info(f'insert in target finished by {(datetime.now() - tgt_insert_t0).seconds}s')

path = spark._jvm.org.apache.hadoop.fs.Path(temp_table)
fs.delete(path, True)

log.info("Finished")

log.info(f'QWERTY Process finished in {(datetime.now() - time_0).seconds}s')