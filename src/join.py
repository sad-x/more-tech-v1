from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import logging
import argparse
import configparser
import os

# Функция получения объема данных из аргументов командной строки
def get_level():
    parser = argparse.ArgumentParser(description = 'join script')
    parser.add_argument("-l", dest="level", default=2, type=int)
    args = parser.parse_args()
    return args.level


level = get_level()
your_bucket_name = "<BUCKET_NAME>" # Имя вашего бакета
your_access_key = "<ACCESS_KEY>" # Ключ от вашего бакета
your_secret_key = "<SECRET_KEY>" # Ключ от вашего бакета

cp = configparser.ConfigParser()
config_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'conf.ini')
cp.read(config_file_path)

# Установка конфигурации спарка
sparkConfigs = dict(cp['Spark'])
conf = SparkConf()
conf.setAll(sparkConfigs.items())

# Задание временной метки, символизирующую, что запись открыта и активна
globalConfigs = dict(cp['Global'])
open_dt = globalConfigs.get('open_dt', "5999-12-31")

spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext

logging.basicConfig(filename=f"/home/alpha/scripts/logs/log_join_level{level}__{datetime.strftime(datetime.now(), r'%y%m%d_%H%M%S')}.txt",
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

log.info(f"Started copying increment to temp table")
time_0 = datetime.now()

# Записываем инкремент и оставляем только нужные нам закрытые записи и минимальные даты
src.write.mode("overwrite").partitionBy("eff_to_month", "eff_from_month").parquet(temp_table)
log.info(f"SRC written, defining closed data")
src_closed = src.filter(col("eff_to_month") != lit(open_dt)).select("id", "eff_from_month", "eff_from_dt").cache()
src.unpersist()

log.info(f"Finished increment copy by {(datetime.now() - time_0).seconds}s")

def run_task(from_dt):
    log.info(f"Partition '{from_dt}' STARTED in thread {threading.current_thread()}")
    t0 = datetime.now()

    # Фильтруем до джойна, так как мы джойним ровно одну субпартицию
    tgt = tgt0.filter(col("eff_to_month") == lit(open_dt)).filter(col("eff_from_month") == from_dt)
    src_closed_single_part = src_closed.filter(col("eff_from_month") == from_dt)

    # Убираем все записи, по которым пришли апдейты
    tgt_no_match = tgt.join(src_closed_single_part, on=["id", "eff_from_dt"], how="left_anti")
    
    # Формируем путь к субпартиции временной таблицы в виде литерала для прямой вставки
    tmp_part = f'{temp_table}/eff_to_month={open_dt}/eff_from_month={from_dt}'
    log.info(f'path to temp part is {tmp_part}')

    # Дозаписываем данные в соответствующую субпартицию
    tgt_no_match.write.mode("append").parquet(tmp_part)
    
    log.info(f"Partition '{from_dt}' FINISHED in thread {threading.current_thread()} by {(datetime.now() - t0).seconds}s")

def run_multiple_jobs(part_list: [str]):
    log.info('START Threads')

    # Запускаем в нескольких потоках для улучшения производительности. 
    with ThreadPoolExecutor(max_workers = globalConfigs.get('max_threads', 4)) as executor:
        futures = []
        for part in part_list:
            futures.append(executor.submit(run_task, from_dt=part))
        for future in as_completed(futures):
            future.result()
    log.info('END Threads')

# Получаем список субпартиций в 5999
rows = tgt0.filter(col("eff_to_month") == lit(open_dt)).select(col("eff_from_month").cast(StringType())).distinct().orderBy("eff_from_month").collect()
partitions = [str(row[0]) for row in rows]

log.info(f'There are {len(partitions)} partitions: {partitions}')

part_t0 = datetime.now()
run_multiple_jobs(partitions)
log.info(f'Finished tmp partition insert in {(datetime.now() - part_t0).seconds}s\n')

# Освобождаем память после использования
src_closed.unpersist()

log.info("Moving temp data")

# Переносим данные из временной таблицы в основную. 
# Закрытые партиции мы переносим просто так, а 5999-12-31 надо перетереть и залить из временной
# Для данного решения считается, что "долёты" в закрытых партициях не являются нормальным поведением 

init_insert_t0 = datetime.now()
hadoop_conf = sc._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(your_bucket), hadoop_conf)
path = spark._jvm.org.apache.hadoop.fs.Path(f"{init_table}/eff_to_month={open_dt}/")
fs.delete(path, True)
log.info(f'deleted open records in target finished by {(datetime.now() - init_insert_t0).seconds}s')

tgt_insert_t0 = datetime.now()
spark.read.parquet(temp_table).write.mode("append").partitionBy("eff_to_month", "eff_from_month").parquet(init_table)
log.info(f'insert in target finished by {(datetime.now() - tgt_insert_t0).seconds}s')

path = spark._jvm.org.apache.hadoop.fs.Path(temp_table)
fs.delete(path, True)

log.info("Finished")

log.info(f'Process finished in {(datetime.now() - time_0).seconds}s')