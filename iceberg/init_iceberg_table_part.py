from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

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
your_bucket_name = "result"  # Имя вашего бакета
your_access_key = "SKTW7WRLVJ020VTV2XEJ"  # Ключ от вашего бакета
your_secret_key = "jJynvObHTG5AeS1nrYoIIVSh815iIaMAZZrjuo2m"  # Ключ от вашего бакета

# Получение конфигурации из файла
cp = configparser.ConfigParser()
config_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'conf.ini')
cp.read(config_file_path)

# Установка конфигурации спарка
sparkConfigs = dict(cp['Spark'])
conf = SparkConf()
conf.setAll(sparkConfigs.items())

# Получение сессии спарка
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Конфигурация логирования
logging.basicConfig(filename=f"/home/alpha/scripts/logs/log_init_level{level}__{datetime.strftime(datetime.now(), r'%y%m%d_%H%M%S')}.txt",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
log = logging.getLogger("App")

log.info(f"Started create iceberg target table level={level}")

# Задание имен необходимых в работе объектов
src_bucket = f"s3a://source-data"
table_to_copy = f"init{level}"
src_init_table = f"{src_bucket}/{table_to_copy}"
trg_iceberg_tab = f"my_catalog.default.trg_ice_part_tab_{level}"

# Чтение данных реплики и удаление полей партицирования
src_df = spark.read.parquet(src_init_table).drop("eff_to_month", "eff_from_month")

# Запись в Iceberg по выражению партицирования
src_df.writeTo(f"{trg_iceberg_tab}") \
    .tableProperty("write.format.default", "orc") \
    .tableProperty("write.parquet.compression-codec", "zlib") \
    .partitionedBy(months("eff_to_dt")) \
    .createOrReplace()


log.info(f"Finished create iceberg target table level={level}")