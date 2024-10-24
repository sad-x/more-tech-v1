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
your_bucket_name = "result" #Имя вашего бакета
your_access_key = "SKTW7WRLVJ020VTV2XEJ" #Ключ от вашего бакета
your_secret_key = "jJynvObHTG5AeS1nrYoIIVSh815iIaMAZZrjuo2m" #Ключ от вашего бакета

# Получение конфигурации из файла
cp = configparser.ConfigParser()
config_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'conf.ini')
cp.read(config_file_path)

# Установка конфигурации спарка
sparkConfigs = dict(cp['Spark'])
conf = SparkConf()
conf.setAll(sparkConfigs.items())

# Задание параметров для Iceberg
icebergConfigs = dict(cp['Iceberg'])
catalog = icebergConfigs.get('catalog')
schema = icebergConfigs.get('schema')

# Получение сессии и контектса спарка
spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext

# Задание имён необходимых в работе объектов
incr_bucket = f"s3a://source-data"
target_bucket = f"s3a://{your_bucket_name}"
incr_table_loc = f"{incr_bucket}/incr{level}"  # таблица с источника ODS , куда мы скопировали инкремент
init_table_loc = f"{target_bucket}/init{level}"  # реплика

iceberg_target_table = f"{catalog}.{schema}.trg_ice_part_tab_{level}"
iceberg_temp_table = f"{catalog}.{schema}.temp_iceberg_{level}_{datetime.strftime(datetime.now(), r'%y%m%d_%H%M%S')}"


# Конфигурация логирования
logging.basicConfig(filename=f"/home/alpha/scripts/logs/log_join_level{level}__{datetime.strftime(datetime.now(), r'%y%m%d_%H%M%S')}.txt",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
log = logging.getLogger("App")

all_time = datetime.now()
log.info("Application start")

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