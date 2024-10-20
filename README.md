# MoreTech SCD2 Loader

## Проблематика

В озере данных ВТБ необходимо хранить полную историю изменений из оперативного хранилища данных, поступающую в формате SCD2.
При обработке новых сущностей записи добавляются в реплику. Но при обновлении существующих сущностей требуется изменить дату окончания действия у предыдущей записи по идентификатору сущности.
Выбранная архитектура озера данных не поддерживает операции обновления. Возникает проблема эффективной обработки инкрементальных данных и корректного обновления реплики в приемлемые для пользователей сроки.

## Решение

- Движок для обработки данных: Apache Spark.
- Формат хранения данных: Apache Parquet.

Алгоритм:
1. Добавление полей eff_from_month и eff_to_month: последние дни месяца даты начала и окончания действия записи. 
2. Партицирование реплики и инкремента по добавленным полям.
3. Запись всех закрытых записей из инкремента в реплику.
4. Поиск открытых записей в реплике, которые не требуют обновления, с последующим добавлением их в инкремент. Реализовано через anti join реплики и инкремента. Обработка происходит в цикле для каждой субпартиции eff_from_month в реплике для партиции eff_to_month=5999-12-31.
Поиск открытых закрытых записей в инкременте и запись в реплику. 
5. Пункты 3 и 4 выполняются параллельно в нескольких потоках.


## Запуск

level - вариант тестируемых данных:
- 2 - маленький набор данных (10млн-1млн)
- 3 - средний набор данных (100млн-10млн)
- 4 - большой набор данных (1млрд-100млн)

1. Инициализация данных

```commandline
/opt/spark/bin/spark-submit --jars ~/spark-hadoop-cloud_2.13-3.5.3.jar src/copy_init.py <level>
```

2. Запуск применения инкремента

```commandline
/opt/spark/bin/spark-submit --jars ~/spark-hadoop-cloud_2.13-3.5.3.jar src/join.py <level>
```

3. Проверка тестируемых данных

```commandline
/opt/spark/bin/spark-submit --jars ~/spark-hadoop-cloud_2.13-3.5.3.jar src/check_result.py <level>
```