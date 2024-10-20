# MoreTech SCD2 Loader

## Запуск

level - вариант тестируемых данных:
- 2 - маленький набор данных (10млн-1млн)
- 3 - средний набор данных (100млн-10млн)
- 4 - большой набор данных (1млрд-100млн)

1. Инициализация данных

```commandline
/opt/spark/bin/spark-submit --jars ~/spark-hadoop-cloud_2.13-3.5.3.jar copy_init.py <level>
```

2. Запуск применения инкремента

```commandline
/opt/spark/bin/spark-submit --jars ~/spark-hadoop-cloud_2.13-3.5.3.jar join.py <level>
```

3. Проверка тестируемых данных

```commandline
/opt/spark/bin/spark-submit --jars ~/spark-hadoop-cloud_2.13-3.5.3.jar check_result.py <level>
```