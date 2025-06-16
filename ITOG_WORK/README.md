https://www.kaggle.com/datasets/mohitbansal31s/dc-characters/data - ссылка на датасет

Команда для загрузки датасета в ydb

ydb --endpoint grpcs://{your_serverless} --database /{your_database} --sa-key-file "{path_your_key}" import file csv --path dc_universe_ --columns "id,PageID,Name,Universe,URL,Identity,Gender,Marital,Teams,Weight,Creators" --delimiter "," --skip-rows 1 --null-value "" --progress dc_characters_dataset_new2.csv


Задание 1. Работа с Yandex DataTransfer
- В ручном режиме создата таблица в YDB
- С помощью bash команды (указана выше) датасет загружен в созданную таблицу
- Создан трансфер данных с источником в YDB и приемником в Object Storage

Задание 2. Автоматизация работы с Yandex Data Processing при помощи Apache AirFlow

Создан DAG DATA_INGEST, который:
  Создает Data Proc кластер.
  Запускает на кластере PySpark-задание для обработки CSV-файла.
  После завершения работы задания удаляет кластер.
  Внутри скрипта-задания происходит группировка данных и сохранение данных
  Результат сохраняется в формате csv:

Задание 3. Работа с топиками Apache Kafka® с помощью PySpark-заданий в Yandex Data Processing
- Создан кластер Data Proc, подняты Managed service for Kafka и Managed service for PostgreSQL.
- Скрипт kafka-write.py
    Скрипт читает csv файл и отправляет в топик Kafka
- Скрипт kafka-read-stream.py
    Скрипт читает данные из топика и сохраняет в Object Storage

Задание 4. Визуализация в Yandex DataLens
Реализовано отображение следующих диаграмм
- диаграмма гендеров
- диаграмма команд
- диаграмма отношений
- диаграмма персонажей во вселенных
