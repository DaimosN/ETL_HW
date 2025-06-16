from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Создание Spark-сессии
spark = SparkSession.builder \
    .appName("create-table") \
    .enableHiveSupport() \
    .getOrCreate()


# Чтение файла из Object Storage
# Укажите правильный путь к вашему файлу и формат (CSV, JSON, Parquet и т.д.)
input_path = "s3a://tempbaket/2025/06/07/dc_universe_.csv"

df = spark.read.csv(input_path,
                    header=False,
                    inferSchema=True)

# Просмотр структуры данных (для отладки)
print("Схема данных:")
df.printSchema()
df = df.toDF("id", "PageID", "Name", 'Universe', 'URL', 'Identity', 'Gender', 'Marital', 'Teams', 'Weight', 'Creators')
print("Первые 5 строк:")
df.show(5)

grouped_df = df.groupBy("Teams").count()

# путь для сохранения
output_path = "s3a://tempbaket/grouped_by_teams_table"

grouped_df.write.mode("overwrite") \
    .option("path", output_path) \
    .saveAsTable("groupteams")

# 4. Проверка результата
print("Результат группировки:")
grouped_df.show()
