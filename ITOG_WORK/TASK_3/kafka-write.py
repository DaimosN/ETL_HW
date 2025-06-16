from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import to_json, col, struct


def main():
    # spark = SparkSession.builder.appName("dataproc-kafka-write-app").getOrCreate()

    # Инициализация SparkSession с поддержкой S3
    spark = SparkSession.builder \
        .appName("csv-to-kafka") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.endpoint", "storage.yandexcloud.net") \
        .getOrCreate()

    # Чтение файла из Object Storage
    # Укажите правильный путь к вашему файлу и формат (CSV, JSON, Parquet и т.д.)
    input_path = "s3a://tempbaket/2025/06/07/dc_universe_.csv"

    df = spark.read \
        .option("header", "false") \
        .csv(input_path) \
        .toDF("id", "PageID", "Name", 'Universe', 'URL', 'Identity', 'Gender', 'Marital', 'Teams', 'Weight',
              'Creators')  # Замените на ваши названия

    # df = spark.createDataFrame([
    #     Row(msg="hello world"),
    #     Row(msg="hello test 2 from kafka"),
    #     Row(msg="hello some msg")
    # ])

    df = df.select(to_json(struct([col(c).alias(c) for c in df.columns])).alias('value'))
    df.write.format("kafka") \
        .option("kafka.bootstrap.servers", "rc1a-vks47mn2qt3ekonl.mdb.yandexcloud.net:9091") \
        .option("topic", "dataproc-kafka-topic") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=user1 "
                "password=password1 "
                ";") \
        .save()


if __name__ == "__main__":
    main()
