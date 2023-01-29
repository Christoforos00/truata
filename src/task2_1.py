from pathlib import Path

from pyspark.sql import SparkSession


def task2_1():
    spark = SparkSession.builder.appName("app").getOrCreate()
    df = spark.read.parquet("sf-airbnb-clean.parquet")
    Path("../out").mkdir(parents=True, exist_ok=True)
    return df


if __name__ == "__main__":
    df = task2_1()
    print(df.take(5))
