from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.task2_1 import task2_1

spark = SparkSession.builder.appName("app").getOrCreate()

df = spark.read.parquet("sf-airbnb-clean.parquet")

def task2_2(df):

    min_price = int(df.select(F.min(df.price)).collect()[0][0])
    max_price = int(df.select(F.max(df.price)).collect()[0][0])
    rows = df.count()

    with open("../out/out_2_2.txt", "w") as f:
        text = f"min_price, max_price, row_count\n{min_price},{max_price},{rows}"
        f.write(text)


if __name__ == "__main__":
    df = task2_1()
    task2_2(df)
