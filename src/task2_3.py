from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.task2_1 import task2_1


# assignment said review_scores_rating == 10, but it doesn't exist
def task2_3(df):
    df = df.filter((df.price > 5000) &
                   (df.review_scores_rating == 100))

    avg_bedrooms = df.select(F.avg(df.bedrooms)).collect()[0][0]
    avg_bathrooms = df.select(F.avg(df.bathrooms)).collect()[0][0]

    with open("../out/out_2_3.txt", "w") as f:
        text = f"avg_bathrooms, avg_bedrooms\n{avg_bathrooms},{avg_bedrooms}"
        f.write(text)


if __name__ == "__main__":
    df = task2_1()
    task2_3(df)
