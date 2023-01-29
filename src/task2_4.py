from pyspark.sql import functions as F

from src.task2_1 import task2_1


def task2_4(df):
    min_price = df.select(F.min(df.price)).collect()[0][0]
    df = df.filter(df.price == min_price)

    max_review = df.select(F.max(df.review_scores_rating)).collect()[0][0]
    df = df.filter(df.review_scores_rating == max_review)

    people = df.select("accommodates").collect()[0][0]

    with open("../out/out_2_4.txt", "w") as f:
        f.write(f"{people}")


if __name__ == "__main__":
    df = task2_1()
    task2_4(df)
