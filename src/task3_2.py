from pathlib import Path

from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression


def task3_2():
    spark = SparkSession.builder.appName("app").getOrCreate()
    # training
    columns = ["sepal_length", "sepal_width", "petal_length", "petal_width", "class"]
    df = spark.read.csv("/tmp/iris.csv", header=False, inferSchema=True).toDF(*columns)
    print(df.show(5))

    assembler = VectorAssembler(inputCols=["sepal_length", "sepal_width", "petal_length", "petal_width"],
                                outputCol="features")
    output_df = assembler.transform(df)
    train_df = output_df.select("features", "class")

    indexer = StringIndexer(inputCol="class", outputCol="class_id")
    train_df_indexed = indexer.fit(train_df).transform(train_df)
    train_df_indexed = train_df_indexed.drop("class")

    lr = LogisticRegression(labelCol="class_id", family="multinomial")
    lrn = lr.fit(train_df_indexed)

    # predictions
    pred_data = spark.createDataFrame(
        [(5.1, 3.5, 1.4, 0.2),
         (6.2, 3.4, 5.4, 2.3)],
        ["sepal_length", "sepal_width", "petal_length", "petal_width"])

    pred_df = assembler.transform(pred_data)
    pred_df = pred_df.select("features")
    examples = pred_df.collect()
    preds0 = lrn.predict(examples[0][0])
    preds1 = lrn.predict(examples[1][0])

    Path("../out").mkdir(parents=True, exist_ok=True)
    with open("../out/out_3_2.txt", "w") as f:
        f.write(f"class\n{preds0}\n{preds1}")


if __name__ == "__main__":
    task3_2()
