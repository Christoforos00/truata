from pyspark import SparkContext


def task1_1():
    sc = SparkContext().getOrCreate()
    rdd = sc.textFile("groceries.csv").map(lambda x: x.split(","))
    return rdd


if __name__ == "__main__":
    rdd = task1_1()
    print(rdd.take(5))
