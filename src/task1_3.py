from task1_1 import task1_1


def explode(row):
    for k in row:
        yield k


def task1_3(rdd):
    rdd = rdd.flatMap(explode).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)
    _list = rdd.takeOrdered(5, key=lambda x: -x[1])

    with open("../out/out_1_3.txt", "w") as f:
        for product in _list:
            f.write(f"{product}\n")

if __name__ == "__main__":
    rdd = task1_1()
    task1_3(rdd)


