from task1_1 import task1_1


def explode(row):
    for k in row:
        yield k


def task1_2(rdd):
    _list = rdd.flatMap(explode).distinct().collect()
    _list = ["product " + product for product in _list]

    with open("../out/out_1_2a.txt", "w") as f:
        for product in _list:
            f.write(f"{product}\n")

    with open("../out/out_1_2b.txt", "w") as f:
        f.write(f"Count:\n{len(_list)}")


if __name__ == "__main__":
    rdd = task1_1()
    task1_2(rdd)
