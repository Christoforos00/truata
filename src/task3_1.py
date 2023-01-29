import os


def task3_1():
    os.system('curl -L "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data" -o /tmp/iris.csv')


if __name__ == "__main__":
    task3_1()
