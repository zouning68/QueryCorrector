from operator import add
from pyspark import SparkContext

def sort_t():
    sc = SparkContext(appName="testWC")
    data = sc.parallelize(["what do you do", "how do you do", "how do you do", "how are you"])
    result = data.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(add).sortBy(lambda x: x[1], False).take(3)
    for k, v in result:
        print(k, v)

if __name__ == '__main__':
    sort_t()