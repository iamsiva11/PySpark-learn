#
from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext

if __name__ == "__main__":

    sc = SparkContext(appName="HelloPy")
    nums = sc.parallelize([1, 2, 3, 4, 5])
    nums.collect()
    bytwo = nums.map(lambda x: x + 2)
    bytwo.collect()
    print "hello world"

    sc.stop()