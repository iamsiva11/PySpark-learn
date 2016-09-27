#
from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext

if __name__ == "__main__":

    sc = SparkContext(appName="HelloPy")

	lines = sc.textFile("datasets/testbigram.txt")
	lines.collect()

	bigrams = lines.map(lambda s : s.split(" ")).flatMap(lambda s: [((s[i],s[i+1]),1) for i in range (0, len(s)-1)])
	bigrams.collect()
	counts = bigrams.reduceByKey(lambda x, y : x+y)
	counts.collect()
	sc.stop()
