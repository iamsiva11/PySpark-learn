import re
import sys

from pyspark import SparkContext

#function to extract the movie rating data from the input, based on position
def extractMovieRatingData(line):
    val = line.strip()
    (userid, movieid, rating) = val.split("|")
    return (movieid, rating)

#function to extract the movie data (not the rating) from the input, based on position
def extractMovie(line):
    val = line.strip()
    (id, name, year) = val.split("|")
    return (id, name)

#Create Spark Context with the master details and the application name
# sc = SparkContext("spark://bigdata-vm:7077", "max_temperature")
sc = SparkContext(appName="Python-Movie-Analysis")

#Create an RDD from the input data in HDFS
# movie = sc.textFile("hdfs://localhost:9000/user/bigdatavm/input/movie/movie.txt")
# movieRatings = sc.textFile("hdfs://localhost:9000/user/bigdatavm/input/movierating/movierating.txt")
# movie = sc.textFile("datasets-pyspark/movie.txt")
# movieRatings = sc.textFile("datasets-pyspark/movierating.txt")
movie = sc.textFile(sys.argv[1], 1)
movieRatings = sc.textFile(sys.argv[2], 1)

#Group by movie id and then aggregate (sum) the movie ratings
movieRatingsSortedAggregated = movieRatings.map(extractMovieRatingData).reduceByKey(lambda a, b: int(a)+int(b))

#Join the aggregated movie ratings and the movie data and finally get the top 3 rated movies by movie name
movieSortedTop3List = movie.map(extractMovie).join(movieRatingsSortedAggregated).map(lambda a: a[1]).map(lambda a: (a[1],a[0])).sortByKey(ascending=False).take(3)

#take() returns a list, which has to be converted into an RDD using parallelize() and then consolidated into a single file using coalesce()
#coalesce() is OK for small data sets, but might be a hit for bigger data sets
movieSortedTop3RDD = sc.parallelize(movieSortedTop3List).map(lambda a: a[1]).coalesce(1)

#dump the top 3 movies into HDFS
movieSortedTop3RDD.saveAsTextFile("datasets-pyspark/output/op.txt")