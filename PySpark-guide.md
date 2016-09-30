#Pyspark programming Guide


###Pyspark first Program


```python
from itertools import cycle
from  operator import add

n = zip(cycle(["even”,”odd”]), range(20))
n[:3]

numbers= sc.parallelise(n)
increased = numbers.mapValues(lambda x :x+1)
grouped = increased.reduceByKey(add)
grouped.collect()
```


###RDD Basics

* All data is stored in RDD
* Fault tolerant.i.e, Can  recreate the data if lost.


```python

#To create - by calling the Parallelise Command

doubled = rdd.map(lambda x:x*2)
doubled.toDebugSting()
```

###Reading a Text File


```python
text = sc.textfile("<textfile>”, minPartitions=20)
text.collect()
text = sc.textfile("<textfile>")

<unicode and utf in text processing?>

#Min partition = minimum no of partitions that it can have
```



###Transformations

* Map
* Filter
* Flatmap
* MapPartitions
* Sample
* Union
* Intersection
* Distinct
* Cartesian
* Pipe
* Coalesce
* Repartition
* RepartitionAndSortWithinPartitions



#### Map

Mapping a function over a list.
Start with a list and call map with a function
old list -> new list

```python
numbers= sc.parallelize(xrange(10))
numbers.map(lambda x: x*10).collect()
```

Instead of lambda, it could also be done by writing a basic function and returning a value, like the below

```python
def times_ten(x):
return x*10

numbers.map(times_ten).collect()
#Note: Should write a pure function. Shouldn’t reference outside the function
```

Since lambda expressions are crisp, terse and readable , we make use of them

>When to use map?
When you need to transform a bunch of(or list) input -> output list


#### Filter

Like in sql, filter is like a WHERE clause

Calls it on each RDD
Spark creates a new RDD from the original rdd


```python
numbers = sc.parallelize(xrange(10))

def is_even(x):
return (x%2)==0
numbers.filter(is_even).collect()

#or in a lambda
numbers.filter(lambda x: (x%2)==0 ).collect()
```

Note:


```python
#Difference between true and truthy (reg boolean)

bool(0) #is false , rule that any no other than 0 is true
bool("”) is false
bool({}) if false
bool("false”) #is true , since empty strings are only false, which can be pretty much confusing in python

>Better to always return in bool to be safe
>If confident enough about py rules, can make use of them
```


#### Flatmap

When you want to map a source rdd to a (list of rdds)
Different from a map (one to one) , while flatmap is one to many

```python
text = sc.textFile(" text file")
text.collect()

words=  text.flatmap(lambda x: x.split(" "))
```

Output will be a giant text file split by spaces as a whole not by line by line, inturn removing one level of grouping

[ list of all words from the file ]

But if you want to achieve such in a line by line fashion we should use map()

[first sentence list of words]
[2nd sentence list of words]
.
.
.


```python
#to count the words:
words.counts()
```

>When to use flat map:
* To choose from going from 0 and many outputs for each input you should reach for flat map

* Flatmap can’t keep track of the origin for every iterations., you can device one such with using a map()



#### MapPartitions

Works with elements within an RDD.
Works on partitions in the RDD(one of the chunks).


```python
text= sc.textFile(file, minPartitions=5)
words = text.flatMap(lambda x:x.split(" "))

def count_words(iterator):
counts={}
for w in iterator:
if w in counts:
counts[w]+=1
else:
counts[w] = 1
yield counts

words_counts = words.mapPartition(count_words)

word_counts.collect()
```


#### Sample

For making bigdatsets smaller, making quick estimates.

Useful for working in the shell to make quick interactive programming and push to production for the large data to a non-interactive spark job.


```python
data = sc.parallize(range(10000))

data.count()

data.sample(False, 0.1).count()

#replacement argument
#sample with replacement  - true
#sample without replacement  false - will never get a double (imagine bag of balls #scenario)
#Seed for the random generation of data for the sample, so everyone gets the same #data in uniform
```


#### Union

when you have data coming from multiple streams , but you want to collate them all together

rdd1 = sc.parallelize(xrange(5,14))
rdd2 = sc.parallelize(xrange(10))

rdd1.union(rdd2).collect()


#### Intersection


See What unique  elements are present in both of your RDDs

```python
rdd1 = sc.parallilize([1,1,2,4,5])

rdd2 = sc.parallize([1,2,5,8,6])

rdd1.intersection(rdd2)
```


>Note:
>We don’t see duplicate outputs.
>Internally spark uses reduce to perform this job
>data travels from one executor to other determined by the partition
>can be slow for large data to move data between data executors
>But should be worth trying out. Most likely if the jobs slow, it might be due to intersection


#### Distinct


```python
rdd= sc.parallilize(["a”,”b”]).cartesian(sc.parallelize(range(100)))
rdd.collect()
first = rdd.map(lambda x: x[0])
first.collect()
first.distinct().collect()
```

>Note:
* Distinct takes an optional argument - numpartitions
* Higher you set - more parallel operation
* Uses reduce underneath  - might be slower than non-reduce transform in bigdata and slow cost networks


#### Cartesian

When we want an all possible combo

```python
#In Pure python,
[(a,b) for a in  ice creams for b in cookies]
```


```python
ice_creams = range(5)
cookies= range(7)

ice_creams= sc.parallilize(range(5))
cookies= sc.parallilize(range(7))
combinations = ice_creams.cartesian(cookies)
combination.collect()
```

#resulting car o/p= 100*200 = 20000



#### Pipe


Any command line line tool on a partition worth of data should be a good fit for using pipe


```python
numbers =  sc.parallelize(xrange(11))
numbers.pipe("grep 1”).collect()
rdd= sc.parallelize(["b,b”, "c,c,c”, "a"])
rdd.pipe("tr ‘[a-z]’ ‘[A-Z]’  ").collect()
rdd.pipe("grep a”).collect()
rdd.pipe("tr -s ',' '[\n*]' ").collect()
```


#### Coalesce

Reduce the no of partitions , try to combine them on the same executors
to ensure minimum data movement between executors.


Recommended -> 2-4 partitions in a cluster.

```python
rdd = sc.parallelize(xrange(10000), numSlices=100)
rdd2 = rdd.coalesce(10)
```




#### Repartition


* A good partitioner should put the same amount of data on each node.
long tail performance issue

* Similar to coalesce but you can grow the partitions
Will increase the data traffic while in the data movement

* Picking the right no of partitions can be tricky

````python
rdd = sc.parallelize(xrange(1000), numSlices=1)
rdd2 = rdd.repartition(100)
```


#### RepartitionAndSortWithinPartitions

* Its pretty rare usecase.It operates on pairs, esp. key value pairs

* But if you want to repartition and sort with in the partition this would do the job

* Useful for presharding a data before we send it to say for instance, a database


```python
pairs= sc.parallelise( [1,1], [1,2], ,[2,3], [3,3] )
pairs.repartitionAndSortWithinPartitions(2).glom().collect()
#glom->  puts elements within a partition into a list
Parameters - pass a Partition function
pairs.repartitionAndSortWithinPartitions(2, partitionFunc= lambda x:x==1).glom().collect()
```


***


###Actions

* Reduce
* Collect
* Count
* First
* Take
* TakeSample
* TakeOrdered
* SaveAsTextFile
* CountByKey
[comment]: #  <* ForEach>


***


#### Reduce

* Can think of it as a giant tournament record.
* Function has to associative and commutative.
* Since order doesn’t matter, computation occurs in parallel.


numbers= sc.parallelize(xrange(10), numSlices=3)
numbers.glom.collect()
numbers.reduce(max)
numbers.reduce(lambda x,y:x+y)

#Note:
#For generating aggregates about the data - reducebyKey can be used


#### Collect

* Pull all the data from the rdd back into the driver program as a list

* Don’t try to do collect() if the data is n gb/tb

```python
huge_rdd = sc.parallelize(xrange(100000))
huge_dd.sample(withReplacement= False, fraction= 0.00001 , seed=1).collect()
```

#### Count


* Very simple count function.
* But has a couple of interesting cousins with it


```python
countApprox(timeout=200,confidentce=0.5)
countApproxDistinct
numbers.countApproxDistinct(relativeSD=0.04)
```


#### First

Pull the first item from rdd. No optional parameters.


```python
sc.parallelize([3,2,1]).sortBy(lambda x:x).first()

#Throws error when used on an empty rdd.
sc.parallelize([]).first()
```


#### Take


* Almost similar to first

* Take returns a list whereas, first returns just an element

numbers.take(1) #or numbers.first()
numbers.take(10) #or numbers.collect() ,if we want more

* Looks at one partition , checks if it satisfies the count of values to take . 

* If not it moves to the next partition and does the same operation until the no of elements required are collected


#### TakeSample


* Takes random sample of elements from your rdd

* Using seed, used for repeaable research


```python
numbers = sc.parallelise(xrange(10))
numbers.takeSample(withReplacement=False, num=1 , seed=1)

#try by changing the seed value
len(numbers.takeSample(withReplacement=False, num=10000, seed=1))
len(numbers.takeSample(withReplacement=True, num=10000, seed=1))
```


#### TakeOrdered

* This is a special take function

* It will Sort the entire RDD and calling a take() function

* When you want to sort a small dataset as n , don’t do takeOrdered. use sort()

* Works well when n is small relative to the size of the dataset.


```python
numbers.takeOrdered(10)
numbers.takeOrdered(10, key= lambda x:-x)
```


#### SaveAsTextFile


* Calls 2 strings on each element on the RDD and takes the results and  writes one per line. 
* Can save either in local or hdfs file system.



```python
numbers= sc.parallelize( xrange(1000), numSlices(5))
numbers.saveAsTextFile("file_name.txt")

#Will save as 5 files. 1 file per partition

#Optional compression Codec document

numbers.saveAsTextFile("file.gz”, CompressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
```


#### CountByKey

pairs= sc.parallelize([("a”,1),("b”,2),("c”,3)])

pairs.countByKey()


[](Comment <#### ForEach>)

