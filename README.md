# Spark

I am using Spark (PySpark) to do some analysis on a .CSV file. This is just a introduction to basic functions of Spark.

 - I have not used Hadoop here and I used single-node mode.

For analysis I used a real log file as sample data.

## Env. Preparation

I am using windows and started Spark session as below.

```bash
$  ./bin/pyspark
```

## Sample Data
The sample data is from http://cran-logs.rstudio.com/.  (data in the `sample_data` folder ). 


## PySpark

### Start PySpark 

```bash
$  ./bin/pyspark
```
also, we can use Jupyter notebook. 
```bash
$  PYSPARK_DRIVER_PYTHON=jupyter ./bin/pyspark
```

A default SparkContext will be created (usually named as "sc").

### Load Data

method used to load data is `textFile`. This method takes an URI for the file (local file or other URI like hdfs://), and will read the data in as a collections of lines. 
```python
# Load the data
>>> raw_content = sc.textFile("2015-04-01.csv")

# Print the type of the object
>>> type(raw_content)
<class 'pyspark.rdd.RDD'>

# Print the number of lines
>>> raw_content.count()
10065
```

### Show the Head (First `n` rows)
We can use `take` method to return first `n` rows.
```python
>>> raw_content.take(5)
[u'"date","time","size","r_version","r_arch","r_os","package","version","country","ip_id"', 
u'"2015-04-01","00:35:58",504726,"3.1.3","x86_64","mingw32","reshape2","1.4.1","US",1', 
u'"2015-04-01","00:36:00",389303,"3.1.3","x86_64","mingw32","foreach","1.4.2","US",1', 
u'"2015-04-01","00:36:05",1155433,"3.1.3","x86_64","mingw32","plyr","1.8.1","US",1', 
u'"2015-04-01","00:36:07",499011,"3.1.3","x86_64","mingw32","BradleyTerry2","1.0-6","US",1']
```

### Transformation (map & flatMap)

```python
>>> content = raw_content.map(lambda x: x.split(','))
>>> content.take(3)
[[u'"date"', u'"time"', u'"size"', u'"r_version"', u'"r_arch"', u'"r_os"', u'"package"', u'"version"', u'"country"', u'"ip_id"'], [u'"2015-04-01"', u'"00:35:58"', u'504726', u'"3.1.3"', u'"x86_64"', u'"mingw32"', u'"reshape2"', u'"1.4.1"', u'"US"', u'1'], [u'"2015-04-01"', u'"00:36:00"', u'389303', u'"3.1.3"', u'"x86_64"', u'"mingw32"', u'"foreach"', u'"1.4.2"', u'"US"', u'1']]
```
`map(function)` returns a new distributed dataset formed by passing each element of the source through a function specified by user. 

There are several ways to define the functions for `map`. Normally, we can use *lambda* function to do this. 
otherwise use separate function in Python fashion and call it within `map` method. 

for ex: you may have noted the doube quotation marks in the imported data above, and I want to remove all of them in each element of our data

```python
# remove the double quotation marks in the imported data
>>> def clean(x):
        for i in range(len(x)):
            x[i]=x[i].replace('"','')
        return(x)

>>> content = content.map(clean)

>>> content.take(10)
[[u'date', u'time', u'size', u'r_version', u'r_arch', u'r_os', u'package', u'version', u'country', u'ip_id'], [u'2015-04-01', u'00:35:58', u'504726', u'3.1.3', u'x86_64', u'mingw32', u'reshape2', u'1.4.1', u'US', u'1'], [u'2015-04-01', u'00:36:00', u'389303', u'3.1.3', u'x86_64', u'mingw32', u'foreach', u'1.4.2', u'US', u'1'], [u'2015-04-01', u'00:36:05', u'1155433', u'3.1.3', u'x86_64', u'mingw32', u'plyr', u'1.8.1', u'US', u'1'], [u'2015-04-01', u'00:36:07', u'499011', u'3.1.3', u'x86_64', u'mingw32', u'BradleyTerry2', u'1.0-6', u'US', u'1'], [u'2015-04-01', u'00:36:08', u'18299', u'3.1.3', u'x86_64', u'linux-gnu', u'crayon', u'1.1.0', u'US', u'2'], [u'2015-04-01', u'00:36:04', u'12810', u'3.0.2', u'x86_64', u'linux-gnu', u'mime', u'0.3', u'NZ', u'3'], [u'2015-04-01', u'00:35:45', u'71094', u'NA', u'NA', u'NA', u'gemtc', u'0.6-1', u'FR', u'4'], [u'2015-04-01', u'00:35:56', u'395331', u'3.1.3', u'x86_64', u'darwin13.4.0', u'colorspace', u'1.2-6', u'CA', u'5'], [u'2015-04-01', u'00:35:59', u'128340', u'3.1.2', u'x86_64', u'darwin13.4.0', u'digest', u'0.6.8', u'CA', u'5']]
```

There is another method named `flatMap` as well. The difference between `map` and `flatMap` can be explained with below example.
```python
>>> text=["a b c", "d e", "f g h"]
>>> sc.parallelize(text).map(lambda x:x.split(" ")).collect()
[['a', 'b', 'c'], ['d', 'e'], ['f', 'g', 'h']]
>>> sc.parallelize(text).flatMap(lambda x:x.split(" ")).collect()
['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']
```
SUmmary- 
map: It returns a new RDD by applying given function to each element of the RDD. Function in map returns only one item.
flatMap: Similar to map, it returns a new RDD by applying a function to each element of the RDD, but output is flattened.     


### Reduce and Count

Now I would like to know how many records each package has. For example, for R package "Rcpp", I want to know how many rows belong to it.
```python
>>> # Note here x[6] is just the 7th element of each row, that is the package name.
>>> package_count = content.map(lambda x: (x[6], 1)).reduceByKey(lambda a,b: a+b)
>>> type(package_count)
<class 'pyspark.rdd.PipelinedRDD'>
>>> package_count.count()
8660
>>> package_count.take(5)
[(u'SIS', 24), 
(u'StatMethRank', 15), 
(u'dbmss', 54), 
(u'searchable', 14), 
(u'RcmdrPlugin.TextMining', 3)]
```

also we can use `countByKey` method. The result returned by it is in hashmap (like dictionary) structure.

```python
>>> package_count_2 = content.map(lambda x: (x[6], 1)).countByKey()
>>> type(package_count_2)
<type 'collections.defaultdict'>
>>> package_count_2['ggplot2']
3913
>>> package_count_2['stm']
25
```

### Sort

After counting by `reduce` method, we can also analyse for the rankings of these packages based on how many downloads they have with `sortByKey` method. * The argument of `sortByKey` (0 or 1) will determine if we're sorting descently ('0') or ascently ('1').

```python
# Sort DESCENTLY and get the first 10
>>> package_count.map(lambda x: (x[1], x[0])).sortByKey(0).take(10)
[(4783, u'Rcpp'),
 (3913, u'ggplot2'),
 (3748, u'stringi'),
 (3449, u'stringr'),
 (3436, u'plyr'),
 (3265, u'magrittr'),
 (3223, u'digest'),
 (3205, u'reshape2'),
 (3046, u'RColorBrewer'),
 (3007, u'scales')]

 # Sort ascently and get the first 10
 >>> package_count.map(lambda x: (x[1], x[0])).sortByKey(1).take(10)
 [(1, u'TSjson'),
 (1, u'ebayesthresh'),
 (1, u'parspatstat'),
 (1, u'gppois'),
 (1, u'JMLSD'),
 (1, u'kBestShortestPaths'),
 (1, u'StVAR'),
 (1, u'mosaicManip'),
 (1, u'em2'),
 (1, u'DART')]
```

### Filter
It can be considered similar to `SELECT * from TABLE WHERE ???` statement in SQL. It returns a new dataset formed by selecting those elements of the source on which the function specified by user returns true.

For example, I would want to obtain these downloading records of R package "Rtts" from China (CN), then the condition is "package == 'Rtts' AND country = 'CN'".

```python
>>> content.filter(lambda x: x[6] == 'Rtts' and x[8] == 'CN').count()
1
>>> content.filter(lambda x: x[6] == 'Rtts' and x[8] == 'CN').take(1)
[[u'2015-04-01', u'20:15:24', u'23820', u'3.2.2', u'x86_64', u'mingw32', u'Rtts', u'0.3.3', u'CN', u'41']]
```

### Collect Result - Action

All the operations above are done as RDD (Resilient Distributed Datasets) which are implemented 'within' Spark. And we may want to transfer some dataset into Python itself.

`take` method we used above can help us fulfill this purpose partially. But we also have `collect` method to do this, and the difference between `collect` and `take` is that the former will return all the elements in the dataset by default and the later one will return the first `n` rows (`n` is specified by user).
```python
>>> temp = content.filter(lambda x: x[6] == 'Rtts' and x[8] == 'US').collect()

>>> type(temp)
<type 'list'>

>>> temp
[
[u'2015-04-01', u'04:52:36', u'23820', u'3.2.3', u'i386', u'mingw32', u'Rtts', u'0.3.3', u'US', u'1652'], 
[u'2015-04-01', u'20:31:45', u'23820', u'3.2.3', u'x86_64', u'linux-gnu', u'Rtts', u'0.3.3', u'US', u'4438']
]
```

### Union/ Intersection / Distinct Operation
Here we would introduce `union`, `intersection`, and `distinct`
- *union of A and B*: return elements of A AND elements of B.
- *intersection of A and B*: return these elements existing in both A and B.
- *distinct of A*: return the distinct values in A. That is, if element `a` appears more than once, it will only appear once in the result returned.

```python
>>> raw_content.count()
# one set's union with itself equals to its "double"
>>> raw_content.union(raw_content).count()
# one set's intersection with itself equals to its disctinct value set
>>> raw_content.intersection(raw_content).count()
>>> raw_content.distinct().count()

```

### Join
although very expensive 

```python

# generate a new RDD in which the 'country' variable is KEY
>>> content_modified=content.map(lambda x:(x[8], x))

# give a mapping table of the abbreviates of four countries and their full name.
>>> mapping=[('DE', 'Germany'), ('US', 'United States'), ('CN', 'China'), ('IN',"India")]
>>> mapping=sc.parallelize(mapping)

# join
>>> content_modified.join(mapping).takeSample(1, 8)
[(u'US', ([u'2015-04-01', u'02:10:48', u'383068', u'3.1.1', u'i386', u'mingw32', u'xtable', u'1.7-4', u'US', u'2727'], 'United States')), (u'US', ([u'2015-04-01', u'22:04:42', u'1155002', u'3.1.1', u'x86_64', u'mingw32', u'plyr', u'1.8.1', u'US', u'20889'], 'United States')), (u'US', ([u'2015-04-01', u'20:10:57', u'129116', u'3.1.0', u'x86_64', u'darwin10.8.0', u'tictoc', u'1.0', u'US', u'18752'], 'United States')), (u'US', ([u'2015-04-01', u'12:00:25', u'2205242', u'3.1.3', u'x86_64', u'mingw32', u'rehh', u'1.11', u'US', u'757'], 'United States')), (u'DE', ([u'2015-04-01', u'14:07:00', u'66212', u'3.1.2', u'x86_64', u'darwin13.4.0', u'merror', u'2.0.1', u'DE', u'10536'], 'Germany')), (u'US', ([u'2015-04-01', u'18:13:37', u'35561', u'3.1.2', u'x86_64', u'mingw32', u'mime', u'0.3', u'US', u'13579'], 'United States')), (u'US', ([u'2015-04-01', u'08:34:23', u'98558', u'3.0.3', u'x86_64', u'mingw32', u'brew', u'1.0-6', u'US', u'7468'], 'United States')), (u'US', ([u'2015-04-01', u'15:42:10', u'697966', u'3.1.3', u'x86_64', u'darwin13.4.0', u'systemfit', u'1.1-14', u'US', u'14712'], 'United States'))]

# left outer join. 
>>> content_modified.leftOuterJoin(mapping).takeSample(1, 8)

```
### Caching

Some RDDs may be repeatedly accessed, like the RDD *content* in the example above. In such situation, we may want to pull such RDDs into cluster-wide in-memory cache so that the computing relating to them will not be repeatedly implemented, which can help save resource and time. This is called "caching" in Spark, and can be done using RDD.cache() or RDD.persist() method. 

```
>>> content.cache()
```

## Submit Application

All examples showed above were implemented interactively. To automate things, we may need to prepare scripts (applications) in advance and call them, instead of entering line by line.

The `spark-submit` script in Spark’s `bin` directory is just used to figure out this problem, i.e. launch applications. It can use all of Spark’s supported cluster managers (Scala, Java, Python, and R) through a uniform interface so you don’t have to configure your application specially for each one. This means that we only need to prepare and call the scripts while we don't need to tell Spark which driver we're using.

```bash
# submit application written with Python
./bin/spark-submit examples/src/main/python/pi.py

While using `spark-submit`, there are also several options we can specify, including which cluster to use (`--master`) and arbitrary Spark configuration property. For details and examples of this, you may refere to *Submitting Applications*.
```

## Spark SQL and DataFrames

Spark SQL is a Spark module for structured data processing. It enables users to run SQL queries on the data within Spark. DataFrame in Spark is conceptually equivalent to a table in a relational database or a data frame in R/Python. SQL queries in Spark will return results as DataFrames.

### Load CSV Data as DataFrame

```{python}
from pyspark.sql import Row

# Load a text file and convert each line to a Row.
lines = sc.textFile("2015-12-12.csv")
parts = lines.map(lambda l: l.replace('"',""))
parts = parts.map(lambda l: l.split(","))
dat_RDD = parts.map(lambda p: Row(date=p[0], time=p[1], \
										 size=p[2], r_version=p[3],\
										 r_arch=p[4], r_os=p[5],\
										 package=p[6], version=p[7],\
										 country=p[8], ip_id=p[9]))

# Infer the schema, and register the DataFrame as a table.
dat_DF = spark.createDataFrame(dat_RDD)
dat_DF.createOrReplaceTempView("dat")

# SQL can be run over DataFrames that have been registered as a table.
spark.sql("select count(*) from dat where package ='Rcpp'").collect()
#Row(count(1)=4783)

#  Another SQL query example
result=spark.sql("select country, count(*) as count from dat where package = 'Rtts' group by country order by 2")
result.collect()
# [Row(country=u'CN', count=1),
#  Row(country=u'TW', count=1),
#  Row(country=u'GB', count=1),
#  Row(country=u'DE', count=1),
#  Row(country=u'AE', count=1),
#  Row(country=u'US', count=2),
#  Row(country=u'EU', count=2),
#  Row(country=u'KR', count=8)]

result = result.rdd.map(lambda x:x['country'] + ":" + str(x['count'])).collect()
result
# [u'CN:1', u'TW:1', u'GB:1', u'DE:1', u'AE:1', u'US:2', u'EU:2', u'KR:8']
``` 


## References
Spark Programming Guide, http://spark.apache.org/docs/latest/programming-guide.html, https://spark.apache.org/docs/latest/sql-programming-guide.html


