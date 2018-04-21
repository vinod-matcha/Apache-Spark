#sparkcontext is fundamemtal starting point to create RDDs.
#SparkConf needed to configure and create Sparkcontext object.
from pyspark import SparkConf, SparkContext
import collections

#We set the master to local system and good practice to give a name to the job, to lookup on Spark Web UI
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")

#using soark conf object created above create the spark context object
sc = SparkContext(conf = conf)

#load the data file from local system, 'textFile' breaks up the data file line by line
#so every line of text corresponds to one value in your RDD 'lines'
lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")

#for each value of RDD 'lines' split it on space and extract the rating value from index 2 
#assign the transformed values to a new RDD
ratings = lines.map(lambda x: x.split()[2])

#Call Action method on RDD, which create key & value tuples. 
#This is python object (dictionary??) and not RDD
result = ratings.countByValue()

#Using Ordered Dict to sort the results based on key (rating)
sortedResults = collections.OrderedDict(sorted(result.items()))

#iterate over each key value pair and print them 
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
