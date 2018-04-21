from pyspark import SparkConf, SparkContext
from collections import OrderedDict

conf = SparkConf().setMaster("local").setAppName("MoviesByRating")
sc = SparkContext(conf = conf)

def parse(line):
   #movie = line.split()[1]
   rating = line.split()[2]
   #return (movie, rating)
   return rating
   
lines = sc.textFile("///C:/SparkCourse/ml-100k/u.data")
#rating = lines.map(lambda line : parse(line))
#rating = lines.map(lambda line : (line.split()[1], line.split()[2]))
rating = lines.map(lambda line : parse(line))

print (rating.top(10))

countRatings = rating.countByValue()
totalRatings = rating.count()
ratingTypes = rating.countByKey()
ratingKeys = rating.keys()
#ratingValues = rating.values()
#totalMovies = rating.groupByKey()
#ratingTypes = rating.groupBy(0)

print ('countByValue: ',countRatings)
print (totalRatings)
print ('countByKey: ',ratingTypes)
print (ratingKeys.top(10))
#print (ratingValues.top(5))
#print (totalMovies)
#print (ratingTypes.top(10))

dict_sorted = OrderedDict(sorted(countRatings.items()))

for key, value in dict_sorted.items():
    print (str(key) + '\t' + str(value))

for key, value in dict_sorted.items():
    print ("%s %d" %(key, value))
    