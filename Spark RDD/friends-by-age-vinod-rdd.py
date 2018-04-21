############################################################
### Spark Program to get the number of friends by age:
### Create 2 separate RDDs to calculate users for age and total friends by age, and then Join RDDs with key as 'age'
### sort the RDD, convert to python object and print result.
############################################################
    #all commands are case sensitive
from pyspark import SparkConf, SparkContext

#can use either double "" or single '' quotes
conf = SparkConf().setMaster('local').setAppName('FriendsByAge')
sc = SparkContext(conf = conf)

def parse(line):
    return (str(line.split(',')[2]), int(line.split(',')[3]))

#can use either double "" or single '' quotes
lines = sc.textFile('///c:/SparkCourse/fakefriends.csv')
print ('RDD values as lines: \n', lines.top(5))

    #Transformation : generates RDD as List of tuples : Convert to [(age, countFriends)] 
lineAgeFriends = lines.map(parse)
print ('RDD values after parsing as (Age, Friends): \n', lineAgeFriends.top(5))

    #Action : lists the keys, as List of keys : Convert to [age]
KeyAges = lineAgeFriends.keys()
    #Action : lists the values, as List of values : Convert to [countFriends]
countValues = lineAgeFriends.values()
    #Action : gives count of all items in RDD : Convert to [countRecords]
countUsers = lineAgeFriends.count()
    #Action : generates a default dictionary, count of occurance of Age : Convert to {age: countAgeOccurances} 
countByKeyAges = lineAgeFriends.countByKey()

    #Action : convert dictionary to list of tuples : Convert to [(age, countAgeOccurances)] 
countByKeyAgesList = lineAgeFriends.countByKey().items()
print ('\nAge, countByKeyAgesList of tuples: \n', countByKeyAgesList)

    #generates RDD from list
countByAgeRDD = sc.parallelize(countByKeyAgesList)
print ('\nAge, Count of users countByAgeRDD: \n', countByAgeRDD.top(5))

    #Transformation : generates RDD as List of tuples : Convert to [(age, totalFriendsByAge)]
friendsTotalByAge = lineAgeFriends.reduceByKey(lambda x, y: x + y)
print ('\nAge, friendsTotalByAge: RDD: \n', friendsTotalByAge.top(5))

    #join the two RDDs on their keys : Convert to [(age, (countAgeOccurances, totalFriendsByAge))] 
joinOnAges = countByAgeRDD.join(friendsTotalByAge)
print ('\nJoin RDDs [(age, (countAgeOccurances, totalFriendsByAge)]: \n', joinOnAges.top(5))

    #Transformation : age sorted RDD as [(age, (avgFriendsByAge, countAgeOccurances))] 
avgFriendsByAge = joinOnAges.mapValues(lambda x: (round(x[1]/x[0]), x[0])).sortByKey()
print ('\nRDD [(age, (avgFriendsByAge, countAgeOccurances))]:\n', avgFriendsByAge.take(5))

    #convert to iterable python list of tuples object
result = avgFriendsByAge.collect()

print ("\nAge \t avgFriends \t noOfUsers")
for i, j in result:
    print ("%s \t %d \t\t %d" %(i, j[0], j[1]))
