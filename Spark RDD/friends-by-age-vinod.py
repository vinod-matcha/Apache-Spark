############################################################
### Spark Program to get the number of friends by age 
############################################################
    #all commands are case sensitive
from pyspark import SparkConf, SparkContext
from collections import OrderedDict

#can use either double "" or single '' quotes
conf = SparkConf().setMaster('local').setAppName('FriendsByAge')
sc = SparkContext(conf = conf)

def parse(line):
    return (str(line.split(',')[2]), int(line.split(',')[3]))

#can use either double "" or single '' quotes
lines = sc.textFile('///c:/SparkCourse/fakefriends.csv')
    #Transformation : generates RDD as List of tuples : Convert to [(age, countFriends)] 
lineAgeFriends = lines.map(parse)
    #Action : lists the keys, as List of keys : Convert to [age]
KeyAges = lineAgeFriends.keys()
    #Action : lists the values, as List of values : Convert to [countFriends]
countValues = lineAgeFriends.values()
    #Action : gives count of all items in RDD : Convert to [countRecords]
countUsers = lineAgeFriends.count()
    #Action : generates a default dictionary, count of occurance of Age : Convert to [(age, countAgeOccurances)] 
countByKeyAges = lineAgeFriends.countByKey()
    #convert dictionary to list, sort the list and add to Ordered Dictionary
orderCountByKeyAges = OrderedDict(sorted(lineAgeFriends.countByKey().items()))
    #Transformation : generates RDD as List of tuples : Convert to [(age, totalFriendsByAge)]
friendsTotalByAge = lineAgeFriends.reduceByKey(lambda x, y: x + y)
    #Action: RDD converted to list of tuples and sorted: Convert to [(age, totalFriendsByAge)] 
totalFriendsByAgeList = sorted(friendsTotalByAge.collect())


#It was not being possible to Join the separate RDD and Python Object calculations above to find AVG.
#Below steps try to generate the countByKeyAges and friendsTotalByAge in a single RDD and calculate AVG
# Convert to RDD as [(age, (countFriends, 1)]
lineAgeFriends2 = lines.map(lambda line: (parse(line)[0], (parse(line)[1], 1)))
# Convert to RDD as [(age, (totalFriendsByAge, countAgeOccurances)]
lineTotalUsersFriendsByAge = lineAgeFriends2.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
# Convert to RDD as [(age, (avgFriendsByAge, countAgeOccurances))]
avgFriendsByAge = lineTotalUsersFriendsByAge.mapValues(lambda x: (round(x[0]/x[1], 4), x[1]))
# Sort by avgFriendsByAge Desc :  [(age, (avgFriendsByAge, countAgeOccurances))]
sortAvgFriendsByAge = avgFriendsByAge.sortBy(lambda x: x[1])


print ('\n+++++++++++++++++++++')
print ('RDD values as lines: \n', lines.top(5))
print ('-----------------------')
print ('RDD values after parsing as (Age, Friends): \n', lineAgeFriends.top(5))
print ('-----------------------')
print ('Top 5 lines Ages in file:\n', KeyAges.top(5))
print ('-----------------------')
print ('Top 5 lines Values in file:\n', countValues.top(5))
print ('-----------------------')
print ('Total records count:\n', countUsers)
print ('-----------------------')
    #countByKeyAges generated as a default dictionary, so cannot use top(10)
print ('Age, Count of users with specific Age as List of tuples: \n', countByKeyAges.items())
    #printing the default dictionary
print ('Age, Count of users with specific Age as Dictionary: \n', countByKeyAges)
print ('Age, ordered dictionary orderCountByKeyAges: \n', orderCountByKeyAges)
print ('-----------------------')
print ('Age, friendsTotalByAge: RDD: \n', friendsTotalByAge.top(5))
    #List object cannot use top(5)
print ('Age, sorted totalFriendsByAgeList: LIST: \n', totalFriendsByAgeList)
print ('-----------------------')
print ('[(age, (totalFriendsByAge, countAgeOccurances)]: \n', lineTotalUsersFriendsByAge.top(5))
print ('-----------------------')
print ('AvgFriendsByAge: \n', avgFriendsByAge.take(10))
print ('-----------------------')
    #difference between using top():displays unsorted data, and take():displays sorted data - from a sorted RDD
print ('top(10) sortAvgFriendsByAge: \n', sortAvgFriendsByAge.top(10))
print ('take(10) sortAvgFriendsByAge: \n', sortAvgFriendsByAge.take(10))
print ('+++++++++++++++++++++++')

result = sortAvgFriendsByAge.collect()
print ("Age \t avgFriends \t noOfUsers")
for i, j in result:
    print ("%s \t %d \t\t %d" %(i, j[0], j[1]))
