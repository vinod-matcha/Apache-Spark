########################################################
##Find most popular movie and name using broadcast variables
########################################################
    #broilerplate stuff
from pyspark import SparkConf, SparkContext

    #build the moviename lookup Dictionary table in python to be used to create broadcast object
def lookuptable():
    dic = {}
    #open, read file as f
    with open("/SparkCourse/ml-100k/u.item") as f:
        #for every line, split and add to dictionary
        for line in f:
            x = line.split('|')
            #dictionary with keys as movie id, and value as movie name
            dic[int(x[0])] = x[1]
        return dic
    
conf = SparkConf().setMaster("local").setAppName("popularMovie")
sc = SparkContext(conf =conf)

    #creating broadcast variable
movieNameDict = sc.broadcast(lookuptable())

    #create RDD with movie id and constant 1
lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
parseLines = lines.map(lambda line: (int(line.split()[1]), 1))
    #transform to RDD  as [(movieid, count)] and sort by count
countByMovie = parseLines.reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1])
print (countByMovie.take(10))
    #fetch the broadcast variable from dictionary and assign to the RDD created
nameMovieIdCount = countByMovie.map(lambda x: (x[0], movieNameDict.value[x[0]], x[1]))
    #create python list
result = nameMovieIdCount.collect()
print ('movieid \t name \t count')
for movieid, name, count in result:
    print ('%s \t %s \t count:%d' %(movieid, name, count))
