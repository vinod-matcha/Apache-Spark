#import required libraries
from pyspark import SparkConf, SparkContext

#create spark conf and spark context
conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

#create a function to parse data to return tuple with stationid, temptype, temp recorded
def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)
    
#import the source file and create RDD applying the parse function
lines = sc.textFile("file:///SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)

#create another RDD by filtering out other than "TMAX" temptypes
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))

#reduceByKey the RDD with MAX function to get max temp for each station key
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))

#collect into list and print
results = maxTemps.collect();

for result in results:
    c = (result[1]-32 )*5/9
    print(result[0] + "\t{:.2f}F".format(result[1]) + "\t{:.2f}C".format(c))
