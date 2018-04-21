########################################################################
## Get the Min and Max temperatures recorded at every weather station:
## Find Min and Max Temp in seperate RDDs, and then Join the RDDs
## 
########################################################################
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("MinMaxTemperatues")
sc = SparkContext(conf = conf)

def parse(line):
    lst = line.split(',')
    return (lst[0], lst[2], int(lst[3]))

lines = sc.textFile("///C:/SparkCourse/1800.csv")
    #transform into RDD list of tuples [(StationID, TMIN\TMAX, TempMin\TempMax)]
parseLines = lines.map(parse)
print ('\nRDD lines after parse:\n', parseLines.top(5))

    #convert to RDD as: [(StationID, TempType, TempMin)]
filterMinTemp = parseLines.filter(lambda x: x[1] == 'TMIN')
mapMinTemp = filterMinTemp.map(lambda x: (x[0], x[2]))
print ('\nStationID, TempMin:\n',mapMinTemp.top(5))

    #convert to RDD as: [(StationID, TempType, TempMax)]
filterMaxTemp = parseLines.filter(lambda x: x[1] == 'TMAX')
mapMaxTemp = filterMaxTemp.map(lambda x: (x[0], x[2]))
print ('\nStationID, TempMax:\n',mapMaxTemp.top(5))

    #convert to RDD as: [(stationId, Temp)]
minByStation = mapMinTemp.reduceByKey(lambda x, y: x if x < y else y)
maxByStation = mapMaxTemp.reduceByKey(lambda x, y: x if x > y else y)
print ('\nMin Temperature By Station:\n',minByStation.take(10))
print ('\nMax Temperature By Station:\n',minByStation.take(10))

    #convert to RDD as: [(stationId, (MinTemp, MaxTemp))]
joinByStation = minByStation.join(maxByStation)
print ('\njoinByStation [(stationId, MinTemp, MaxTemp)] RDD:\n',joinByStation.take(10))

    #print output as python List object
result = joinByStation.collect()
print ('\nStationId \t MinTemp \t\t MaxTemp\n')
for i, j in result:
        #print the temperatures in Centigrade and Farenheit
    print ('%s \t %sC / %sF \t %sC / %sF' %(i, round((j[0]-32)*5/9,2), j[0], round((j[1]-32)*5/9, 2), j[1]))
