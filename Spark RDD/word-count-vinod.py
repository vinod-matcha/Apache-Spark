#######################################################
## Count Number of words in a text file.
## Customer count the words using regular expression parsing and sort with in the RDD 
#######################################################
import re
from pyspark import SparkConf, SparkContext

def normalizeWords(words):
    return re.compile(r'\W+', re.UNICODE).split(words.lower())
    
conf = SparkConf().setMaster("local").setAppName("wordCountOfBook")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///SparkCourse/Book.txt")
    #Transformation on RDD to [(word)]
parseLines = lines.flatMap(normalizeWords)
print ('\n ***** parseLines RDD: ***** \n', parseLines.top(5))

    #Action on RDD to create Dictionary [(word, count)]
countByWord = parseLines.countByValue()
print ('\n ***** countByWord Dictionary List of Items: *****')
for index, element in enumerate(countByWord.items()):
    print ('{}  {}'.format(element[0], element[1]))
    if index == 4:
        break 

    #Custom approach: Transform RDD into [(word, 1)]
parseByWordCustom = parseLines.map(lambda word: (word, 1))
print ('\n ***** parseByWordCustom RDD: ***** \n', parseByWordCustom.take(5))

    #Transform RDD into [(word, count)] and sort it
countByWordCustom = parseByWordCustom.reduceByKey(lambda x, y: x + y)
sortCountByWord = countByWordCustom.sortBy(lambda x: x[1])
print ('\n ***** sortCountByWord RDD: ***** \n', sortCountByWord.take(5))

    #Action to convert to python List object and print
result = sortCountByWord.collect()
for word, count in result:
    print ('{} \t {}'.format(word, count))



