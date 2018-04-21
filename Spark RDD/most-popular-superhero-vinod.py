###################################################
## Find the most popular super hero and occurances, using RDD Lookup
## multiple RDDs created
###################################################
from pyspark import SparkConf, SparkContext

def lookupname(line):
    # return (SuperHeroID, Name)
    x = line.split('\"')
    return (int(x[0]), x[1])
    
def parse(line):
    # return (superhero_id, countOtherHerosAppearedAlongwWith)
    return (int(line.split()[0]), len(line.split())-1)

conf = SparkConf().setMaster("local").setAppName("popularSuperHero.py")
sc = SparkContext(conf =conf)

    #RDD with superhero id and count of popularity
lines = sc.textFile("file:///SparkCourse/Marvel-Graph.txt")
parseLines = lines.map(parse)
countBySuperHero = parseLines.reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1])
print ('countBySuperHero:',countBySuperHero.top(3))

    #Action on RDD results in python object
flipped = countBySuperHero.map(lambda x: (x[1], x[0]))
popularSuperHeroCount = flipped.max()
print ('popularSuperHeroCount', popularSuperHeroCount)

    #RDD with superhero names
lineNames = sc.textFile("file:///SparkCourse/Marvel-Names.txt")
parseNames = lineNames.map(lookupname)
print (parseNames.top(3))

    #lookup the popular super hero name in the RDD with key
popularSuperHero = parseNames.lookup(popularSuperHeroCount[1])

print ('\nPopular SuperHero is: %s with %d appearances \n' %(popularSuperHero, popularSuperHeroCount[1]))

