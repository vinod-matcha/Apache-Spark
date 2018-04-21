from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("customerAmountSpent")
sc = SparkContext(conf = conf)

def parse(line):
    i = line.split(',')
    return (i[0], i[1], round(float(i[2]),3))

lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
    #convert to RDD [(cust, item, amount)]
parseLines = lines.map(parse)

    #test the elements in mapValues, it will Not include the third element
custAmountSpent2 = parseLines.mapValues(lambda x: x)
print ('\ncustAmountSpent1 mapValues\n', custAmountSpent2.top(10))

    #convert to (key,value) pair RDD [(cust, amount)], get Total spent by Customer and sort by amount spent
custAmountSpent = parseLines.map(lambda x: (x[0], x[2])).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1])

    #convert to List of Items and print output
result = custAmountSpent.collect()
for customer, amount in result:
    print ('%s \t %1.3f' %(customer, amount))    

