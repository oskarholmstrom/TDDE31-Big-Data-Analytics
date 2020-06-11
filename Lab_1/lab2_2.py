from pyspark import SparkContext

sc = SparkContext(appName = "lab 2b")

# Read the data and split on every ';' for each line
data = sc.textFile("BDA/input/temperature-readings.csv")
rows = data.map(lambda line: line.split(";"))

#Filter years between 1950 to 2015 and temprature above 10.
rows = rows.filter(lambda x: int(x[1][0:4]) >= 1950 and int(x[1][0:4]) <= 2014 and float(x[3]) > 10)

#Restructure the data as station;year-month as key and 1. Then select only one for
#every key that is the same, so only one temperature-reading over 10 is counted.
stations_year_month = rows.map(lambda x: (x[0]+';'+x[1][0:7], 1))
stations_year_month = stations_year_month.reduceByKey(lambda a, b: a)

#Restructure the data year-month and 1. Then count.
year_month = stations_year_month.map(lambda x: (x[0].split(';')[1], 1))
year_month_count = year_month.reduceByKey(lambda a, b: a + b)
#year_month_count = year_month.reduceByKey(add)

#Restructure the data as year,month,count
year_month_count = year_month_count.map(lambda x: (int(x[0][0:4]), int(x[0][5:7]), x[1]))

# Save the RDD to a text file
year_month_count.saveAsTextFile("BDA/output")

