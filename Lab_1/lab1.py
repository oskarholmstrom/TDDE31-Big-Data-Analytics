from pyspark import SparkContext
sc = SparkContext(appName = "lab 1")

#Read the data and split on every ';' for each line
data = sc.textFile("BDA/input/temperature-readings.csv")
rows = data.map(lambda line: line.split(";"))

#(year,temperature)
year_temperature = rows.map(lambda x: (x[1][0:4], float(x[3])))

#Fliter out years that not between 1950 and 2014
year_temperature = year_temperature.filter(lambda x: int(x[0])>=1950 and int(x[0])<=2014)

#Find max temperature for every year
max_temperatures = year_temperature.reduceByKey(max)

#Sort RDD in temperature-descending order
max_temperatures = max_temperatures.sortBy(ascending=False, keyfunc=lambda k: k[1])

#Find min temperature for every year
min_temperatures = year_temperature.reduceByKey(min)

#Sort RDD in temperature-descending order
min_temperatures = min_temperatures.sortBy(ascending=False, keyfunc=lambda k: k[1])

# Save the RDD to a text file
max_temperatures.saveAsTextFile("BDA/output/max_output")
min_temperatures.saveAsTextFile("BDA/output/min_output")
