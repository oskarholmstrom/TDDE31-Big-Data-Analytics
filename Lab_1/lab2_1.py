from pyspark import SparkContext

sc = SparkContext(appName = "lab 2a")

# Read the data and split on every ';' for each line
data = sc.textFile("BDA/input/temperature-readings.csv")
rows = data.map(lambda line: line.split(";"))

#Resrtructure the data so the key value is year-month and save the temperature
# as a tuple (temperature, 1) for later addition. Next step is to filter years
# between 1950 to 2015 and temprature above 10.
year_temperature = rows.map(lambda x: (x[1][0:7], (float(x[3]), 1)))
year_temperature = year_temperature.filter(lambda x: int(x[0][0:4])>=1950 and int(x[0][0:4])<=2014 and x[1][0] > 10.0)

#For every month add the ones together to count how many times the temperature was
# above 10.
year_month_count = year_temperature.reduceByKey(lambda a, b: (a[0], a[1] + b[1]))

#Restructure the data as year-month-count
year_month_count = year_month_count.map(lambda x: (int(x[0][0:4]), int(x[0][5:7]), x[1][1]))

# Save the RDD to a text file
year_month_count.saveAsTextFile("BDA/output")






