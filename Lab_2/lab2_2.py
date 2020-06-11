from pyspark import SparkContext
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "lab 2")
sqlContext = SQLContext(sc)

# Read and split the data from the csv-file 
data = sc.textFile("BDA/input/temperature-readings.csv")
rows = data.map(lambda line: line.split(";"))

# Create data into rows
temp_readings_row = rows.map(lambda p: (p[0], p[1], int(p[1].split("-")[0]), int(p[1].split("-")[1]), p[2], float(p[3]), p[4] ))
# Create column names 
temp_readings_string = ["station", "date", "year", "month", "time", "temp", "quality"]
# Create a dataframe from the rows and column names
schema_temp_readings = sqlContext.createDataFrame(temp_readings_row, temp_readings_string)

# Remove every row where the year and temperature is outside of the desired interval
schema_temp_readings = schema_temp_readings.filter( (schema_temp_readings.year >= 1950) & (schema_temp_readings.year <= 2014) & (schema_temp_readings.temp > 10.0) )

# Count the occurances of each combination of year and month
schema_temp_readings_count = schema_temp_readings.groupBy('year', 'month').count()
# Sort the rows in descending order in regard to count
schema_temp_readings_count = schema_temp_readings_count.orderBy('count', ascending=False)
schema_temp_readings_count.show()

# Save only each unique combination of year, month and station
schema_temp_readings_distinct_count = schema_temp_readings.select('year','month','station').distinct()
# Count the occurances of each combination of year and month, leading to
# each station only being counted once.
schema_temp_readings_distinct_count = schema_temp_readings_distinct_count.groupBy('year','month').count()
# Sort the rows in descending order in regard to count
schema_temp_readings_distinct_count = schema_temp_readings_distinct_count.orderBy('count', ascending=False)

schema_temp_readings_distinct_count.show()

