from pyspark import SparkContext
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "lab 2")
sqlContext = SQLContext(sc)

# Read and split the data from the csv-file 
temp_data = sc.textFile("BDA/input/temperature-readings.csv")
temp_rows = temp_data.map(lambda line: line.split(";"))

# Create data into rows
temp_readings_row = temp_rows.map(lambda p: (p[0], p[1], int(p[1].split("-")[0]), int(p[1].split("-")[1]), p[2], float(p[3]), p[4] ))
# Create column names 
temp_readings_string = ["station", "date", "year", "month", "time", "temp", "quality"]
# Create a dataframe from the rows and column names
schema_temp_readings = sqlContext.createDataFrame(temp_readings_row, temp_readings_string)

# Find the max temperature for each station
schema_temp_readings = schema_temp_readings.groupBy('station').agg(F.max('temp').alias('max_temp'))
# Remove all rows where the max temp is outside of the desired interval
schema_temp_readings = schema_temp_readings.filter( (schema_temp_readings.max_temp >= 25.0) & (schema_temp_readings.max_temp <= 30.0))
schema_temp_readings.show()

# Read and split the data from the csv-file 
preci_data = sc.textFile("BDA/input/precipitation-readings.csv")
preci_rows = preci_data.map(lambda line: line.split(";"))

# Create data into rows
preci_readings_row = preci_rows.map(lambda p: (p[0], p[1], int(p[1].split("-")[0]), int(p[1].split("-")[1]), p[2], float(p[3]), p[4] ))
# Create column names 
preci_readings_string = ["station", "date", "year", "month", "time", "preci", "quality"]
# Create a dataframe from the rows and column names
schema_preci_readings = sqlContext.createDataFrame(preci_readings_row, preci_readings_string)

# For each day and station, sum the precipitation to find 
# the daily precipitation
schema_preci_readings = schema_preci_readings.groupBy('station', 'date').agg(F.sum('preci').alias('daily_preci'))
# Find the maximum precipitation for each station
schema_preci_readings = schema_preci_readings.groupBy('station').agg(F.max('daily_preci').alias('max_preci'))
# Remove all rows where the max precipitation is outside 
# of the desired interval
schema_preci_readings = schema_preci_readings.filter( (schema_preci_readings.max_preci >= 100.0) & (schema_preci_readings.max_preci <= 200.0))
schema_preci_readings.show()

# Join the tables station and max temp with station and max precipitation
# on all occurances where the station is the same in both tables.
schema_station_readings = schema_temp_readings.join(schema_preci_readings, 'station', 'inner')

schema_station_readings.show()


