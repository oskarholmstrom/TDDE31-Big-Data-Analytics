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

# Remove every row where the year is outside of the desired interval
schema_temp_readings = schema_temp_readings.filter( (schema_temp_readings.year >= 1950) & (schema_temp_readings.year <= 2014))

# Find the max temperature for each year
schema_temp_readings_max = schema_temp_readings.groupBy('year').agg(F.max('temp').alias('temp'))
# Join the year and temperature data with the corresponding station
schema_temp_readings_max = schema_temp_readings_max.join(schema_temp_readings, ['year','temp'], 'inner').select('year','station','temp')
# Sort the data in descending order in regard to max temperature
schema_temp_readings_max= schema_temp_readings_max.orderBy('temp', ascending=False)
schema_temp_readings_max.show()

# Find the min temperature for each year
schema_temp_readings_min = schema_temp_readings.groupBy('year').agg(F.min('temp').alias('temp'))
# Join the year and temperature data with the corresponding station
schema_temp_readings_min = schema_temp_readings_min.join(schema_temp_readings, ['year', 'temp'] , 'inner').select('year','station','temp')
# Sort the data in descending order in regard to min temperature
schema_temp_readings_min= schema_temp_readings_min.orderBy('temp', ascending=False)
schema_temp_readings_min.show()

