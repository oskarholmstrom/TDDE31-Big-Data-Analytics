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
schema_temp_readings = schema_temp_readings.filter( (schema_temp_readings.year >= 1960) & (schema_temp_readings.year <= 2014))

# Save the max and min temperature for each combination of date and station
schema_temp_readings_avg = schema_temp_readings.groupBy('year', 'month', 'date', 'station').agg(F.max('temp').alias('max_temp'), F.min('temp').alias('min_temp'))

# Add the min and max for each day by adding the max_temp and min_temp column
# and dividing them by 2 in order to get the daily average
schema_temp_readings_avg = schema_temp_readings_avg.withColumn('daily_avg', (schema_temp_readings_avg.max_temp+schema_temp_readings_avg.min_temp)/2 )
# Get the monthly average by taking the average of each months daily averages
schema_temp_readings_avg = schema_temp_readings_avg.groupBy('year', 'month', 'station').agg(F.avg('daily_avg').alias('monthly_avg'));
# Sorthe the rows in descending order in regard to the monthly average temperature
schema_temp_readings_avg = schema_temp_readings_avg.orderBy('monthly_avg', ascending=False)
schema_temp_readings_avg.show()

