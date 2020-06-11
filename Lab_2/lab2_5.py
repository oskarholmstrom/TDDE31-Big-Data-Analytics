from pyspark import SparkContext
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "lab 2")
sqlContext = SQLContext(sc)

# Read and split the data from the csv-file 
temp_data = sc.textFile("BDA/input/stations-Ostergotland.csv")
temp_rows = temp_data.map(lambda line: line.split(";"))

# Create data into rows
stations_row = temp_rows.map(lambda p: (p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7]))
# Create column names 
stations_string = ["station", "name", "height", "lat", "long", "read_from", "read_to", "elev"]
# Create a dataframe from the rows and acolumn names
stations_ostergotland = sqlContext.createDataFrame(stations_row, stations_string)

# Select only the station column in the data
stations_ostergotland = stations_ostergotland.select('station')

# Create data into rows
preci_data = sc.textFile("BDA/input/precipitation-readings.csv")
preci_rows = preci_data.map(lambda line: line.split(";"))
preci_readings_row = preci_rows.map(lambda p: (p[0], p[1], int(p[1].split("-")[0]), int(p[1].split("-")[1]), p[2], float(p[3]), p[4] ))
# Create column names 
preci_readings_string = ["station", "date", "year", "month", "time", "preci", "quality"]
# Create a dataframe from the rows and acolumn names
schema_preci_readings = sqlContext.createDataFrame(preci_readings_row, preci_readings_string)


# Join the precipitation data with the stations data on station number
# (Only the rows with the corresponding station number in
#  the precipitation data frame will be saved)
schema_preci_readings = schema_preci_readings.join(stations_ostergotland, 'station', 'inner')

# Remove every row where the year is outside of the desired interval
schema_preci_readings = schema_preci_readings.filter( (schema_preci_readings.year >= 1993) & (schema_preci_readings.year <= 2016) )

# For every combination of year, month and station, sum the precipitation.
schema_preci_readings = schema_preci_readings.groupBy('year', 'month', 'station').agg(F.sum('preci').alias('total_preci'))

# For every combination of year and month, take the average of the
# total precipitation over the stations
schema_preci_readings = schema_preci_readings.groupBy('year', 'month').agg(F.avg('total_preci').alias('avg_preci'))

# Sort the data in descending order in regard to year and month.
schema_preci_readings = schema_preci_readings.orderBy(['year', 'month'], ascending=[0,0])
schema_preci_readings.show()
