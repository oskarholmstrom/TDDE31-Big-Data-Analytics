from pyspark import SparkContext
sc = SparkContext(appName="lab4")

# Read the data and split on every ';' for each line
data_temp = sc.textFile("BDA/input/temperature-readings.csv").cache()
rows = data_temp.map(lambda x: x.split(';'))

# ("station", temperature)
station = rows.map(lambda x: (x[0], float(x[3])))

# Save the max temperature for each station
station_max = station.reduceByKey(max)

# Remove all stations that do not have a max temperature inside the desired interval
station_max_temp_filtered = station_max.filter(lambda x: x[1] > 25.0 and x[1] < 30.0)

# Read the data and split on every ';' for each line
data_prec = sc.textFile("BDA/input/precipitation-readings.csv").cache()
rows_prec = data_prec.map(lambda x: x.split(';'))

# ("station;year-month-day",  precipitation)
station_date = rows_prec.map(lambda x: (x[0]+';'+x[1], float(x[3])))

# Add the amount of precipitation in a day
station_date_sum = station_date.reduceByKey(lambda a, b: a+b)

# Restructure the data so that the key value is station and then find the
# max for each station
# ("station", precipitation)
station_prec = station_date_sum.map(lambda x: (x[0].split(';')[0], x[1]))
station_max_prec = station_prec.reduceByKey(max)

# Remove all stations that do nat have a max precicipation between the desired values
station_max_prec_filtered = station_max_prec.filter(lambda x: x[1] > 100.0 and x[1] < 200.0)

# Join the precipitation data and temperature data by station (key value)
stations_temp_prec = station_max_temp_filtered.join(station_max_prec_filtered)

# Save the RDD to a text file
stations_temp_prec.saveAsTextFile("BDA/output")
