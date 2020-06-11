from pyspark import SparkContext
sc = SparkContext(appName="lab2b")

# Read the data and split on every ';' for each line
data_stations = sc.textFile("BDA/input/stations-Ostergotland.csv").cache()
# Create a list out of the data
stations_list = data_stations.map(lambda x: x.split(';')[0]).collect()

# Read the data and split on every ';' for each line
data_prec = sc.textFile("BDA/input/precipitation-readings.csv").cache()
# ("station", "year-month-day", "hour-min-sec", "percipitation", "quality")
rows = data_prec.map(lambda x: x.split(';'))

# Filter on year and if the station is in the stations_list, 
# i.e. if it's a station from Ostergotland
rows = rows.filter(lambda x: int(x[1][0:4]) >= 1993 and int(x[1][0:4]) <= 2016 and x[0] in stations_list)


# ("station;year-month", precipitation)
station_month = rows.map(lambda x: (x[0]+';'+x[1][0:7], float(x[3])))

# Add all of the precipitation for a station in a specific month
station_month = station_month.reduceByKey(lambda a, b: a+b)


# ("year-month", (total precipitation per station and month, station count))
station_month_avg = station_month.map(lambda x: (x[0].split(';')[1], (x[1], 1)))

# ("year-month", (total precipitation per month, number of stations))
month_avg = station_month_avg.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))

# Calculate the average precipitation for a specific month
# (year, month, average precipitation)
year_month_avg_prec = month_avg.map(lambda x: (int(x[0][0:4]), int(x[0][5:7]), x[1][0]/x[1][1]))

# Save the RDD to a text file
year_month_avg_prec.saveAsTextFile("BDA/output")
