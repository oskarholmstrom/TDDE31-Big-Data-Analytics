from pyspark import SparkContext
sc = SparkContext(appName="lab3")

# Read the data and split on every ';' for each line
data = sc.textFile("BDA/input/temperature-readings.csv").cache()

# ("station", "year-month-day", "hour-min-sec", "temperature", "quality")
rows = data.map(lambda x: x.split(';'))

# Filter all data based on year
rows = rows.filter(lambda x: int(x[1][0:4]) >= 1960 and int(x[1][0:4]) <= 2014)

# Restructre the data so that key consist of station and date
# ("station;year-month-date", (min_temp, max_temp))
station_date = rows.map(lambda x: (x[0]+';'+x[1], (float(x[3]), float(x[3]))))

# Find the min and max for every station on a specific day
station_date_avg = station_date.reduceByKey(lambda a, b: (min(a[0], b[0]), max(a[1],b[1])))

# Restructure the data so that the key is the station and specific month
# and add the min and max for each day, and number of observations to calculate
# the daily average from the daily sum.
# ("station;year-month" (daily sum=min+max, observation count))
station_month = station_date_avg.map(lambda x: (x[0][:-2], (x[1][0] + x[1][1], 2)))

# Add all daily sums and every observation count in order to calculate monthly average
station_month = station_month.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))

# Create the desired structure of the output and calculate the monthly average
# (year, month, "station", monthly average)
station_month_avg = station_month.map(lambda x: (int(x[0].split(';')[1][0:4]), int(x[0].split(';')[1][5:7]), x[0].split(';')[0], x[1][0]/x[1][1]))

# Save the RDD to a text file
station_month_avg.saveAsTextFile("BDA/output")



