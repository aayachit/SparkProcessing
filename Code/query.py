from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import substring
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import time
import sys

begin_time = time.time()

sc = SparkContext('local')
spark = SparkSession(sc)

#==========================================================================================================================================================================================================

#Data Load:
locations = sys.argv[1]
recordings = sys.argv[2]
output = sys.argv[3]

location_rdd = spark.read.option("header", "true").csv(locations)
recordings_rdd = sc.textFile("file:" + str(recordings) + "/*.txt")
#recordings_rdd = sc.textFile("file:" + str(recordings) + "/2006.txt")

#==========================================================================================================================================================================================================

#Task 1 (25 points): As your datasets contain data from stations around the world while what you are asked is about US states, you would first need to identify which stations are within the United States. 
#Then you would need to group stations by state.

US_rdd = location_rdd.filter(location_rdd["CTRY"] == "US")
US_State_rdd = US_rdd.groupBy("STATE")
US_State_rdd.count().show(20)
 
#==========================================================================================================================================================================================================

#Data Preparation for Task 2

def translate_PRCP(PRCP):
	last_letter = PRCP[0][len(PRCP[0])-1]
	number = float(PRCP[0][:-1]) #extract number and convert to float

	if last_letter == 'A':
	    number *= (24/6)
	elif last_letter == 'B' or last_letter == 'E':
	    number *= (24/12)
	elif last_letter == 'C':
	    number *= (24/18)
	#D,F,G has data from 24 hours therefore value remains the same
	#H,I has 0 so value remains the same
	return number

def split_data(x):
	x = x.split()
	STN = x[0:1] #For join
	date = x[2:3] #For Month
	PRCP = x[19:20] #For Precipitation

	prcp = translate_PRCP(PRCP)
		
	split_fields = STN + date
	split_fields.append(str(prcp))

	return split_fields

header_rdd = recordings_rdd.first()
column_rdd = [StructField("STN---", StringType(), True), StructField("YEARMODA", StringType(), True), StructField("PRCP", StringType(), True)]

recordings_rdd = recordings_rdd.filter(lambda row:row != header_rdd)
recordings_rdd = recordings_rdd.filter(lambda row:row.split()[19][:-1] != "99.99")
final_rdd = recordings_rdd.map(lambda k: split_data(k)) #Get required data & translate PRCP into actual values

df = spark.createDataFrame(final_rdd, schema=StructType(column_rdd)) #create new dataframe with columns: STN, YEARMODA, PRCP(actual values)

#Convert Month column and drop YEARMODA
change_month =  udf(lambda x: datetime.strptime(x, '%Y%m%d'), DateType())
df1 = df.withColumn("MONTH", date_format(change_month(col("YEARMODA")), 'MMMM'))
df1 = df1.drop("YEARMODA")
df1 = df1.withColumnRenamed("STN---", "STATION")

#==========================================================================================================================================================================================================


#Task 2 (25 points): For each state with readings, you will first need to find the average precipitation recorded for each month (ignoring year).

#Join two datasets based on Station and Country (need to eliminate state=NULL as well) and only select required columns
df2 = location_rdd.join(df1, [location_rdd["USAF"] == df1["STATION"], location_rdd["CTRY"] == "US", location_rdd["STATE"].isNotNull()])
df2 = df2.select(location_rdd.STATE, df1.STATION, df1.PRCP, df1.MONTH)

#Average Precipitation per State per Month
df4 = df2.groupBy("STATE", "MONTH").agg({"PRCP": 'avg'})
df4.show(20)

#==========================================================================================================================================================================================================

#Task 3 (25 points) : Then find the months with the highest and lowest averages for each state.
#MAX and MIN PRCP:
min_df4 = df4.groupBy("STATE").agg({"avg(PRCP)": 'min'})
max_df4 = df4.groupBy("STATE").agg({"avg(PRCP)": 'max'})
#Join to get State with MIN and MIN
min_df5 = df4.join(min_df4, [df4["STATE"] == min_df4["STATE"], df4["avg(PRCP)"] == min_df4["min(avg(PRCP))"]])
min_df5 = min_df5.select(min_df4["STATE"], df4["avg(PRCP)"], df4["MONTH"])
max_df5 = df4.join(max_df4, [df4["STATE"] == max_df4["STATE"], df4["avg(PRCP)"] == max_df4["max(avg(PRCP))"]])
max_df5 = max_df5.select(max_df4["STATE"], df4["avg(PRCP)"], df4["MONTH"])

min_df5 = min_df5.withColumnRenamed("avg(PRCP)", "PRCP_MIN")
min_df5 = min_df5.withColumnRenamed("MONTH", "PRCP_MIN_MONTH")
max_df5 = max_df5.withColumnRenamed("avg(PRCP)", "PRCP_MAX")
max_df5 = max_df5.withColumnRenamed("MONTH", "PRCP_MAX_MONTH")

#Join MAX and MIN
df6 = max_df5.join(min_df5, max_df5["STATE"] == min_df5["STATE"])
df6 = df6.select(max_df5["STATE"], min_df5["PRCP_MIN"], min_df5["PRCP_MIN_MONTH"], max_df5["PRCP_MAX"], max_df5["PRCP_MAX_MONTH"])
df6.show(20)

#==========================================================================================================================================================================================================

#Task 4 (25 points) : You will need to order the states by the difference between the highest and lowest month average, in ascending order.

df7 = df6.withColumn("max(PRCP)-min(PRCP)", col("PRCP_MAX") - col("PRCP_MIN"))
df8 = df7.orderBy('max(PRCP)-min(PRCP)')
df8.show(20)
df8.coalesce(1).write.format("csv").option("header", "true").save(str(output) + "/Output")

#==========================================================================================================================================================================================================

end_time = time.time()
print('Time required to get the output =  ', end_time-begin_time, 'seconds')

