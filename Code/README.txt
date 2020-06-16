Project By:
Abhishek Ayachit (aayac001@ucr.edu)

Requirements:
1. Python 2.7
2. Spark 2.4.X
3. Dataset Link: https://drive.google.com/file/d/1TAyiKiN1Vi-Ne-kSVWia_NKZSTRTVZ5d/view

Execution Steps:
1. Start spark
2. To Execute the code: bash execute.sh <path-for-locations-file> <path-for-recodordings-directory> <path-for-output-location>
	For eg: bash execute.sh ~/Desktop/CS236/Dataset/WeatherStationLocations.csv ~/Desktop/CS236/Dataset/Records/ ~/Desktop/CS236/

Description:
1. An overall description of how you chose to divide the problem into different spark operations, with reasoning.

There are 4 divisions of tasks in the projec:
1.1 Identify which state are in United States and group the stations by the states
1.2 Find Average Precipitation recorded for each month for each state
1.3 Find Month with highest and lowest average precipitation for each state
1.4 Calculate difference between highest and lowest average precipitation per state and order in ascending order of the difference

2. A description of each step job

2.1 Task 1:
2.1.1 Load the WeatherStationLocations.csv in an RDD
2.1.2 Filter out the data based on country (US in this case) and group the data by states
2.1.3 Display the results

Data Preparation for rest of the project:
a. Load all the Recordings data in an RDD
b. Filter all the columns with values = header
c. Ignore the rows having Precipitation values equal to 99.99
d. Select only the columns those are required: 'STN---', 'YEARMODA', 'PRCP'
e. Based on value in 'PRCP' column, multiply by respective value
f. Create a dataframe with above data
g. Extract Month from 'YEARMODA'. Add Month to the RDD and drop 'YEARMODA' column


2.2 Task 2:
2.2.1 Join the dataframe with locations data based on Country and Station and select only required columns
2.2.2 Group data based on State and Month and use 'avg' as aggregate funtion

2.3 Task 3:
2.3.1 Find Max and Min precipitation for each state(group by) from above results into different dataframes.
2.3.2 Join the respective dataframes with result of Task 2 based on State and average PRCP value
2.3.3 Join the two dataframes(Min and Max)

2.4 Task 4:
2.4.1 From the result of Task 3, create new column with Max-Min values 
2.4.2 Arrange the dataframe in ascending order of the calculated difference
2.4.3 Store the result in a file specified before execution


3. Total runtime for the whole project
('Time required to get the output =  ', 209.4492928981781, 'seconds')
