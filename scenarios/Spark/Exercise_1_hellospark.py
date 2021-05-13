from pyspark.sql import SparkSession

# TO-DO: create a variable with the absolute path to the text file
logFile =  "/home/workspace/Test.txt"

# TO-DO: create a Spark session
spark = SparkSession.builder.appName("HelloSpark").getOrCreate()
# TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

# TO-DO: using the Spark session variable, call the appropriate
# function referencing the text file path to read the text file 
logData = spark.read.text(logFile).cache()

# TO-DO: call the appropriate function to filter the data containing
# the letter 'a', and then count the rows that were found
numDs = logData.filter(logData.value.contains('d')).count()

# TO-DO: call the appropriate function to filter the data containing
# the letter 'b', and then count the rows that were found
numSs = logData.filter(logData.value.contains('s')).count()


# TO-DO: print the count for letter 'd' and letter 's'
print("*******")
print("*******")
print("*****Lines with d: %i, lines with s: %i" % (numDs, numSs))
print("*******")
print("*******")

# TO-DO: stop the spark application
spark.stop()


#commands:
#start spark cluster
#cd /home/workspace/spark/sbin ./start-master.sh
#verify whether running or not and copy URI from log (similar to) Logging to /home/workspace/spark/logs/spark--org.apache.spark.deploy.master.Master-1-5a00814ba363.out    
#Look for a message like this: Starting Spark master at spark://5a00814ba363:7077
#cd /home/workspace/spark/sbin ./start-slave.sh [Spark URI]
#Create a hello world Spark application and submit it to the cluster
#cd /home/workspace/spark/bin
#Type: ./spark-submit /home/workspace/hellospark.py
#watch for o/p




