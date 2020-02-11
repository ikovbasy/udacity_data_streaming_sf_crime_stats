## udacity data streaming 
# sf_crime_stats_with_spark
Second project of Data Streaming program

1. SparkSession property parameters control multiple process settings resulting throughput and latency.
For example it is possible to compress the stats data, to control the size of batches for columnar caching, allocate driver and executor memory, control partition size, etc.

2.
Optimal values depend on multiple factors and can be checked by investigating plans with Spark UI
spark.default.parallelism
spark.storage.memoryMapThreshold	
spark.executor.memory
spark.sql.files.maxPartitionBytes
spark.sql.shuffle.partitions
