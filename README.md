# dataengineering_scala_superstore

Spark application  that develop to run on  cloudera spark cluser and tested localy and read couple of local csv file(this can change to file on HDFS for production deployment) and perform basic data transformation on these file such as join,filter,aggregation,deriving new column withColumn and withColumnRenamed

Final result are being save a csv file .

scalaVersion := "2.11.8"
spark:="2.1.0"
sbt.version=1.1.2
