package com.org.cdh.superstore.util

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import com.typesafe.config.Config
import org.apache.log4j.Logger


/**
 * An object representing the process of writing a transform to a database
 */
object writeToUtil{
  
 
  
  /**
   * inserts the data into corresponding partitioned table using parquet format
   * @param dataOut - the dataframe containing the data to write out
   * @param table - a string representing the table to write to
   */
  def writeToTableAppend(dataOut: DataFrame, table: String) (implicit spark: SparkSession) = {
     import spark.implicits._
     dataOut.printSchema
     //dataOut.show
     import spark.sql
          dataOut
          .write
          .mode(SaveMode.Append)
          .format("parquet")
          .insertInto(table)

  }
  
 /**
   * inserts the data into corresponding partitioned table using parquet format
   * @param dataOut - the dataframe containing the data to write out
   * @param table - a string representing the table to write to
   */
  def writeToTableOverwrite(dataOut: DataFrame, table: String) (implicit spark: SparkSession) = {
     import spark.implicits._
     dataOut.printSchema
     //dataOut.show
     import spark.sql
     dataOut.write
            .mode(SaveMode.Overwrite)
            .format("parquet")
            .insertInto(table)

  }
  
   /**
   * inserts the data into corresponding partitioned table using parquet format
   * @param dataOut - the dataframe containing the data to write out
   * @param table - a string representing the table to write to
   */
  def writeToHDFSOverwrite(dataOut: DataFrame, location: String) (implicit spark: SparkSession) = {
     import spark.implicits._
     dataOut.printSchema
     dataOut.write.format("parquet").mode(SaveMode.Overwrite).save(location)

  }
   
   
  
  
}