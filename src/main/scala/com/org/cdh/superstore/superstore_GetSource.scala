package com.org.cdh.superstore

import org.apache.spark.sql.SparkSession
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.DataTypes
import java.math.BigDecimal
import scala.collection.mutable.ListBuffer
import org.joda.time.LocalDate
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import com.org.cdh.superstore.util.readAppConfig
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.DateTime
import com.org.cdh.superstore.getSchema._
object superstore_GetSource {

  val inputConfig = readAppConfig.getConfig("input")

  def fetchSourceData()(implicit sparkSession: SparkSession) = {

  }
  //Use only for local Test
  def readSuperStore()(implicit sparkSession: SparkSession): DataFrame = {

  val file_location = inputConfig.getString("Superstore")
  val file_type = "csv"
 // val infer_schema = "false"
  val first_row_is_header = "true"
  val delimiter = ","
  sparkSession.read.format(file_type).option("header", first_row_is_header).option("sep", delimiter).option("escape", "\"").schema(SuperStoreSchema).load(file_location)
 
    
   
  }
  
 def readReturn()(implicit sparkSession: SparkSession): DataFrame = {

  val file_location = inputConfig.getString("returns")
  val file_type = "csv"
  val infer_schema = "true"
  val first_row_is_header = "true"
  val delimiter = ","
  sparkSession.read.format(file_type).option("inferSchema", infer_schema).option("header", first_row_is_header).option("sep", delimiter).load(file_location).dropDuplicates()
   
  }

 def cleanColumnName(df: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
    val df_clean = sparkSession.createDataFrame(df.rdd, 
StructType(df.schema.map(s => StructField(s.name.replaceAll(" ", "_").replaceAll("-", "_").replaceAll("/", "_"), 
s.dataType, s.nullable))))
      df_clean 
 }
 
  def toDateTime(dtString: String, fmt: DateTimeFormatter): DateTime = {
    fmt.parseDateTime(dtString)
  }
 
  
  
}