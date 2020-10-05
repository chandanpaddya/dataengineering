package com.org.cdh.superstore

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import com.org.cdh.superstore.util.readAppConfig
import com.org.cdh.superstore.util.getSparkSession._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{ to_date }
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions.round
import java.text.SimpleDateFormat
import java.util.Date
import com.org.cdh.superstore.superstore_GetSource._
import org.apache.log4j._

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object Entry {
  implicit val config = readAppConfig.getConfig("input") // Only to Test in Local
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
   
    implicit val sparkSession = getSparkSessionNoHive // Only to Test in Local
    
     import sparkSession.sqlContext.implicits._
     
     print(inputConfig.getString("Superstore"))
    
    //sparkSession.read.format("csv").load("./data/Superstore.csv")
 

    //fetchSourceData
   
   val df_superstore=readSuperStore 
    
    val df_returns=readReturn 
    df_superstore.printSchema()
    val df_superstore_merge=df_superstore.join(df_returns,Seq("Order ID"),"left_outer")
    
   
    val df_superstore_clean=cleanColumnName(df_superstore_merge)
                        .filter(col("Country_Region")==="United States")
                        .withColumn("OrderDateClean",to_date(unix_timestamp(col("Order_Date"),"MM/dd/yyyy").cast("timestamp")))
                        .withColumn("ShipDateClean",to_date(unix_timestamp(col("Ship_Date"),"MM/dd/yyyy").cast("timestamp")))
                        .withColumn("duration",datediff(col("ShipDateClean"),col("OrderDateClean")))
                        .withColumn("Returned_Order",when(col("Returned") === "Yes", 1).otherwise(0))

   df_superstore_clean.show(1)
     //val df_superstore_agg=df_superstore_clean.groupBy("State","Category","Sub_Category","Ship_Mode")
       //                                       .agg(sum("Returned_Order"))
    
                        
     val df_superstore_agg=df_superstore_clean.groupBy("State","Category","Sub_Category","Ship_Mode")
                                            .agg(sum("Sales"),sum("Quantity"),sum("Profit"),sum("Returned_Order"),avg("duration"),count("Order_ID"),countDistinct("Customer_ID"))
    
     //df_superstore_agg.show(5)
     df_superstore_agg.printSchema()
    df_superstore_agg.show(5)
     
    val df_superstore_final=df_superstore_agg.select(col("State"),col("Category"),col("Sub_Category")        
        ,round(col("sum(Sales)"),2).alias("Sales")
        ,col("sum(Quantity)").alias("Quantity")
        ,round(col("sum(Profit)"),2).alias("Profit")
        ,col("sum(Returned_Order)").alias("ReturnedCnt")
        ,col("count(Order_ID)").alias("OrderCnt")
        ,col("count(DISTINCT Customer_ID)").alias("CustomerCnt")
        ,col("Ship_Mode")  ,round(col("avg(duration)"),2).alias("Duration")
    )
        
        //,round($"avg(duration)",2).alias("duration"))
    
    df_superstore_final.show(2)
    
   
    df_superstore_final.coalesce(1).write.format("csv").option("header","true").mode(SaveMode.Overwrite).save(readAppConfig.getConfig("output").getString("superstore_output_table"))
  
  }

}