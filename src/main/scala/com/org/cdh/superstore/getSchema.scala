

package com.org.cdh.superstore

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataTypes

object getSchema {
  
  val DecimalType = DataTypes.createDecimalType(15, 4) 
    
    val SuperStoreSchema = StructType(
         StructField("Row ID", IntegerType, true) ::  
        StructField("Order ID", StringType, true) ::
           
        StructField("Order Date", StringType, true) ::
        StructField("Ship Date", StringType, true) ::
        StructField("Ship Mode", StringType, true) ::    
        
        StructField("Customer ID", StringType, true) ::
        StructField("Customer Name", StringType, true) ::
        
        StructField("Segment", StringType, true) ::
        
  
        StructField("Country/Region", StringType, true) ::
        StructField("City", StringType, true) ::
        StructField("State", StringType, true) ::
        
        StructField("Postal Code", StringType, true) ::
        
        StructField("Region", StringType, true) ::
        StructField("Product ID", StringType, true) ::
        StructField("Category", StringType, true) ::
        StructField("Sub-Category", StringType, true) ::
        
        StructField("Product Name", StringType, true) ::
        StructField("Sales", DecimalType, true) ::
        StructField("Quantity", IntegerType, true) ::
        StructField("Discount", DecimalType, true) ::
        StructField("Profit", DecimalType, true) ::
        
   
       
      Nil)
}