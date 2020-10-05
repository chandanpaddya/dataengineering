package com.org.cdh.superstore.util

import org.apache.spark.sql.SparkSession
import com.typesafe.config.Config

object getSparkSession {
  val inputconfig = readAppConfig.getConfig("input")
  def getSparkSession(implicit config:Config) : SparkSession = {
        val spark:SparkSession = SparkSession.builder()
                                             .appName(inputconfig.getString("appName"))
                                             .enableHiveSupport
                                             .config("hive.exec.dynamic.partition", "true")
                                             .config("hive.exec.dynamic.partition.mode", "nonstrict")
                                             .config("spark.sql.hive.caseSensitiveInferenceMode", "INFER_ONLY")
                                             .getOrCreate
        spark
  }
  
  //This is only to test in local
   def getSparkSessionNoHive(implicit config:Config) : SparkSession = {
        val spark:SparkSession = SparkSession.builder()
                                             .appName(inputconfig.getString("appName"))
                                             .master("local[*]")
                                             .getOrCreate
        spark
  }
  
  def readConfig(app:String = "input") : Config = {
       readAppConfig.getConfig(app)
  }
  
}