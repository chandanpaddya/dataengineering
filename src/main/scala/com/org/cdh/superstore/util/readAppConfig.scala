package com.org.cdh.superstore.util

import com.typesafe.config.{Config, ConfigFactory}

object readAppConfig {

  val appConfig = ConfigFactory.load("application.conf")

  def getConfig(moduleName: String): Config = {

    val config = appConfig.getConfig(moduleName).withFallback(appConfig.getConfig("common"))
    config
  }

  def loadConfig(fileName: String): Config = {

    ConfigFactory.load(fileName)

  }

  def hasProp(config: Config,  prop: String) : Boolean = {
    try {
      config.hasPath(prop)
    } catch{
      case e: Exception => false
    }

  }

}