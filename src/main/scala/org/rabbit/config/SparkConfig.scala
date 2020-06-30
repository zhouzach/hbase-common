package org.rabbit.config

import org.apache.spark.sql.SparkSession

object SparkConfig {
  val spark: SparkSession = SparkSession
    .builder()
    .config("spark.sql.crossJoin.enabled", "true")
    .config("spark.debug.maxToStringFields", 100)
    .config("spark.sql.broadcastTimeout", 36000)
    .config("spark.driver.maxResultSize", "30g")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    .enableHiveSupport()
    .master("local")
    .getOrCreate()


}
