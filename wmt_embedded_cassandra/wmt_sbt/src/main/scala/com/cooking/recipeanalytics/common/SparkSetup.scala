package com.cooking.recipeanalytics.common

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

trait SparkSetup {
  val sparkConf: SparkConf = new SparkConf().setAppName("WMT_Analytics")
    .set("spark.sql.orc.filterPushdown", "true")
    .set("spark.sql.hive.convertMetastoreOrc", "false")
    .set("spark.ui.port", (4040 + scala.util.Random.nextInt(1000)).toString)
    .set("spark.cassandra.connection.host", "127.0.0.1")
    .set("spark.executor.heartbeatInterval", "100s")


  val spark = SparkSession.builder
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()
  val sc: SparkContext = spark.sparkContext
  val hiveContext: SQLContext = spark.sqlContext
  hiveContext.setConf("hive.exec.dynamic.partition", "true")
  hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
  hiveContext.setConf("hive.execution.engine", "tez")
}
