package com.telecomanalytics.spark

import org.apache.spark.sql.SparkSession


object SparkExecutor {

  def withSpark(app: String)(f: SparkSession => Unit): Unit = {
    val spark = SparkSession.builder()
      .appName(app)
      .master("local[*]")
      .getOrCreate()

    f(spark)
    spark.stop()
  }
}
