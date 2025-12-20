package com.telecomanalytics.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import com.telecomanalytics.domain._
import com.telecomanalytics.spark.Encoders._


object GraphFrameAdapter {

  def verticesDF(graph: GraphSchema)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    graph.vertices.toDS().toDF()
  }

  def edgesDF(graph: GraphSchema)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    graph.edges.toDS().toDF()
  }

  def show(graph: GraphSchema)(implicit spark: SparkSession): Unit = {
    verticesDF(graph).show()
    edgesDF(graph).show()
  }
}
