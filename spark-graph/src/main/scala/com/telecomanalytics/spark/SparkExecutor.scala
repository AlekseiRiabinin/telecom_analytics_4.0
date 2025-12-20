package com.telecomanalytics.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import com.telecomanalytics.domain._
import com.telecomanalytics.engine._


object SparkExecutor {

  def withSpark[T](app: String)(f: SparkSession => T): T = {
    val spark = SparkSession.builder()
      .appName(app)
      .master("local[*]")
      .getOrCreate()

    try f(spark)
    finally spark.stop()
  }

  def execute(
    plan: LogicalPlan,
    graph: GraphSchema
  )(implicit spark: SparkSession): Double = plan match {

    case Scalar(v) =>
      v

    case BinaryOp("+", left, right) =>
      execute(left, graph) + execute(right, graph)

    case BinaryOp("*", left, right) =>
      execute(left, graph) * execute(right, graph)

    case EdgeAggregation(_, _, "sum") =>
      graph.edges.map(_.weight).sum

    case EdgeAggregation(_, _, "avg") =>
      val w = graph.edges.map(_.weight)
      if (w.nonEmpty) w.sum / w.size else 0.0

    case VertexAggregation(_, _, "max") =>
      graph.vertices.map(_.value).max

    case IfPlan(cond, whenTrue, whenFalse) =>
      if (execute(cond, graph) > 0) execute(whenTrue, graph)
      else execute(whenFalse, graph)

    case other =>
      throw new UnsupportedOperationException(
        s"Unsupported logical plan: $other"
      )
  }
}
