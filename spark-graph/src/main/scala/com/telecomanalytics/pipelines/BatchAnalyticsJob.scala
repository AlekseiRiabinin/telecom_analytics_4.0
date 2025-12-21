package com.telecomanalytics.pipelines

import com.telecomanalytics.api._
import com.telecomanalytics.engine._
import com.telecomanalytics.graph._
import com.telecomanalytics.spark.SparkExecutor


object BatchAnalyticsJob {

  def main(args: Array[String]): Unit = {

    // Load graph (domain-level, no Spark yet)
    val graph = GraphLoader.loadSampleGraph()

    // Define business model (math intent)
    val formula: Formula =
      RiskModel.formula()

    // Build logical plan
    val logicalPlan = Planner.plan(formula)

    // Optimize plan
    val optimizedPlan = Optimizer.optimize(logicalPlan)

    // Execute on Spark
    SparkExecutor.withSpark("graph-batch-analytics") { implicit spark =>
      val result = SparkExecutor.execute(optimizedPlan, graph)
      println(s"Batch analytics result = $result")
    }
  }
}
