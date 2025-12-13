package com.telecomanalytics.pipelines

import com.telecomanalytics.spark.SparkExecutor
import com.telecomanalytics.api.RiskModel


object BatchAnalyticsJob {

  def main(args: Array[String]): Unit = {
    SparkExecutor.withSpark("graph-batch-job") { spark =>
      val model = new RiskModel(spark)
      model.evaluate().show()
    }
  }
}
