package com.telecomanalytics.api

import org.apache.spark.sql.Dataset


trait Model[T] {
  def evaluate(): Dataset[T]
}
