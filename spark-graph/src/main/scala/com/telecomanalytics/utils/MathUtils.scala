package com.telecomanalytics.utils


object MathUtils {
  def normalize(x: Double): Double = math.max(0.0, math.min(1.0, x))
}
