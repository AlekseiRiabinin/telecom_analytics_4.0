package com.telecomanalytics.engine

import com.telecomanalytics.api._


object Evaluator {

  def evaluate(formula: Formula): Double = formula match {
    case Constant(v) => v
    case Add(a, b) => evaluate(a) + evaluate(b)
    case Multiply(a, b) => evaluate(a) * evaluate(b)
  }
}
