package com.telecomanalytics.engine

import com.telecomanalytics.api._


object LiteralEvaluator {

  def eval(formula: Formula): Double = formula match {
    case Const(v) => v
    case Add(a, b) => eval(a) + eval(b)
    case Mul(a, b) => eval(a) * eval(b)
    case Sub(a, b) => eval(a) - eval(b)
    case Div(a, b) => eval(a) / eval(b)
    case f =>
      throw new UnsupportedOperationException(
        s"Formula $f requires execution context"
      )
  }
}
