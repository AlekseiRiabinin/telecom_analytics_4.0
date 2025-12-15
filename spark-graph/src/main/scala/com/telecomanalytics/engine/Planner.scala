package com.telecomanalytics.engine

import com.telecomanalytics.api._
import com.telecomanalytics.engine._


object Planner {

  def plan(formula: Formula): LogicalPlan = formula match {

    case Const(v) =>
      Scalar(v)

    case Add(a, b) =>
      BinaryOp("+", plan(a), plan(b))

    case Mul(a, b) =>
      BinaryOp("*", plan(a), plan(b))

    case SumEdges(expr) =>
      EdgeAggregation(
        field = "weight",
        expr  = expr,
        agg   = "sum"
      )

    case AvgEdges(expr) =>
      EdgeAggregation(
        field = "weight",
        expr  = expr,
        agg   = "avg"
      )

    case f =>
      throw new UnsupportedOperationException(
        s"Formula $f cannot be planned without execution context"
      )
  }
}
