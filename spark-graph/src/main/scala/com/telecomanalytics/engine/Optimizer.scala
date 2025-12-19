package com.telecomanalytics.engine


object Optimizer {

  def optimize(plan: LogicalPlan): LogicalPlan = plan match {

    // ----------------------------
    // Constant folding
    // ----------------------------
    case BinaryOp("+", Scalar(a), Scalar(b)) =>
      Scalar(a + b)

    case BinaryOp("*", Scalar(a), Scalar(b)) =>
      Scalar(a * b)

    case BinaryOp("-", Scalar(a), Scalar(b)) =>
      Scalar(a - b)

    case BinaryOp("/", Scalar(a), Scalar(b)) =>
      Scalar(a / b)

    // ----------------------------
    // Identity rules
    // ----------------------------
    case BinaryOp("+", left, Scalar(0)) =>
      optimize(left)

    case BinaryOp("+", Scalar(0), right) =>
      optimize(right)

    case BinaryOp("*", left, Scalar(1)) =>
      optimize(left)

    case BinaryOp("*", Scalar(1), right) =>
      optimize(right)

    case BinaryOp("*", _, Scalar(0)) =>
      Scalar(0)

    case BinaryOp("*", Scalar(0), _) =>
      Scalar(0)

    // ----------------------------
    // Recursive descent
    // ----------------------------
    case BinaryOp(op, left, right) =>
      BinaryOp(
        op,
        optimize(left),
        optimize(right)
      )

    case IfPlan(cond, whenTrue, whenFalse) =>
      IfPlan(
        optimize(cond),
        optimize(whenTrue),
        optimize(whenFalse)
      )

    // ----------------------------
    // Graph ops (pass-through for now)
    // ----------------------------
    case v: VertexAggregation => v
    case e: EdgeAggregation   => e

    // ----------------------------
    // Scalars
    // ----------------------------
    case s: Scalar => s
  }
}
