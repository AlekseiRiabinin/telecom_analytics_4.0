package com.telecomanalytics.engine


import com.telecomanalytics.api.Formula

// ----------------------------
// Base logical plan
// ----------------------------
sealed trait LogicalPlan

// ----------------------------
// Scalar / literal
// ----------------------------
case class Scalar(value: Double) extends LogicalPlan

// ----------------------------
// Algebra
// ----------------------------
case class BinaryOp(
  op: String,
  left: LogicalPlan,
  right: LogicalPlan
) extends LogicalPlan

// ----------------------------
// Graph-aware operations
// ----------------------------
case class VertexAggregation(
  field: String,
  expr: Formula,
  agg: String // sum, avg, max
) extends LogicalPlan

case class EdgeAggregation(
  field: String,
  expr: Formula,
  agg: String
) extends LogicalPlan

// ----------------------------
// Conditional logic
// ----------------------------
case class IfPlan(
  condition: LogicalPlan,
  whenTrue: LogicalPlan,
  whenFalse: LogicalPlan
) extends LogicalPlan
