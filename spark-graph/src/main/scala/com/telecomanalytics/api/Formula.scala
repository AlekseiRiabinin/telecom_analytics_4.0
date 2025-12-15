package com.telecomanalytics.api

// ==============================
// Formula Algebra (AST)
// ==============================
sealed trait Formula

// ---------- Core leaves ----------
case class Const(value: Double) extends Formula
case class VertexField(name: String) extends Formula
case class EdgeField(name: String) extends Formula

// ---------- Arithmetic ----------
case class Add(left: Formula, right: Formula) extends Formula
case class Sub(left: Formula, right: Formula) extends Formula
case class Mul(left: Formula, right: Formula) extends Formula
case class Div(left: Formula, right: Formula) extends Formula

// ---------- Aggregations ----------
sealed trait Aggregation extends Formula {
  def expr: Formula
}
case class SumEdges(expr: Formula) extends Aggregation
case class AvgEdges(expr: Formula) extends Aggregation
case class MaxVertices(expr: Formula) extends Aggregation

// ---------- Transformations ----------
case class Log(expr: Formula) extends Formula
case class Normalize(expr: Formula) extends Formula

// ---------- Comparisons ----------
case class Gt(left: Formula, right: Formula) extends Formula
case class Lt(left: Formula, right: Formula) extends Formula
case class Ge(left: Formula, right: Formula) extends Formula
case class Le(left: Formula, right: Formula) extends Formula
case class Eq(left: Formula, right: Formula) extends Formula
case class Ne(left: Formula, right: Formula) extends Formula

// ---------- Conditionals ----------
case class If(cond: Formula, yes: Formula, no: Formula) extends Formula
