package com.telecomanalytics.api

import scala.language.implicitConversions


object Operators {

  // Arithmetic
  def +(a: Formula, b: Formula): Formula = Add(a, b)
  def -(a: Formula, b: Formula): Formula = Sub(a, b)
  def *(a: Formula, b: Formula): Formula = Mul(a, b)
  def /(a: Formula, b: Formula): Formula = Div(a, b)

  // Comparisons
  def >(a: Formula, b: Formula): Formula = Gt(a, b)
  def <(a: Formula, b: Formula): Formula = Lt(a, b)
  def >=(a: Formula, b: Formula): Formula = Ge(a, b)
  def <=(a: Formula, b: Formula): Formula = Le(a, b)
  def ===(a: Formula, b: Formula): Formula = Eq(a, b)
  def =!=(a: Formula, b: Formula): Formula = Ne(a, b)

  // Aggregations
  def sumEdges(expr: Formula): Formula = SumEdges(expr)
  def avgEdges(expr: Formula): Formula = AvgEdges(expr)

  // Literals
  implicit def doubleToConst(v: Double): Formula = Const(v)
}
