package com.telecomanalytics.api


object Operators {
  def +(a: Formula, b: Formula): Formula = Add(a, b)
  def *(a: Formula, b: Formula): Formula = Multiply(a, b)
}
