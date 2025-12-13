package com.telecomanalytics.api

sealed trait Formula

case class Constant(value: Double) extends Formula
case class Add(left: Formula, right: Formula) extends Formula
case class Multiply(left: Formula, right: Formula) extends Formula
