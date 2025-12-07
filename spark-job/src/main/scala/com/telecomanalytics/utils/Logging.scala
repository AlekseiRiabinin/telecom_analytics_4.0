package com.telecomanalytics.utils

import org.slf4j.LoggerFactory

trait Logging {
  protected val logger = LoggerFactory.getLogger(this.getClass)

  def logInfo(msg: String): Unit = logger.info(msg)
  def logWarn(msg: String): Unit = logger.warn(msg)
  def logError(msg: String): Unit = logger.error(msg)
}
