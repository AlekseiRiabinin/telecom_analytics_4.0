package com.telecomanalytics.utils


trait Logging {
  protected val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}
