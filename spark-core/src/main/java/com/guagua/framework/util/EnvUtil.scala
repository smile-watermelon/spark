package com.guagua.framework.util

import org.apache.spark.SparkContext

object EnvUtil {

  private val scLocal = new ThreadLocal[SparkContext]()

  def setSparkContext(sc: SparkContext): Unit = {
    scLocal.set(sc)
  }

  def getSparkContext(): SparkContext = {
    scLocal.get()
  }

  def clear(): Unit = {
    scLocal.remove()
  }

}
