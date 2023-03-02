package com.guagua.framework.common

import com.guagua.framework.util.EnvUtil
import jdk.internal.util.EnvUtils
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {

  def start(master: String = "local[*]", appName: String = "app")(op: => Unit): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = new SparkContext(sparkConf)
    EnvUtil.setSparkContext(sc)
    try {
      op
    } catch {
      case e: Exception => println(e.getMessage)
    }

    // 3、关闭连接
    sc.stop()
    EnvUtil.clear()
  }

}
