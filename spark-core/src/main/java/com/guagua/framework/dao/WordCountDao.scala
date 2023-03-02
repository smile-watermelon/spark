package com.guagua.framework.dao

import com.guagua.framework.common.TDao
import com.guagua.framework.util.EnvUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class WordCountDao extends TDao {
  override def readFile(path: String): RDD[String] = {
    val sc: SparkContext = EnvUtil.getSparkContext()
    val lines: RDD[String] = sc.textFile(path)
    lines
  }

}
