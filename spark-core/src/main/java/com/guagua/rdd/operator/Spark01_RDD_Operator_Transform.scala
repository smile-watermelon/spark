package com.guagua.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    /**
     * 获取apache.log 日志中的 url
     */
    val url: RDD[String] = sc.textFile("data/apache.log").map(line => {
      val fields: Array[String] = line.split(" ")
      val length: Int = fields.length
      fields(length - 1)
    })

    url.saveAsTextFile("output")

    sc.stop()
  }

}
