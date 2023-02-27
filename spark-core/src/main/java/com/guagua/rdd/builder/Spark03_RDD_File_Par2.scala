package com.guagua.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_File_Par2 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    // 【*】当前本机CPU的所有核数，local 用单线程模拟单核
    sparkConf.setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    /**
     * 14 / 2 = 7 byte
     * 14 / 7 = 2 分区
     *
     * 1234567@@  =》0123456 7 8
     * 89@@       =》910 11 12
     * 0          =》14
     *
     * 分区字节偏移
     * 0  =》【0，7】
     * 1  =》【7，14】
     */
    val rdd: RDD[String] = sc.textFile("data/word.txt", 2)

    rdd.saveAsTextFile("output")

    sc.stop()
  }
}
