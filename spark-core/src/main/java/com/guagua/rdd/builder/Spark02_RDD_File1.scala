package com.guagua.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File1 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    // 【*】当前本机CPU的所有核数，local 用单线程模拟单核
    sparkConf.setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 可以是文件具体路径，也可以是目录
    //    val value: RDD[String] = sc.textFile("data/1.txt")

    // 读取文件目录数据
    // val value: RDD[String] = sc.textFile("data")

    // wholeTextFiles 以文件为单位读取数据，读取的数据为元组，第一个元素表示文件路径，第二个元素为数据
    val value: RDD[(String, String)] = sc.wholeTextFiles("data")

    value.collect().foreach(println)

    sc.stop()
  }
}
