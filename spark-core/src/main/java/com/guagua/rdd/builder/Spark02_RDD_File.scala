package com.guagua.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    // 【*】当前本机CPU的所有核数，local 用单线程模拟单核
    sparkConf.setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 可以是文件具体路径，也可以是目录
    //    val value: RDD[String] = sc.textFile("data/1.txt")

    // 读取文件目录数据
    // val value: RDD[String] = sc.textFile("data")

    // 正则匹配文件
    val value: RDD[String] = sc.textFile("data/1*.txt")

    // 从Hadoop 读取文件
    // sc.textFile("hafs://hadoop-01:9000/test.txt")

    value.collect().foreach(println)

    sc.stop()
  }
}
