package com.guagua.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    // 【*】当前本机CPU的所有核数，local 用单线程模拟单核
    sparkConf.setMaster("local[*]").setAppName("RDD")
    sparkConf.set("spark.default.parallelism", "5")

    val sc: SparkContext = new SparkContext(sparkConf)

    // rdd 的并行度 & 分区
    // numSlices 表示的分区数，不传使用默认值，默认并行度
    //    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    // 默认并行度是CPU的最大核数, 默认会取 SparkConf 设置的默认值，没有设置会取当前运行环境最大CPU核数
    // todo scheduler.conf.getInt("spark.default.parallelism", totalCores)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    rdd.saveAsTextFile("output")

    sc.stop()
  }
}
