package com.guagua.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {

  def main(args: Array[String]): Unit = {
    // 1、建立和spark 框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 分区内计算，分区间计算
//    val sum: Int = rdd.reduce(_ + _)
//    println(sum)

    var sum = 0
    rdd.foreach(
      num => {
        sum += num
      }
    )
    println(sum)

    sc.stop()
  }

}
