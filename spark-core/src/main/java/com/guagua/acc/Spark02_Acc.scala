package com.guagua.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc {

  def main(args: Array[String]): Unit = {
    // 1、建立和spark 框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)
    // 设置累加器
    val sum: LongAccumulator = sc.longAccumulator("sum")

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    rdd.foreach(
      num => {
        sum.add(num)
      }
    )
    println(sum.value)

    sc.stop()
  }

}
