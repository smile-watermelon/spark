package com.guagua.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Acc {

  def main(args: Array[String]): Unit = {
    // 1、建立和spark 框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)
    // 设置累加器
    val sum: LongAccumulator = sc.longAccumulator("sum")

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // map是转换算子，不触发作业执行，会出现少加 和多加的情况
    // 一般情况下，累加器会放在行动算子中操作
    // 累加器是分布式 只写 变量
    val mapRDD: RDD[Unit] = rdd.map(
      num => {
        sum.add(num)
      }
    )

    //    println(sum.value) // 0
    mapRDD.collect()
    mapRDD.collect()

    println(sum.value) // 20

    sc.stop()
  }

}
